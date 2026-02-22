import os
import json
import time
import csv
import requests

from datetime import datetime, timedelta, timezone, date
from dilution_scanner.master_idx_parser import parse_master_idx
from dilution_scanner.filings import FilingRef, fetch_primary_filing_text, filing_artifact_basename
from dilution_scanner.rules import scan_filing_text_for_labels

SYSTEM_VERSION = "1.1.2"

OUTPUT_DIR = "output"

# Default scan range behavior:
# - In daily cron mode (no start/end provided): scan "yesterday" only
# - In workflow_dispatch mode: scan user-provided start/end (inclusive)
DEFAULT_DAILY_LOOKBACK_DAYS = 1

# EDGAR index parsing constraints
ALLOWED_FORMS = ["424B", "S-3", "S-1", "F-3", "8-K"]

# SEC requires a descriptive User-Agent with real contact email
SEC_USER_AGENT = "DilutionTickerScanner/1.1.2 (contact: kerrychoe@gmail.com)"
SEC_CONTACT_EMAIL = "kerrychoe@gmail.com"

# Output schema (locked)
VERBOSE_COLUMNS = [
    "date",
    "cik",
    "ticker",
    "company_name",
    "form_type",
    "filename",
    "index_url",
    "labels",
    "matched_terms",
]

SUMMARY_COLUMNS = [
    "date",
    "ticker",
    "labels",
    "index_url",
]

# Persistent master schema (locked)
ALL_VERBOSE_COLUMNS = [
    "ticker",
    "first_seen_date",
    "last_seen_date",
    "seen_count",
    "last_labels",
    "last_filing_url",
]

ALL_SUMMARY_COLUMNS = [
    "ticker",
    "first_seen_date",
    "last_seen_date",
    "seen_count",
]

# Master prune (locked)
MASTER_PRUNE_DAYS = 180

# Massive float gate (locked)
MASSIVE_API_URL = "https://api.massive.com/stocks/v1/getStockFloatVX"
MASSIVE_API_KEY_ENV = "MASSIVE_API_KEY"
FLOAT_MAX = 10_000_000
FLOAT_RETRIES = 3
FLOAT_RETRY_SLEEP_SECONDS = 1.0

FLOAT_CACHE_PATH = f"{OUTPUT_DIR}/float_cache.json"

FLOAT_GATE_PASS_CSV = f"{OUTPUT_DIR}/float_gate_pass.csv"
FLOAT_GATE_FAIL_CSV = f"{OUTPUT_DIR}/float_gate_fail.csv"
FLOAT_GATE_UNKNOWN_CSV = f"{OUTPUT_DIR}/float_gate_unknown.csv"

# v1.1.x additive severity output
SEVERITY_BY_TICKER_CSV = f"{OUTPUT_DIR}/dilution_severity_by_ticker.csv"

# v1.1.1 additive derived output
AVOID_TICKERS_CSV = f"{OUTPUT_DIR}/avoid_tickers.csv"

# v1.1.2 additive: persistent severity events master (repo root persisted; output written each run)
SEVERITY_EVENTS_ROOT = "dilution_severity_events_all.csv"
SEVERITY_EVENTS_OUT = f"{OUTPUT_DIR}/dilution_severity_events_all.csv"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_file_text(path: str, content: str):
    ensure_output_dir()
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(content)


def read_file_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def csv_escape(value: str) -> str:
    if value is None:
        return ""
    s = str(value)
    # Ensure deterministic CSV quoting behavior using csv module rules via manual escape
    if any(ch in s for ch in [",", '"', "\n", "\r"]):
        s = s.replace('"', '""')
        return f'"{s}"'
    return s


def _safe_int(x, default=0) -> int:
    try:
        if x is None:
            return int(default)
        s = str(x).strip()
        if s == "":
            return int(default)
        return int(float(s))
    except Exception:
        return int(default)


def _date_to_obj(date_str: str):
    try:
        return date.fromisoformat(date_str)
    except Exception:
        return None


def _iso_today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _iso_yesterday_utc() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def _date_range_inclusive(start_iso: str, end_iso: str) -> list[str]:
    start = date.fromisoformat(start_iso)
    end = date.fromisoformat(end_iso)
    out = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


def _json_dump_stable(obj) -> str:
    return json.dumps(obj, indent=2, sort_keys=True)


def _read_csv_dicts(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _write_csv_dicts(path: str, columns: list[str], rows: list[dict]):
    lines = [",".join(columns) + "\n"]
    for r in rows:
        vals = []
        for c in columns:
            vals.append(csv_escape(r.get(c, "")))
        lines.append(",".join(vals) + "\n")
    write_file_text(path, "".join(lines))


def _load_float_cache() -> dict:
    if not os.path.exists(FLOAT_CACHE_PATH):
        return {}
    try:
        return json.loads(read_file_text(FLOAT_CACHE_PATH))
    except Exception:
        return {}


def _save_float_cache(cache: dict):
    write_file_text(FLOAT_CACHE_PATH, _json_dump_stable(cache))


def _massive_get_float_for_ticker(ticker: str) -> dict:
    """
    Calls Massive API getStockFloatVX for a single ticker.
    Deterministic:
      - no concurrency
      - fixed retries
      - stable request payload
    """
    api_key = os.environ.get(MASSIVE_API_KEY_ENV, "").strip()
    if not api_key:
        return {"ok": False, "error": "MASSIVE_API_KEY missing", "float": None, "raw": None}

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "x-api-key": api_key,
    }
    payload = {"tickers": [ticker]}

    last_err = None
    for attempt in range(FLOAT_RETRIES):
        try:
            resp = requests.post(MASSIVE_API_URL, headers=headers, json=payload, timeout=30)
            if resp.status_code != 200:
                last_err = f"status={resp.status_code}"
                time.sleep(FLOAT_RETRY_SLEEP_SECONDS)
                continue
            data = resp.json()
            # The API returns a list of results per ticker. We take the most recent effective date.
            # Deterministic sorting happens in downstream parsing.
            return {"ok": True, "error": None, "float": None, "raw": data}
        except Exception as e:
            last_err = str(e)
            time.sleep(FLOAT_RETRY_SLEEP_SECONDS)

    return {"ok": False, "error": last_err or "unknown_error", "float": None, "raw": None}


def _extract_best_float_from_massive_raw(raw: dict) -> dict:
    """
    Deterministically selects the best float record for the requested ticker from Massive response:
      - choose the highest effectiveDate (string compare after parsing)
      - return freeFloatShares if present else None
    """
    if not isinstance(raw, dict):
        return {"float": None, "effective_date": None, "percent": None}

    results = raw.get("results")
    if not isinstance(results, list) or len(results) == 0:
        return {"float": None, "effective_date": None, "percent": None}

    # Each entry in results corresponds to a ticker. Expect fields like:
    # { "ticker": "XYZ", "stockFloat": [ { "effectiveDate": "...", "freeFloatShares": ..., "freeFloatPercent": ... }, ... ] }
    entry = results[0]
    stock_float = entry.get("stockFloat")
    if not isinstance(stock_float, list) or len(stock_float) == 0:
        return {"float": None, "effective_date": None, "percent": None}

    # Deterministic: sort by effectiveDate desc, then freeFloatShares desc
    def key_fn(x):
        d = str(x.get("effectiveDate") or "")
        # Keep string; assume ISO-like
        f = _safe_int(x.get("freeFloatShares"), default=0)
        return (d, f)

    best = sorted(stock_float, key=key_fn, reverse=True)[0]
    return {
        "float": _safe_int(best.get("freeFloatShares"), default=0) or None,
        "effective_date": str(best.get("effectiveDate") or "") or None,
        "percent": best.get("freeFloatPercent"),
    }


def _float_gate_for_tickers(tickers: list[str], audit: dict) -> tuple[list[str], list[dict], list[dict], list[dict]]:
    """
    Returns:
      - pass_tickers list (float <= 10M)
      - pass_rows
      - fail_rows
      - unknown_rows
    Deterministic ordering.
    """
    cache = _load_float_cache()
    pass_tickers = []
    pass_rows = []
    fail_rows = []
    unknown_rows = []

    tickers_norm = sorted({(t or "").strip().upper() for t in tickers if (t or "").strip()})

    for t in tickers_norm:
        if t in cache:
            rec = cache[t]
        else:
            api_resp = _massive_get_float_for_ticker(t)
            if not api_resp.get("ok"):
                rec = {"float": None, "effective_date": None, "raw": None, "error": api_resp.get("error")}
            else:
                best = _extract_best_float_from_massive_raw(api_resp.get("raw"))
                rec = {
                    "float": best.get("float"),
                    "effective_date": best.get("effective_date"),
                    "raw": api_resp.get("raw"),
                    "error": None,
                }
            cache[t] = rec

        f = rec.get("float")
        eff = rec.get("effective_date") or ""

        if f is None:
            unknown_rows.append({"ticker": t, "float": "", "effective_date": eff, "reason": "float_unknown"})
            continue

        if int(f) <= int(FLOAT_MAX):
            pass_tickers.append(t)
            pass_rows.append({"ticker": t, "float": str(int(f)), "effective_date": eff, "reason": "pass"})
        else:
            fail_rows.append({"ticker": t, "float": str(int(f)), "effective_date": eff, "reason": "float_gt_10m"})

    _save_float_cache(cache)

    audit["float_gate"] = {
        "tickers_considered": len(tickers_norm),
        "pass_count": len(pass_tickers),
        "fail_count": len(fail_rows),
        "unknown_count": len(unknown_rows),
    }

    return pass_tickers, pass_rows, fail_rows, unknown_rows


def csv_lines_for_float_gate(rows: list[dict]) -> str:
    cols = ["ticker", "float", "effective_date", "reason"]
    out = [",".join(cols) + "\n"]
    for r in rows:
        out.append(",".join([csv_escape(r.get(c, "")) for c in cols]) + "\n")
    return "".join(out)


#
# -----------------------------
# v1.1.2 — PERSISTENT SEVERITY EVENTS MASTER (OPTION B)
# -----------------------------
SEVERITY_EVENTS_COLUMNS = [
    "event_key",
    "date",
    "ticker",
    "cik",
    "form_type",
    "filename",
    "filing_url",
    "labels",
    "matched_terms",
    "bank_score",
    "term_score",
    "final_filing_score",
]


def _severity_event_key(ticker: str, date_iso: str, filename: str) -> str:
    t = (ticker or "").strip().upper()
    d = (date_iso or "").strip()
    f = (filename or "").strip()
    return f"{t}|{d}|{f}"


def _parse_severity_events_csv(path: str) -> dict:
    """
    Returns dict[event_key] = row dict (only SEVERITY_EVENTS_COLUMNS)
    Deterministic: ignores unknown columns; last duplicate event_key wins (stable due to file order).
    """
    if not os.path.exists(path):
        return {}

    out = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return {}
        for row in reader:
            ek = (row.get("event_key") or "").strip()
            if not ek:
                # deterministic: skip malformed rows
                continue
            rec = {}
            for col in SEVERITY_EVENTS_COLUMNS:
                rec[col] = (row.get(col) or "").strip()
            out[ek] = rec
    return out


def _write_severity_events_csv(path: str, records_by_key: dict):
    """
    Deterministic ordering: event_key asc
    """
    lines = [",".join(SEVERITY_EVENTS_COLUMNS) + "\n"]
    for ek in sorted(records_by_key.keys()):
        rec = records_by_key[ek]
        row = []
        for col in SEVERITY_EVENTS_COLUMNS:
            row.append(csv_escape(rec.get(col, "")))
        lines.append(",".join(row) + "\n")
    write_file_text(path, "".join(lines))


def update_severity_events_master(matched_allowed_all: list[dict], end_date_iso: str, audit: dict | None = None) -> list[dict]:
    """
    v1.1.2 Option B:
      - Load existing severity events master (repo root preferred)
      - Append new matched events from this run (labels present AND ticker present)
      - Dedupe by event_key
      - Prune events older than END_DATE-179 days (180d window inclusive)
      - Write output/dilution_severity_events_all.csv
      - Return pruned events list (as dicts)
    """
    prior = {}
    prior_source = "none"
    if os.path.exists(SEVERITY_EVENTS_ROOT):
        prior = _parse_severity_events_csv(SEVERITY_EVENTS_ROOT)
        prior_source = "repo_root"
    elif os.path.exists(SEVERITY_EVENTS_OUT):
        prior = _parse_severity_events_csv(SEVERITY_EVENTS_OUT)
        prior_source = "output"

    prior_count = len(prior)

    try:
        end_obj = date.fromisoformat(end_date_iso)
    except Exception:
        end_obj = None

    cutoff_obj = (end_obj - timedelta(days=179)) if end_obj else None
    cutoff_iso = cutoff_obj.isoformat() if cutoff_obj else ""

    # Build new events from this run
    new_events = {}
    new_count_raw = 0

    for r in (matched_allowed_all or []):
        tkr = str(r.get("ticker") or "").strip().upper()
        labels = r.get("labels") or []
        if not tkr or not labels:
            continue

        dstr = str(r.get("date") or "").strip()
        dobj = _date_to_obj(dstr)
        if not dobj:
            continue

        filename = str(r.get("filename") or "").strip()
        filing_url = str(r.get("index_url") or "").strip()
        cik = str(r.get("cik") or "").strip()
        form_type = str(r.get("form_type") or "").strip()
        matched_terms = r.get("matched_terms") or []

        labels_norm = sorted(set([str(x) for x in labels if str(x)]))
        terms_norm = sorted(set([str(x) for x in matched_terms if str(x)]))

        bank_score = _severity_bank_score(terms_norm)
        term_score = _severity_term_score(terms_norm)
        final_score = _severity_final_filing_score(labels=labels_norm, matched_terms=terms_norm)

        ek = _severity_event_key(tkr, dstr, filename)
        new_count_raw += 1
        new_events[ek] = {
            "event_key": ek,
            "date": dstr,
            "ticker": tkr,
            "cik": cik,
            "form_type": form_type,
            "filename": filename,
            "filing_url": filing_url,
            "labels": "|".join(labels_norm),
            "matched_terms": "|".join(terms_norm),
            "bank_score": str(int(bank_score)),
            "term_score": str(int(term_score)),
            "final_filing_score": str(int(final_score)),
        }

    # Merge (dedupe by event_key; new overrides old deterministically)
    merged = dict(prior)
    for ek in sorted(new_events.keys()):
        merged[ek] = new_events[ek]

    merged_count = len(merged)

    # Prune
    kept = {}
    removed = 0
    for ek in sorted(merged.keys()):
        rec = merged[ek]
        dobj = _date_to_obj(rec.get("date", ""))
        if cutoff_obj and dobj and dobj < cutoff_obj:
            removed += 1
            continue
        if cutoff_obj and not dobj:
            # deterministic hygiene: drop unparseable
            removed += 1
            continue
        kept[ek] = rec

    final_count = len(kept)

    _write_severity_events_csv(SEVERITY_EVENTS_OUT, kept)

    if audit is not None:
        audit["severity_events_master"] = {
            "prior_source": prior_source,
            "prior_count": prior_count,
            "new_events_raw": new_count_raw,
            "new_events_unique": len(new_events),
            "merged_count": merged_count,
            "removed_pruned_count": removed,
            "final_count": final_count,
            "cutoff_date_inclusive_180d_window": cutoff_iso,
            "output_path": SEVERITY_EVENTS_OUT,
            "root_path": SEVERITY_EVENTS_ROOT,
        }

    # Return as list for downstream severity build
    out_list = []
    for ek in sorted(kept.keys()):
        out_list.append(dict(kept[ek]))
    return out_list


# -----------------------------
# v1.1.0/v1.1.1 — SEVERITY INTELLIGENCE (ADDITIVE)
# -----------------------------

LABEL_WEIGHT = {
    "dilution_bank": 5,
    "pipe_financing": 3,
    "convert_financing": 3,
}

# LOCKED bank weights (tier 1–5 based on FINRA final regulatory event tiering)
BANK_WEIGHT = {
    # tier 5
    "h.c. wainwright": 5,
    "hc wainwright": 5,
    "maxim group": 5,
    "maxim": 5,
    "roth capital": 5,
    "roth": 5,
    "ladenburg thalmann": 5,
    "ladenburg": 5,
    "a.g.p.": 5,
    "agp": 5,
    # tier 4
    "b. riley": 4,
    "b.riley": 4,
    "b riley": 4,
    "b. riley securities": 4,
    "briley": 4,
    "ladenburg thalmann & co.": 4,
    "l.p.": 4,
    "benjamin": 4,
    # tier 3
    "canaccord": 3,
    "raymond james": 3,
    "jefferies": 3,
    "cowen": 3,
    "thinkequity": 3,
    # tier 2
    "cantor": 2,
    "wedbush": 2,
    "btig": 2,
    "stifel": 2,
    "piper sandler": 2,
    # tier 1
    "goldman": 1,
    "morgan stanley": 1,
    "j.p. morgan": 1,
    "jp morgan": 1,
    "citigroup": 1,
    "citi": 1,
}

# LOCKED term weights (convertible-heavy language dominates)
TERM_WEIGHT = {
    "at the market": 2,
    "atm": 2,
    "equity line": 2,
    "equity purchase": 2,
    "equity purchase agreement": 3,
    "registered direct offering": 3,
    "rdo": 3,
    "private placement": 3,
    "pipe": 3,
    "purchase agreement": 2,
    "commitment": 1,
    "underwriter": 2,
    "placement agent": 2,
    "selling stockholder": 2,
    "resale registration statement": 3,
    "resale": 1,
    "convertible": 4,
    "convertible note": 5,
    "convertible debenture": 5,
    "senior convertible": 5,
    "secured convertible": 5,
    "convertible preferred": 4,
    "conversion price": 4,
    "conversion rate": 3,
    "conversion": 2,
    "variable conversion": 5,
    "floor price": 3,
    "reset": 3,
    "anti-dilution": 4,
    "down round": 4,
    "price protection": 3,
    "beneficial ownership limitation": 3,
    "9.99%": 2,
    "4.99%": 2,
    "beneficial ownership": 2,
    "cashless exercise": 3,
    "pre-funded": 2,
    "pre funded": 2,
    "warrant": 2,
    "warrants": 2,
    "exercise price": 2,
    "exercise": 1,
    "registration rights": 3,
    "registration rights agreement": 3,
}

BANK_MULTIPLIER = {
    0: 100,
    1: 105,
    2: 110,
    3: 120,
    4: 135,
    5: 150,
}

BANK_BACKSTOP_MIN = 4
TERM_BACKSTOP_MIN = 8
FINAL_SEVERITY_MIN = 20


def _severity_label_score(labels: list[str]) -> int:
    unique = sorted(set([str(x) for x in (labels or []) if str(x)]))
    s = 0
    for lab in unique:
        s += int(LABEL_WEIGHT.get(lab, 0))
    return int(s)


def _severity_bank_score(matched_terms: list[str]) -> int:
    """
    bank_score = max matched bank weight across matched_terms
    """
    terms = [str(x) for x in (matched_terms or [])]
    best = 0
    for b, w in BANK_WEIGHT.items():
        if b in terms:
            if int(w) > best:
                best = int(w)
    return int(best)


def _severity_term_score(matched_terms: list[str]) -> int:
    """
    term_score = sum TERM_WEIGHT for matched_terms (unique terms only)
    """
    terms = sorted(set([str(x) for x in (matched_terms or []) if str(x)]))
    s = 0
    for t in terms:
        s += int(TERM_WEIGHT.get(t, 0))
    return int(s)


def _severity_final_filing_score(labels: list[str], matched_terms: list[str]) -> int:
    """
    LOCKED — Option C Multiplier

    label_score = sum(unique label weights)
    bank_score  = max matched bank weight
    term_score  = sum term weights

    term_component = (term_score * BANK_MULTIPLIER[bank_score]) // 100
    final_filing_score = label_score + term_component + bank_score
    """
    label_score = _severity_label_score(labels)
    bank_score = _severity_bank_score(matched_terms)
    term_score = _severity_term_score(matched_terms)

    mult = int(BANK_MULTIPLIER.get(int(bank_score), 100))
    term_component = (int(term_score) * int(mult)) // 100

    return int(label_score + int(term_component) + int(bank_score))


def _write_severity_csv(path: str, rows: list[dict]):
    cols = [
        "ticker",
        "severity_score_90d",
        "severity_score_180d",
        "match_count_90d",
        "match_count_180d",
        "last_seen_date",
        "last_labels",
        "top_terms",
        "top_banks",
        "max_bank_score_180d",
        "term_score_90d",
        "avoid_flag",
    ]
    lines = [",".join(cols) + "\n"]
    for r in rows:
        lines.append(",".join([csv_escape(r.get(c, "")) for c in cols]) + "\n")
    write_file_text(path, "".join(lines))


def build_dilution_severity_by_ticker_from_events(events_all: list[dict], end_date_iso: str):
    """
    Additive v1.1.x artifact:
      output/dilution_severity_by_ticker.csv

    Uses pruned severity events master + end_date.
    No new API calls.
    Deterministic ordering and integer scoring.
    """
    try:
        end_obj = date.fromisoformat(end_date_iso)
    except Exception:
        return

    start_90 = end_obj - timedelta(days=89)
    start_180 = end_obj - timedelta(days=179)

    by_ticker = {}

    # Include only events with required fields
    for ev in (events_all or []):
        tkr = str(ev.get("ticker") or "").strip().upper()
        labels_str = str(ev.get("labels") or "").strip()
        if not tkr:
            continue
        if not labels_str:
            continue

        dstr = str(ev.get("date") or "").strip()
        try:
            dobj = date.fromisoformat(dstr)
        except Exception:
            continue

        filename = str(ev.get("filename") or "").strip()
        matched_terms_str = str(ev.get("matched_terms") or "").strip()

        matched_terms = [x for x in matched_terms_str.split("|") if x] if matched_terms_str else []
        labels = [x for x in labels_str.split("|") if x] if labels_str else []

        # Prefer stored deterministic values; fall back to recompute if blank
        bank_score = _safe_int(ev.get("bank_score")) or _severity_bank_score(matched_terms)
        term_score = _safe_int(ev.get("term_score")) or _severity_term_score(matched_terms)
        filing_score = _safe_int(ev.get("final_filing_score")) or _severity_final_filing_score(labels=labels, matched_terms=matched_terms)

        item = {
            "date": dobj,
            "date_iso": dobj.isoformat(),
            "filename": filename,
            "labels": [str(x) for x in labels],
            "matched_terms": [str(x) for x in matched_terms],
            "bank_score": int(bank_score),
            "term_score": int(term_score),
            "filing_score": int(filing_score),
        }

        by_ticker.setdefault(tkr, []).append(item)

    out_rows = []

    for tkr in sorted(by_ticker.keys()):
        items = sorted(
            by_ticker[tkr],
            key=lambda x: (x["date_iso"], x.get("filename", "")),
        )

        # windows
        items_90 = [x for x in items if x["date"] >= start_90 and x["date"] <= end_obj]
        items_180 = [x for x in items if x["date"] >= start_180 and x["date"] <= end_obj]

        severity_90 = sum([x["filing_score"] for x in items_90])
        severity_180 = sum([x["filing_score"] for x in items_180])

        match_count_90 = len(items_90)
        match_count_180 = len(items_180)

        last_seen_date = items[-1]["date_iso"] if items else ""
        last_labels = "|".join(sorted(set(sum([x["labels"] for x in items], []))))

        # top terms/banks: deterministic counts then alpha
        term_counts = {}
        bank_counts = {}

        max_bank_score_180 = 0
        term_score_90 = 0

        for x in items_180:
            if x["bank_score"] > max_bank_score_180:
                max_bank_score_180 = x["bank_score"]
            for t in x["matched_terms"]:
                term_counts[t] = term_counts.get(t, 0) + 1

            # include matched banks only (terms list already contains bank substrings)
            for b in BANK_WEIGHT.keys():
                if b in x["matched_terms"]:
                    bank_counts[b] = bank_counts.get(b, 0) + 1

        for x in items_90:
            term_score_90 += x["term_score"]

        top_terms = "|".join([k for k, _v in sorted(term_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]])
        top_banks = "|".join([k for k, _v in sorted(bank_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]])

        avoid_flag = 0
        if (max_bank_score_180 >= BANK_BACKSTOP_MIN and severity_90 >= FINAL_SEVERITY_MIN) or (term_score_90 >= TERM_BACKSTOP_MIN):
            avoid_flag = 1

        out_rows.append(
            {
                "ticker": tkr,
                "severity_score_90d": int(severity_90),
                "severity_score_180d": int(severity_180),
                "match_count_90d": int(match_count_90),
                "match_count_180d": int(match_count_180),
                "last_seen_date": last_seen_date,
                "last_labels": last_labels,
                "top_terms": top_terms,
                "top_banks": top_banks,
                "max_bank_score_180d": int(max_bank_score_180),
                "term_score_90d": int(term_score_90),
                "avoid_flag": int(avoid_flag),
            }
        )

    out_rows = sorted(
        out_rows,
        key=lambda r: (-r["severity_score_90d"], -r["severity_score_180d"], r["ticker"]),
    )

    _write_severity_csv(path=f"{OUTPUT_DIR}/dilution_severity_by_ticker.csv", rows=out_rows)


def write_avoid_tickers_csv():
    """
    v1.1.1 derived file:
      output/avoid_tickers.csv (one column: ticker)

    Built from output/dilution_severity_by_ticker.csv where avoid_flag == 1
    Deterministic sort: ticker asc
    """
    if not os.path.exists(SEVERITY_BY_TICKER_CSV):
        write_file_text(AVOID_TICKERS_CSV, "ticker\n")
        return

    rows = _read_csv_dicts(SEVERITY_BY_TICKER_CSV)
    tickers = []
    for r in rows:
        if _safe_int(r.get("avoid_flag")) == 1:
            t = str(r.get("ticker") or "").strip().upper()
            if t:
                tickers.append(t)

    tickers = sorted(set(tickers))
    lines = ["ticker\n"]
    for t in tickers:
        lines.append(f"{csv_escape(t)}\n")
    write_file_text(AVOID_TICKERS_CSV, "".join(lines))


def _merge_master_verbose(existing: list[dict], new_matches: list[dict], end_date_iso: str) -> list[dict]:
    """
    Merge new matches into persistent master:
      - existing is dilution_tickers_all_verbose.csv content (repo root)
      - new_matches is today's matched + float-gated records

    Locked behavior:
      - update last_seen_date, seen_count, last_labels, last_filing_url
      - first_seen_date remains unchanged
    """
    existing_map = {}
    for r in existing:
        t = str(r.get("ticker") or "").strip().upper()
        if not t:
            continue
        existing_map[t] = {
            "ticker": t,
            "first_seen_date": str(r.get("first_seen_date") or "").strip(),
            "last_seen_date": str(r.get("last_seen_date") or "").strip(),
            "seen_count": _safe_int(r.get("seen_count"), default=0),
            "last_labels": str(r.get("last_labels") or "").strip(),
            "last_filing_url": str(r.get("last_filing_url") or "").strip(),
        }

    # deterministic: sort new matches by ticker, date, filename
    def nm_key(x):
        return (
            str(x.get("ticker") or "").strip().upper(),
            str(x.get("date") or "").strip(),
            str(x.get("filename") or "").strip(),
        )

    new_sorted = sorted(new_matches, key=nm_key)

    for r in new_sorted:
        t = str(r.get("ticker") or "").strip().upper()
        if not t:
            continue
        d = str(r.get("date") or "").strip()

        labels = r.get("labels") or []
        labels_str = "|".join(sorted(set([str(x) for x in labels if str(x)])))
        filing_url = str(r.get("index_url") or "").strip()

        if t not in existing_map:
            existing_map[t] = {
                "ticker": t,
                "first_seen_date": d,
                "last_seen_date": d,
                "seen_count": 1,
                "last_labels": labels_str,
                "last_filing_url": filing_url,
            }
        else:
            rec = existing_map[t]
            rec["last_seen_date"] = d
            rec["seen_count"] = int(rec.get("seen_count", 0)) + 1
            rec["last_labels"] = labels_str
            rec["last_filing_url"] = filing_url
            existing_map[t] = rec

    # prune (locked): remove ticker if last_seen_date < END_DATE - 180 days
    try:
        end_obj = date.fromisoformat(end_date_iso)
    except Exception:
        end_obj = None

    cutoff = (end_obj - timedelta(days=MASTER_PRUNE_DAYS)) if end_obj else None

    out = []
    for t in sorted(existing_map.keys()):
        rec = existing_map[t]
        if cutoff:
            last = _date_to_obj(rec.get("last_seen_date"))
            if not last:
                continue
            if last < cutoff:
                continue
        out.append(rec)

    return out


def _derive_all_summary(master_verbose: list[dict]) -> list[dict]:
    out = []
    for r in master_verbose:
        out.append(
            {
                "ticker": r.get("ticker", ""),
                "first_seen_date": r.get("first_seen_date", ""),
                "last_seen_date": r.get("last_seen_date", ""),
                "seen_count": r.get("seen_count", ""),
            }
        )
    return out


def _normalize_matched_row(row: dict) -> dict:
    """
    Normalize matched row for verbose output.
    """
    labels = row.get("labels") or []
    matched_terms = row.get("matched_terms") or []
    return {
        "date": str(row.get("date") or "").strip(),
        "cik": str(row.get("cik") or "").strip(),
        "ticker": str(row.get("ticker") or "").strip().upper(),
        "company_name": str(row.get("company_name") or "").strip(),
        "form_type": str(row.get("form_type") or "").strip(),
        "filename": str(row.get("filename") or "").strip(),
        "index_url": str(row.get("index_url") or "").strip(),
        "labels": "|".join(sorted(set([str(x) for x in labels if str(x)]))),
        "matched_terms": "|".join(sorted(set([str(x) for x in matched_terms if str(x)]))),
    }


def _normalize_summary_row(row: dict) -> dict:
    labels = row.get("labels") or []
    return {
        "date": str(row.get("date") or "").strip(),
        "ticker": str(row.get("ticker") or "").strip().upper(),
        "labels": "|".join(sorted(set([str(x) for x in labels if str(x)]))),
        "index_url": str(row.get("index_url") or "").strip(),
    }


def _read_master_verbose_repo_root() -> list[dict]:
    path = "dilution_tickers_all_verbose.csv"
    return _read_csv_dicts(path)


def _write_master_verbose_output(master_rows: list[dict]):
    _write_csv_dicts(f"{OUTPUT_DIR}/dilution_tickers_all_verbose.csv", ALL_VERBOSE_COLUMNS, master_rows)


def _write_master_summary_output(master_rows: list[dict]):
    summary = _derive_all_summary(master_rows)
    _write_csv_dicts(f"{OUTPUT_DIR}/dilution_tickers_all.csv", ALL_SUMMARY_COLUMNS, summary)


def _filingref_to_row(d: str, ref: FilingRef) -> dict:
    return {
        "date": d,
        "cik": ref.cik,
        "ticker": ref.ticker or "",
        "company_name": ref.company_name or "",
        "form_type": ref.form_type or "",
        "filename": ref.filename or "",
        "index_url": ref.index_url or "",
        "labels": [],
        "matched_terms": [],
    }


def _write_sample_filing_fetch(sample: dict):
    write_file_text(f"{OUTPUT_DIR}/sample_filing_fetch.json", _json_dump_stable(sample))


def main():
    ensure_output_dir()

    # Inputs
    start_date = os.environ.get("START_DATE", "").strip()
    end_date = os.environ.get("END_DATE", "").strip()

    date_mode = "daily"
    if start_date and end_date:
        date_mode = "range"
    else:
        # daily mode default: yesterday only
        end_date = _iso_yesterday_utc()
        start_date = end_date

    dates = _date_range_inclusive(start_date, end_date)

    # Audit log root
    audit = {
        "system_version": SYSTEM_VERSION,
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "date_mode": date_mode,
        "scan_days": dates,
        "allowed_forms": ALLOWED_FORMS,
    }

    # 1) Parse EDGAR master index for each day
    allowed_filings_all = []
    parsed_rows_total = 0

from dilution_scanner.filings import sec_get  # add near imports if not already present

for d in dates:
    url = master_idx_url_for_date(d)

    resp = sec_get(url, user_agent=SEC_USER_AGENT)
    if resp.status_code != 200 or not resp.content:
        continue

    text = resp.content.decode("latin-1", errors="replace")
    rows = parse_master_idx(text)  # <-- correct usage per master_idx_parser.py

    parsed_rows_total += len(rows)

    # build your allowed filings from `rows` (filter forms, map cik->ticker, etc.)
    # (keep your existing deterministic sort keys)

    # deterministic ordering of allowed filings (date, cik, form_type, filename)
    def allowed_key(x):
        return (
            str(x.get("date") or ""),
            str(x.get("cik") or ""),
            str(x.get("form_type") or ""),
            str(x.get("filename") or ""),
        )

    allowed_filings_all = sorted(allowed_filings_all, key=allowed_key)

    audit["master_idx"] = {
        "parsed_rows_total": int(parsed_rows_total),
        "allowed_filings_total": int(len(allowed_filings_all)),
    }

    # 2) Fetch filing text + apply literal substring rules
    matched_allowed_all = []
    sample_capture = None

    for r in allowed_filings_all:
        ref = FilingRef(
            date=str(r.get("date") or ""),
            cik=str(r.get("cik") or ""),
            ticker=str(r.get("ticker") or ""),
            company_name=str(r.get("company_name") or ""),
            form_type=str(r.get("form_type") or ""),
            filename=str(r.get("filename") or ""),
            index_url=str(r.get("index_url") or ""),
        )

        txt = fetch_primary_filing_text(ref=ref, user_agent=SEC_USER_AGENT)
        found = scan_filing_text_for_labels(txt)

        # found: {"labels":[...], "matched_terms":[...]}
        labels = found.get("labels") or []
        matched_terms = found.get("matched_terms") or []

        if labels:
            row = _filingref_to_row(ref.date, ref)
            row["labels"] = labels
            row["matched_terms"] = matched_terms
            matched_allowed_all.append(row)

            # capture a deterministic sample (first match in deterministic order)
            if sample_capture is None:
                sample_capture = {
                    "date": ref.date,
                    "cik": ref.cik,
                    "ticker": ref.ticker,
                    "company_name": ref.company_name,
                    "form_type": ref.form_type,
                    "filename": ref.filename,
                    "index_url": ref.index_url,
                    "labels": labels,
                    "matched_terms": matched_terms,
                    "filing_text_first_1200": (txt or "")[:1200],
                }

    if sample_capture is None:
        sample_capture = {"note": "no matched filings in this run"}

    _write_sample_filing_fetch(sample_capture)

    audit["matching"] = {
        "matched_allowed_count": int(len(matched_allowed_all)),
    }

    # 3) Float gate (Massive API) — apply to matched tickers only
    matched_tickers = []
    for r in matched_allowed_all:
        t = str(r.get("ticker") or "").strip().upper()
        if t:
            matched_tickers.append(t)

    pass_tickers, pass_rows, fail_rows, unknown_rows = _float_gate_for_tickers(matched_tickers, audit=audit)

    write_file_text(FLOAT_GATE_PASS_CSV, csv_lines_for_float_gate(pass_rows))
    write_file_text(FLOAT_GATE_FAIL_CSV, csv_lines_for_float_gate(fail_rows))
    write_file_text(FLOAT_GATE_UNKNOWN_CSV, csv_lines_for_float_gate(unknown_rows))

    # filter matched rows to float gate pass
    pass_set = set(pass_tickers)
    matched_allowed_pass = []
    for r in matched_allowed_all:
        t = str(r.get("ticker") or "").strip().upper()
        if not t:
            continue
        if t in pass_set:
            matched_allowed_pass.append(r)

    # 4) Write daily outputs
    verbose_rows = [_normalize_matched_row(r) for r in matched_allowed_pass]
    summary_rows = [_normalize_summary_row(r) for r in matched_allowed_pass]

    # deterministic order for verbose and summary (date, ticker, form_type, filename)
    verbose_rows = sorted(verbose_rows, key=lambda x: (x.get("date", ""), x.get("ticker", ""), x.get("form_type", ""), x.get("filename", "")))
    summary_rows = sorted(summary_rows, key=lambda x: (x.get("date", ""), x.get("ticker", ""), x.get("index_url", "")))

    _write_csv_dicts(f"{OUTPUT_DIR}/dilution_tickers_verbose.csv", VERBOSE_COLUMNS, verbose_rows)
    _write_csv_dicts(f"{OUTPUT_DIR}/dilution_tickers.csv", SUMMARY_COLUMNS, summary_rows)

    # 5) Persistent ticker master update (locked behavior)
    existing_master = _read_master_verbose_repo_root()
    merged_master = _merge_master_verbose(existing=existing_master, new_matches=matched_allowed_pass, end_date_iso=end_date)
    _write_master_verbose_output(merged_master)
    _write_master_summary_output(merged_master)

    # 6) Label summary outputs (existing behavior)
    label_counts = {}
    for r in matched_allowed_pass:
        for lab in (r.get("labels") or []):
            label_counts[lab] = label_counts.get(lab, 0) + 1

    label_summary = []
    for lab in sorted(label_counts.keys()):
        label_summary.append({"label": lab, "count": int(label_counts[lab])})

    write_file_text(f"{OUTPUT_DIR}/label_summary.json", _json_dump_stable(label_summary))
    _write_csv_dicts(f"{OUTPUT_DIR}/label_summary.csv", ["label", "count"], label_summary)

    # 7) Raw intermediate dumps
    write_file_text(f"{OUTPUT_DIR}/allowed_filings.json", json.dumps(allowed_filings_all, indent=2))
    write_file_text(f"{OUTPUT_DIR}/matched_allowed_filings.json", json.dumps(matched_allowed_all, indent=2))

    # v1.1.2 additive: update persistent severity events master + recompute severity from master
    events_master = update_severity_events_master(matched_allowed_all=matched_allowed_all, end_date_iso=end_date, audit=audit)
    build_dilution_severity_by_ticker_from_events(events_all=events_master, end_date_iso=end_date)

    # v1.1.1 additive: write avoid tickers output (derived from severity output)
    write_avoid_tickers_csv()

    # 8) Audit + metadata outputs
    write_file_text(f"{OUTPUT_DIR}/audit_log.json", _json_dump_stable(audit))

    run_time = datetime.now(timezone.utc).isoformat()
    run_metadata = {
        "system_version": SYSTEM_VERSION,
        "output_artifacts": [
            "allowed_filings.json",
            "matched_allowed_filings.json",
            "dilution_tickers_verbose.csv",
            "dilution_tickers.csv",
            "dilution_tickers_all_verbose.csv",
            "dilution_tickers_all.csv",
            "float_cache.json",
            "float_gate_pass.csv",
            "float_gate_fail.csv",
            "float_gate_unknown.csv",
            "label_summary.json",
            "label_summary.csv",
            "audit_log.json",
            "run_metadata.json",
            "sample_filing_fetch.json",
            "dilution_severity_by_ticker.csv",
            "avoid_tickers.csv",
            "dilution_severity_events_all.csv",
        ],
        "run_timestamp_utc": run_time,
        "scan_start_date": start_date,
        "scan_end_date": end_date,
        "date_mode": date_mode,
        "scan_days": dates,
        "allowed_forms": ALLOWED_FORMS,
    }

    write_file_text(f"{OUTPUT_DIR}/run_metadata.json", _json_dump_stable(run_metadata))


if __name__ == "__main__":
    main()
