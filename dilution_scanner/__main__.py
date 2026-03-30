# Updated __main__.py for DilutionTicker Scanner v1.1.2
# Fixes:
# 1) severity_events_master.new_events_unique now counts only events not already present in prior master
# 2) deterministic SEC pacing sleep between master.idx requests (SEC_REQUEST_SLEEP_SECONDS)

import os
import json
import time
import requests

from datetime import datetime, timedelta, timezone, date
from dilution_scanner.master_idx_parser import parse_master_idx
from dilution_scanner.filings import FilingRef, fetch_primary_filing_text, filing_artifact_basename
from dilution_scanner.rules import scan_filing_text_for_labels

SYSTEM_VERSION = "1.1.2"

OUTPUT_DIR = "output"

MAX_SAMPLE_BYTES = 2_000_000  # 2 MB

# deterministic SEC pacing between master.idx requests
SEC_REQUEST_SLEEP_SECONDS = 0.2

FLOAT_MAX_SHARES = 10_000_000
FLOAT_GATE_POLICY = "strict_tradeable_only"

STALE_DAYS = 180

ALLOWED_FORMS = ["424B", "S-3", "S-1", "F-3", "8-K"]

SEC_USER_AGENT = "DilutionTickerScanner/1.1.2 (contact: kerrychoe@gmail.com)"
SEC_CONTACT_EMAIL = "kerrychoe@gmail.com"

VERBOSE_COLUMNS = [
    "date",
    "ticker",
    "cik",
    "company",
    "form_type",
    "accession",
    "filing_url",
    "free_float_shares",
    "labels",
    "matched_terms",
]

FLOAT_CACHE_PATH = f"{OUTPUT_DIR}/float_cache.json"
FLOAT_PASS_CSV = f"{OUTPUT_DIR}/float_gate_pass.csv"
FLOAT_FAIL_CSV = f"{OUTPUT_DIR}/float_gate_fail.csv"
FLOAT_UNKNOWN_CSV = f"{OUTPUT_DIR}/float_gate_unknown.csv"

ALL_TICKERS_ROOT = "dilution_tickers_all.csv"
ALL_VERBOSE_ROOT = "dilution_tickers_all_verbose.csv"
ALL_TICKERS_OUT = f"{OUTPUT_DIR}/dilution_tickers_all.csv"
ALL_VERBOSE_OUT = f"{OUTPUT_DIR}/dilution_tickers_all_verbose.csv"

ALL_VERBOSE_COLUMNS = [
    "ticker",
    "first_seen_date",
    "last_seen_date",
    "seen_count",
    "last_labels",
    "last_filing_url",
]

AVOID_TICKERS_OUT = f"{OUTPUT_DIR}/avoid_tickers.csv"

SEVERITY_EVENTS_ROOT = "dilution_severity_events_all.csv"
SEVERITY_EVENTS_OUT = f"{OUTPUT_DIR}/dilution_severity_events_all.csv"
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


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_file_bytes(path, content_bytes: bytes):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content_bytes)


def write_file_text(path, content_text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content_text)


def read_json_file(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_dates():
    start_env = os.getenv("START_DATE", "").strip()
    end_env = os.getenv("END_DATE", "").strip()

    if start_env and end_env:
        start_date = start_env
        end_date = end_env
        mode = "explicit"
    else:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
        start_date = yesterday
        end_date = yesterday
        mode = "default_yesterday"

    return start_date, end_date, mode


def iter_date_range_inclusive(start_iso: str, end_iso: str) -> list[str]:
    s = date.fromisoformat(start_iso)
    e = date.fromisoformat(end_iso)
    if e < s:
        s, e = e, s

    out = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


def sec_get(url: str, timeout_sec: int = 30) -> requests.Response:
    headers = {
        "User-Agent": SEC_USER_AGENT,
        "From": SEC_CONTACT_EMAIL,
        "Accept": "text/plain,application/json,application/octet-stream,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Connection": "close",
        "Referer": "https://www.sec.gov/",
    }

    last_exc = None
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout_sec)
            return resp
        except Exception as e:
            last_exc = e
            time.sleep(attempt)

    raise RuntimeError(f"SEC GET failed after 3 attempts: {url}") from last_exc


def master_idx_url_for_date(date_iso: str) -> str:
    year, month, day = date_iso.split("-")
    y = int(year)
    m = int(month)
    qtr = (m - 1) // 3 + 1
    yyyymmdd = f"{year}{month}{day}"
    return f"https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{qtr}/master.{yyyymmdd}.idx"


def accession_from_filename(filename: str) -> str:
    base = os.path.basename(filename)
    if base.lower().endswith(".txt"):
        return base[:-4]
    return base


def normalize_cik(cik_str: str) -> str:
    try:
        return str(int(str(cik_str).strip()))
    except Exception:
        return str(cik_str).strip()


def csv_escape(value) -> str:
    if value is None:
        s = ""
    else:
        s = str(value)

    needs_quote = ("," in s) or ('"' in s) or ("\n" in s) or ("\r" in s)
    if '"' in s:
        s = s.replace('"', '""')
    if needs_quote:
        return f'"{s}"'
    return s


def _split_csv_line(line: str) -> list[str]:
    parts = []
    cur = ""
    in_q = False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == '"' and (i == 0 or line[i - 1] != "\\"):
            if in_q and i + 1 < len(line) and line[i + 1] == '"':
                cur += '"'
                i += 2
                continue
            in_q = not in_q
            i += 1
            continue
        if ch == "," and not in_q:
            parts.append(cur)
            cur = ""
            i += 1
            continue
        cur += ch
        i += 1
    parts.append(cur)
    return parts


def new_audit(run_time_utc: str, start_date: str, end_date: str, date_mode: str) -> dict:
    return {
        "run_timestamp_utc": run_time_utc,
        "start_date": start_date,
        "end_date": end_date,
        "date_mode": date_mode,
        "events": [],
        "counts": {},
        "by_date": [],
        "error_samples": [],
        "float_gate": {},
        "stale_prune": {},
        "severity_events_master": {},
    }


def audit_event(audit: dict, event: str, data: dict | None = None):
    audit["events"].append({"event": event, "data": (data or {})})


def _parse_company_tickers_json(raw_bytes: bytes) -> dict:
    data = json.loads(raw_bytes.decode("utf-8", errors="replace"))
    if isinstance(data, dict):
        items = data.values()
    elif isinstance(data, list):
        items = data
    else:
        items = []

    out = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        cik_val = item.get("cik_str")
        ticker = item.get("ticker")
        if cik_val is None or ticker is None:
            continue
        cik_key = normalize_cik(cik_val)
        ticker_val = str(ticker).strip().upper()
        if cik_key and ticker_val:
            out[cik_key] = ticker_val
    return out


def _parse_company_tickers_exchange_json(raw_bytes: bytes) -> dict:
    data = json.loads(raw_bytes.decode("utf-8", errors="replace"))
    if isinstance(data, dict):
        items = list(data.values())
    elif isinstance(data, list):
        items = data
    else:
        items = []

    out = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        cik_val = item.get("cik") or item.get("cik_str")
        ticker = item.get("ticker")
        if cik_val is None or ticker is None:
            continue
        cik_key = normalize_cik(cik_val)
        ticker_val = str(ticker).strip().upper()
        if cik_key and ticker_val:
            out[cik_key] = ticker_val
    return out


def load_cik_to_ticker_map_dual_source() -> tuple:
    primary_url = "https://www.sec.gov/files/company_tickers.json"
    exchange_url = "https://www.sec.gov/files/company_tickers_exchange.json"

    resp1 = sec_get(primary_url)
    if resp1.status_code != 200 or not resp1.content:
        raise RuntimeError(f"Failed to fetch company_tickers.json (status={resp1.status_code})")
    write_file_bytes(f"{OUTPUT_DIR}/sec_company_tickers.json", resp1.content)
    primary_map = _parse_company_tickers_json(resp1.content)

    resp2 = sec_get(exchange_url)
    if resp2.status_code != 200 or not resp2.content:
        raise RuntimeError(f"Failed to fetch company_tickers_exchange.json (status={resp2.status_code})")
    write_file_bytes(f"{OUTPUT_DIR}/sec_company_tickers_exchange.json", resp2.content)
    exchange_map = _parse_company_tickers_exchange_json(resp2.content)

    combined = dict(primary_map)
    filled_from_exchange = 0
    for cik_key in sorted(exchange_map.keys()):
        if cik_key not in combined:
            combined[cik_key] = exchange_map[cik_key]
            filled_from_exchange += 1

    meta = {
        "primary_url": primary_url,
        "exchange_url": exchange_url,
        "primary_count": len(primary_map),
        "exchange_count": len(exchange_map),
        "combined_count": len(combined),
        "filled_from_exchange": filled_from_exchange,
        "primary_saved_path": "output/sec_company_tickers.json",
        "exchange_saved_path": "output/sec_company_tickers_exchange.json",
    }
    return combined, meta


def write_ticker_list(path: str, tickers: list):
    uniq = sorted(set([t.strip().upper() for t in tickers if t and t.strip()]))
    write_file_text(path, "\n".join(uniq) + ("\n" if uniq else ""))


def _parse_all_verbose_csv(path: str) -> dict:
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    if not lines:
        return {}

    header = lines[0].split(",")
    idx = {name: i for i, name in enumerate(header)}

    out = {}
    for line in lines[1:]:
        if not line.strip():
            continue

        parts = _split_csv_line(line)

        ticker = ""
        if "ticker" in idx and idx["ticker"] < len(parts):
            ticker = parts[idx["ticker"]].strip().upper()
        if not ticker:
            continue

        rec = {}
        for col in ALL_VERBOSE_COLUMNS:
            v = ""
            if col in idx and idx[col] < len(parts):
                v = parts[idx[col]].strip()
            rec[col] = v
        out[ticker] = rec

    return out


def _write_all_verbose_csv(path: str, records: dict):
    lines = [",".join(ALL_VERBOSE_COLUMNS) + "\n"]
    for tkr in sorted(records.keys()):
        rec = records[tkr]
        row = []
        for col in ALL_VERBOSE_COLUMNS:
            row.append(csv_escape(rec.get(col, "")))
        lines.append(",".join(row) + "\n")
    write_file_text(path, "".join(lines))


def _date_to_obj(d: str) -> date | None:
    try:
        return date.fromisoformat(d)
    except Exception:
        return None


def _safe_int(x):
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        s = str(x).strip().replace(",", "")
        if not s:
            return None
        if "." in s:
            s = s.split(".", 1)[0]
        return int(s)
    except Exception:
        return None


def load_float_cache() -> dict:
    if not os.path.exists(FLOAT_CACHE_PATH):
        return {}
    try:
        obj = read_json_file(FLOAT_CACHE_PATH)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_float_cache(cache: dict):
    write_file_text(FLOAT_CACHE_PATH, json.dumps(cache, indent=2, sort_keys=True))


def massive_get_float_records(ticker: str) -> tuple[bool, dict | None, str | None]:
    url_template = os.getenv("MASSIVE_FLOAT_URL_TEMPLATE", "").strip()
    api_key = os.getenv("MASSIVE_API_KEY", "").strip()

    if not url_template or not api_key:
        return False, None, "Missing MASSIVE_FLOAT_URL_TEMPLATE or MASSIVE_API_KEY"

    url = url_template.replace("{ticker}", ticker)

    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "identity",
        "Connection": "close",
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "DilutionTickerScanner/1.1.2",
    }

    last_exc = None
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                return False, None, f"Non-200 (status={resp.status_code})"
            try:
                return True, resp.json(), None
            except Exception:
                return False, None, "JSON decode failed"
        except Exception as e:
            last_exc = e
            time.sleep(attempt)

    return False, None, f"Request failed after retries: {last_exc}"


def _extract_effective_date_str(obj: dict) -> str:
    for k in ["effective_date", "effectiveDate", "asOfDate", "as_of_date", "date", "dt"]:
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_float_shares(obj: dict) -> int | None:
    keys = [
        "free_float_shares",
        "freeFloatShares",
        "free_float",
        "freeFloat",
        "float_shares",
        "floatShares",
        "float",
        "shares",
    ]
    for k in keys:
        if k in obj:
            val = _safe_int(obj.get(k))
            if val is not None:
                return val
    return None


def pick_float_from_massive_response(resp_obj: dict) -> tuple[int | None, str, list[dict]]:
    candidates = []

    def add_record(rec):
        if not isinstance(rec, dict):
            return
        fs = _extract_float_shares(rec)
        eds = _extract_effective_date_str(rec)
        candidates.append({"float_shares": fs, "effective_date": eds, "raw": rec})

    if isinstance(resp_obj, dict):
        for key in ["results", "data", "floats", "records", "items"]:
            v = resp_obj.get(key)
            if isinstance(v, list):
                for rec in v:
                    add_record(rec)
        if not candidates:
            add_record(resp_obj)

    def parse_date(dstr: str):
        if not dstr:
            return "0000-00-00"
        s = dstr.strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
        if len(s) == 8 and s.isdigit():
            return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
        if "T" in s and len(s) >= 10:
            head = s.split("T", 1)[0]
            if len(head) == 10:
                return head
        return "0000-00-00"

    valid = []
    for c in candidates:
        fs = c.get("float_shares")
        if fs is None:
            continue
        ed = parse_date(c.get("effective_date", ""))
        valid.append((ed, fs, c))

    if not valid:
        return None, "", candidates

    valid.sort(key=lambda t: (t[0], t[1]))
    best = valid[-1]
    return best[1], best[0], candidates


def csv_lines_for_float_gate(rows: list[dict]) -> str:
    cols = ["ticker", "float_shares", "effective_date", "status", "source", "error"]
    out = [",".join(cols) + "\n"]
    for r in rows:
        out.append(
            ",".join(
                [
                    csv_escape(r.get("ticker", "")),
                    csv_escape(r.get("float_shares", "")),
                    csv_escape(r.get("effective_date", "")),
                    csv_escape(r.get("status", "")),
                    csv_escape(r.get("source", "")),
                    csv_escape(r.get("error", "")),
                ]
            )
            + "\n"
        )
    return "".join(out)


LABEL_WEIGHT = {
    "dilution_bank": 5,
    "pipe_financing": 3,
    "convert_financing": 3,
}

BANK_WEIGHT = {
    "aegis capital": 5,
    "maxim group": 5,
    "maxim": 5,
    "a.g.p.": 4,
    "agp": 4,
    "alliance global partners": 4,
    "h.c. wainwright": 4,
    "hc wainwright": 4,
    "roth capital": 4,
    "roth mkms": 4,
    "westpark capital": 3,
    "thinkequity": 3,
    "boustead securities": 2,
    "benjamin securities": 2,
    "ef hutton": 1,
}

TERM_WEIGHT = {
    "convertible note": 5,
    "convertible notes": 5,
    "convertible debenture": 5,
    "convertible debentures": 5,
    "senior convertible": 4,
    "conversion price": 4,
    "conversion feature": 4,
    "convertible preferred": 4,
    "variable rate": 5,
    "reset price": 5,
    "price reset": 5,
    "pipe financing": 3,
    "private investment in public equity": 3,
    "private investment in public equities": 3,
    "private placement": 3,
    "registered direct": 3,
    "equity line of credit": 2,
    "at-the-market": 2,
    "at the market": 2,
    "atm offering": 2,
    "eloc": 2,
}

BANK_MULTIPLIER_BPS = {
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
    uniq = sorted(set([str(x) for x in (labels or []) if str(x)]))
    s = 0
    for lab in uniq:
        s += LABEL_WEIGHT.get(lab, 0)
    return s


def _severity_bank_score(matched_terms: list[str]) -> int:
    best = 0
    for t in (matched_terms or []):
        w = BANK_WEIGHT.get(t, 0)
        if w > best:
            best = w
    return best


def _severity_term_score(matched_terms: list[str]) -> int:
    s = 0
    for t in (matched_terms or []):
        s += TERM_WEIGHT.get(t, 0)
    return s


def _severity_final_filing_score(labels: list[str], matched_terms: list[str]) -> int:
    label_score = _severity_label_score(labels)
    bank_score = _severity_bank_score(matched_terms)
    term_score = _severity_term_score(matched_terms)

    mult = BANK_MULTIPLIER_BPS.get(bank_score, 100)
    term_component = (term_score * mult) // 100

    return label_score + term_component + bank_score


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


def build_dilution_severity_by_ticker(matched_allowed_all: list[dict], end_date_iso: str):
    try:
        end_obj = date.fromisoformat(end_date_iso)
    except Exception:
        return

    start_90 = end_obj - timedelta(days=89)
    start_180 = end_obj - timedelta(days=179)

    by_ticker = {}

    for r in (matched_allowed_all or []):
        tkr = str(r.get("ticker") or "").strip().upper()
        labels = r.get("labels") or []
        if not tkr or not labels:
            continue

        dstr = str(r.get("date") or "").strip()
        try:
            dobj = date.fromisoformat(dstr)
        except Exception:
            continue

        matched_terms = r.get("matched_terms") or []
        filename = str(r.get("filename") or "").strip()

        bank_score = _severity_bank_score(matched_terms)
        term_score = _severity_term_score(matched_terms)
        filing_score = _severity_final_filing_score(labels=labels, matched_terms=matched_terms)

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
        items = by_ticker[tkr]

        sev_90 = 0
        sev_180 = 0
        cnt_90 = 0
        cnt_180 = 0

        term_freq = {}
        bank_freq = {}

        max_bank_180 = 0
        term_score_90 = 0

        last_key = None
        last_seen_date = ""
        last_labels = []

        for it in items:
            d = it["date"]
            score = int(it["filing_score"])
            bsc = int(it.get("bank_score", 0))
            tsc = int(it.get("term_score", 0))

            if start_180 <= d <= end_obj:
                sev_180 += score
                cnt_180 += 1

                if bsc > max_bank_180:
                    max_bank_180 = bsc

                for term in it["matched_terms"]:
                    if term in TERM_WEIGHT:
                        term_freq[term] = term_freq.get(term, 0) + 1
                    if term in BANK_WEIGHT:
                        bank_freq[term] = bank_freq.get(term, 0) + 1

                key = (it["date_iso"], it["filename"])
                if (last_key is None) or (key > last_key):
                    last_key = key
                    last_seen_date = it["date_iso"]
                    last_labels = it["labels"]

            if start_90 <= d <= end_obj:
                sev_90 += score
                cnt_90 += 1
                term_score_90 += tsc

        top_terms_list = sorted(term_freq.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        top_banks_list = sorted(bank_freq.items(), key=lambda kv: (-kv[1], kv[0]))[:3]

        top_terms = "|".join([k for k, _v in top_terms_list])
        top_banks = "|".join([k for k, _v in top_banks_list])

        avoid_flag = 0
        if ((max_bank_180 >= BANK_BACKSTOP_MIN) and (sev_90 >= FINAL_SEVERITY_MIN)) or (term_score_90 >= TERM_BACKSTOP_MIN):
            avoid_flag = 1

        out_rows.append(
            {
                "ticker": tkr,
                "severity_score_90d": str(sev_90),
                "severity_score_180d": str(sev_180),
                "match_count_90d": str(cnt_90),
                "match_count_180d": str(cnt_180),
                "last_seen_date": last_seen_date,
                "last_labels": "|".join(sorted(set(last_labels))) if last_labels else "",
                "top_terms": top_terms,
                "top_banks": top_banks,
                "max_bank_score_180d": str(max_bank_180),
                "term_score_90d": str(term_score_90),
                "avoid_flag": str(avoid_flag),
            }
        )

    def _to_int(s):
        try:
            return int(str(s))
        except Exception:
            return 0

    out_rows.sort(key=lambda r: (-_to_int(r["severity_score_90d"]), -_to_int(r["severity_score_180d"]), r["ticker"]))

    _write_severity_csv(f"{OUTPUT_DIR}/dilution_severity_by_ticker.csv", out_rows)


def write_avoid_tickers_csv_from_severity():
    sev_path = f"{OUTPUT_DIR}/dilution_severity_by_ticker.csv"
    if not os.path.exists(sev_path):
        write_file_text(AVOID_TICKERS_OUT, "")
        return

    with open(sev_path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    if not lines:
        write_file_text(AVOID_TICKERS_OUT, "")
        return

    header = lines[0].split(",")
    idx = {name: i for i, name in enumerate(header)}
    if "ticker" not in idx or "avoid_flag" not in idx:
        write_file_text(AVOID_TICKERS_OUT, "")
        return

    tickers = set()
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = _split_csv_line(line)
        if idx["ticker"] >= len(parts) or idx["avoid_flag"] >= len(parts):
            continue
        tkr = parts[idx["ticker"]].strip().upper()
        af = parts[idx["avoid_flag"]].strip()
        if tkr and af == "1":
            tickers.add(tkr)

    out = []
    for t in sorted(tickers):
        out.append(f"{csv_escape(t)}\n")
    write_file_text(AVOID_TICKERS_OUT, "".join(out))


def _severity_event_key(ticker: str, date_iso: str, filename: str) -> str:
    return f"{(ticker or '').strip().upper()}|{(date_iso or '').strip()}|{(filename or '').strip()}"


def _parse_severity_events_csv(path: str) -> dict:
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    if not lines:
        return {}

    header = lines[0].split(",")
    idx = {name: i for i, name in enumerate(header)}

    out = {}
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = _split_csv_line(line)

        ek = ""
        if "event_key" in idx and idx["event_key"] < len(parts):
            ek = parts[idx["event_key"]].strip()
        if not ek:
            continue

        rec = {}
        for col in SEVERITY_EVENTS_COLUMNS:
            v = ""
            if col in idx and idx[col] < len(parts):
                v = parts[idx[col]].strip()
            rec[col] = v
        out[ek] = rec

    return out


def _write_severity_events_csv(path: str, records_by_key: dict):
    lines = [",".join(SEVERITY_EVENTS_COLUMNS) + "\n"]
    for ek in sorted(records_by_key.keys()):
        rec = records_by_key[ek]
        row = []
        for col in SEVERITY_EVENTS_COLUMNS:
            row.append(csv_escape(rec.get(col, "")))
        lines.append(",".join(row) + "\n")
    write_file_text(path, "".join(lines))


def update_severity_events_master(matched_allowed_all: list[dict], end_date_iso: str, audit: dict) -> list[dict]:
    prior = {}
    prior_source = "none"
    if os.path.exists(SEVERITY_EVENTS_ROOT):
        prior = _parse_severity_events_csv(SEVERITY_EVENTS_ROOT)
        prior_source = "repo_root"
    elif os.path.exists(SEVERITY_EVENTS_OUT):
        prior = _parse_severity_events_csv(SEVERITY_EVENTS_OUT)
        prior_source = "output"

    prior_count = len(prior)

    end_obj = _date_to_obj(end_date_iso)
    cutoff_obj = (end_obj - timedelta(days=179)) if end_obj else None
    cutoff_iso = cutoff_obj.isoformat() if cutoff_obj else ""

    new_events = {}
    new_raw = 0

    for r in (matched_allowed_all or []):
        labels = r.get("labels") or []
        tkr = str(r.get("ticker") or "").strip().upper()
        dstr = str(r.get("date") or "").strip()
        filename = str(r.get("filename") or "").strip()
        if not tkr or not labels:
            continue
        if not dstr or not _date_to_obj(dstr):
            continue
        if not filename:
            continue

        matched_terms = r.get("matched_terms") or []
        cik = str(r.get("cik") or "").strip()
        form_type = str(r.get("form_type") or "").strip()
        filing_url = str(r.get("index_url") or "").strip()

        bank_score = _severity_bank_score(matched_terms)
        term_score = _severity_term_score(matched_terms)
        final_score = _severity_final_filing_score(labels=labels, matched_terms=matched_terms)

        ek = _severity_event_key(tkr, dstr, filename)
        new_raw += 1
        new_events[ek] = {
            "event_key": ek,
            "date": dstr,
            "ticker": tkr,
            "cik": cik,
            "form_type": form_type,
            "filename": filename,
            "filing_url": filing_url,
            "labels": "|".join([str(x) for x in labels]),
            "matched_terms": "|".join([str(x) for x in matched_terms]),
            "bank_score": str(int(bank_score)),
            "term_score": str(int(term_score)),
            "final_filing_score": str(int(final_score)),
        }

    # FIX: "new unique" means "not already present in prior master"
    new_vs_prior = 0
    for ek in new_events.keys():
        if ek not in prior:
            new_vs_prior += 1

    merged = dict(prior)
    for ek in sorted(new_events.keys()):
        merged[ek] = new_events[ek]

    kept = {}
    removed = 0
    for ek in sorted(merged.keys()):
        rec = merged[ek]
        dobj = _date_to_obj(rec.get("date", ""))
        if cutoff_obj and dobj and dobj < cutoff_obj:
            removed += 1
            continue
        if cutoff_obj and not dobj:
            removed += 1
            continue
        kept[ek] = rec

    _write_severity_events_csv(SEVERITY_EVENTS_OUT, kept)

    audit["severity_events_master"] = {
        "prior_source": prior_source,
        "prior_count": prior_count,
        "new_events_raw": new_raw,
        "new_events_unique": new_vs_prior,  # FIXED
        "merged_count": len(merged),
        "removed_pruned_count": removed,
        "final_count": len(kept),
        "cutoff_date_inclusive_180d_window": cutoff_iso,
        "output_path": SEVERITY_EVENTS_OUT,
        "root_path": SEVERITY_EVENTS_ROOT,
    }

    out_list = []
    for ek in sorted(kept.keys()):
        out_list.append(dict(kept[ek]))
    return out_list


def events_to_matched_rows_for_severity(events: list[dict]) -> list[dict]:
    out = []
    for ev in (events or []):
        tkr = str(ev.get("ticker") or "").strip().upper()
        dstr = str(ev.get("date") or "").strip()
        if not tkr or not dstr:
            continue
        labels_s = str(ev.get("labels") or "").strip()
        terms_s = str(ev.get("matched_terms") or "").strip()
        labels = [x for x in labels_s.split("|") if x] if labels_s else []
        terms = [x for x in terms_s.split("|") if x] if terms_s else []
        out.append(
            {
                "date": dstr,
                "ticker": tkr,
                "labels": labels,
                "matched_terms": terms,
                "filename": str(ev.get("filename") or "").strip(),
            }
        )
    out.sort(key=lambda r: (r.get("ticker", ""), r.get("date", ""), r.get("filename", "")))
    return out


def main():
    ensure_output_dir()

    run_time = datetime.now(timezone.utc).isoformat()
    start_date, end_date, date_mode = parse_dates()
    dates = iter_date_range_inclusive(start_date, end_date)

    audit = new_audit(run_time_utc=run_time, start_date=start_date, end_date=end_date, date_mode=date_mode)
    audit_event(audit, "run_start", {"date_mode": date_mode, "start_date": start_date, "end_date": end_date, "days": dates})

    cik_to_ticker = {}
    cik_map_ok = False
    cik_map_error = None
    cik_map_meta = None
    try:
        cik_to_ticker, cik_map_meta = load_cik_to_ticker_map_dual_source()
        cik_map_ok = True
    except Exception as e:
        cik_map_error = str(e)
        cik_to_ticker = {}

    audit_event(
        audit,
        "cik_ticker_map_loaded",
        {"ok": cik_map_ok, "error": cik_map_error, "meta": cik_map_meta, "count": (len(cik_to_ticker) if cik_map_ok else 0)},
    )

    total_parsed_rows = 0
    total_allowed_rows = 0
    total_matched_rows = 0
    total_blank_ticker_rows = 0
    total_scan_fetch_fail = 0
    total_scan_skipped_due_to_size = 0
    total_scan_scanned = 0

    label_counts_total = {}
    verbose_rows_all = []
    run_tickers_all = []

    allowed_filings_all = []
    matched_allowed_all = []

    # PASS 1: candidates
    candidate_ticker_set = set()
    candidate_ticker_sources = 0
    blank_ticker_candidates = 0

    for target_date in dates:
        url = master_idx_url_for_date(target_date)
        try:
            resp = sec_get(url)
            time.sleep(SEC_REQUEST_SLEEP_SECONDS)  # FIX: deterministic sleep

            if resp.status_code != 200 or not resp.content:
                continue
            text = resp.content.decode("latin-1")
            parsed_rows = parse_master_idx(text)

            allowed_rows = []
            for r in parsed_rows:
                if r.form_type in ("S-1", "S-3", "F-3", "8-K") or r.form_type.startswith("424B"):
                    allowed_rows.append(r)

            allowed_rows.sort(key=lambda r: (r.form_type, r.cik, r.filename))

            for r in allowed_rows:
                cik_key = normalize_cik(r.cik)
                t = cik_to_ticker.get(cik_key, "")
                if t:
                    candidate_ticker_set.add(t)
                    candidate_ticker_sources += 1
                else:
                    blank_ticker_candidates += 1
        except Exception:
            continue

    candidate_tickers = sorted(candidate_ticker_set)

    audit_event(
        audit,
        "float_gate_candidates_built",
        {
            "unique_candidate_tickers": len(candidate_tickers),
            "candidate_sources_rows_with_ticker": candidate_ticker_sources,
            "candidate_rows_blank_ticker": blank_ticker_candidates,
        },
    )

    # Float gate
    float_cache = load_float_cache()
    float_gate_api_calls = 0

    pass_rows = []
    fail_rows = []
    unknown_rows = []
    pass_tickers = set()

    for tkr in candidate_tickers:
        cached = float_cache.get(tkr)
        if isinstance(cached, dict) and "float_shares" in cached and "effective_date" in cached and "status" in cached:
            status = str(cached.get("status", "")).strip()
            fs = _safe_int(cached.get("float_shares"))
            ed = str(cached.get("effective_date", "")).strip()
            err = str(cached.get("error", "")).strip()
            source = "cache"

            if status == "pass" and fs is not None:
                pass_tickers.add(tkr)
                pass_rows.append({"ticker": tkr, "float_shares": fs, "effective_date": ed, "status": "pass", "source": source, "error": err})
            elif status == "fail" and fs is not None:
                fail_rows.append({"ticker": tkr, "float_shares": fs, "effective_date": ed, "status": "fail", "source": source, "error": err})
            else:
                unknown_rows.append({"ticker": tkr, "float_shares": (fs if fs is not None else ""), "effective_date": ed, "status": "unknown", "source": source, "error": err})
            continue

        ok, obj, err = massive_get_float_records(tkr)
        float_gate_api_calls += 1

        if not ok or obj is None:
            float_cache[tkr] = {"status": "unknown", "float_shares": None, "effective_date": "", "error": (err or "unknown_error")}
            unknown_rows.append({"ticker": tkr, "float_shares": "", "effective_date": "", "status": "unknown", "source": "api", "error": (err or "")})
            continue

        fs, ed, _records = pick_float_from_massive_response(obj)
        if fs is None:
            float_cache[tkr] = {"status": "unknown", "float_shares": None, "effective_date": "", "error": "no_float_in_response"}
            unknown_rows.append({"ticker": tkr, "float_shares": "", "effective_date": "", "status": "unknown", "source": "api", "error": "no_float_in_response"})
            continue

        if fs <= FLOAT_MAX_SHARES:
            float_cache[tkr] = {"status": "pass", "float_shares": fs, "effective_date": ed, "error": ""}
            pass_tickers.add(tkr)
            pass_rows.append({"ticker": tkr, "float_shares": fs, "effective_date": ed, "status": "pass", "source": "api", "error": ""})
        else:
            float_cache[tkr] = {"status": "fail", "float_shares": fs, "effective_date": ed, "error": ""}
            fail_rows.append({"ticker": tkr, "float_shares": fs, "effective_date": ed, "status": "fail", "source": "api", "error": ""})

    save_float_cache(float_cache)
    write_file_text(FLOAT_PASS_CSV, csv_lines_for_float_gate(sorted(pass_rows, key=lambda r: r["ticker"])))
    write_file_text(FLOAT_FAIL_CSV, csv_lines_for_float_gate(sorted(fail_rows, key=lambda r: r["ticker"])))
    write_file_text(FLOAT_UNKNOWN_CSV, csv_lines_for_float_gate(sorted(unknown_rows, key=lambda r: r["ticker"])))

    audit["float_gate"] = {
        "policy": FLOAT_GATE_POLICY,
        "float_max_shares": FLOAT_MAX_SHARES,
        "unique_candidate_tickers": len(candidate_tickers),
        "api_calls": float_gate_api_calls,
        "pass_tickers": len(pass_tickers),
        "fail_tickers": len(fail_rows),
        "unknown_tickers": len(unknown_rows),
        "cache_path": FLOAT_CACHE_PATH,
        "pass_csv": FLOAT_PASS_CSV,
        "fail_csv": FLOAT_FAIL_CSV,
        "unknown_csv": FLOAT_UNKNOWN_CSV,
        "massive_url_template_present": bool(os.getenv("MASSIVE_FLOAT_URL_TEMPLATE", "").strip()),
    }

    # PASS 2: scan
    for target_date in dates:
        url = master_idx_url_for_date(target_date)

        try:
            resp = sec_get(url)
            time.sleep(SEC_REQUEST_SLEEP_SECONDS)  # FIX: deterministic sleep

            status = resp.status_code
            content = resp.content or b""
            if status != 200 or not content:
                continue

            text = content.decode("latin-1")
            parsed_rows = parse_master_idx(text)

            allowed_rows = []
            for r in parsed_rows:
                if r.form_type in ("S-1", "S-3", "F-3", "8-K") or r.form_type.startswith("424B"):
                    allowed_rows.append(r)

            allowed_rows.sort(key=lambda r: (r.form_type, r.cik, r.filename))

            allowed_filings = []
            for r in allowed_rows:
                cik_key = normalize_cik(r.cik)
                tkr = cik_to_ticker.get(cik_key, "")
                if not tkr:
                    continue
                if tkr not in pass_tickers:
                    continue

                allowed_filings.append(
                    {
                        "date": target_date,
                        "cik": r.cik,
                        "company": r.company,
                        "form_type": r.form_type,
                        "date_filed": r.date_filed,
                        "filename": r.filename,
                        "index_url": f"https://www.sec.gov/Archives/{r.filename}",
                        "ticker": tkr,
                    }
                )

            allowed_filings.sort(key=lambda x: (x["form_type"], x["cik"], x["filename"]))
            allowed_filings_all.extend(allowed_filings)

            for item in allowed_filings:
                filing = FilingRef(
                    cik=item["cik"],
                    company=item["company"],
                    form_type=item["form_type"],
                    date_filed=item["date_filed"],
                    filename=item["filename"],
                    index_url=item["index_url"],
                )

                ok, content_bytes, err_str, http_status = fetch_primary_filing_text(filing=filing, user_agent=SEC_USER_AGENT)

                bytes_len = (len(content_bytes) if content_bytes is not None else 0)
                skipped_due_to_size = False
                labels = []
                matched_terms = []

                if ok and content_bytes is not None:
                    skipped_due_to_size = bytes_len > MAX_SAMPLE_BYTES
                    if not skipped_due_to_size:
                        filing_text = content_bytes.decode("utf-8", errors="replace")
                        labels, matched_terms = scan_filing_text_for_labels(filing_text)

                ticker_val = item.get("ticker", "")

                matched_allowed_all.append(
                    {
                        "date": target_date,
                        "cik": filing.cik,
                        "ticker": ticker_val,
                        "company": filing.company,
                        "form_type": filing.form_type,
                        "date_filed": filing.date_filed,
                        "filename": filing.filename,
                        "index_url": filing.index_url,
                        "labels": labels,
                        "matched_terms": matched_terms,
                    }
                )

                if labels:
                    free_float_shares = ""
                    cached = float_cache.get(ticker_val)
                    if isinstance(cached, dict):
                        fs = _safe_int(cached.get("float_shares"))
                        if fs is not None:
                            free_float_shares = str(fs)

                    row = {
                        "date": target_date,
                        "ticker": ticker_val,
                        "cik": filing.cik,
                        "company": filing.company,
                        "form_type": filing.form_type,
                        "accession": accession_from_filename(filing.filename),
                        "filing_url": filing.index_url,
                        "free_float_shares": free_float_shares,
                        "labels": "|".join(labels),
                        "matched_terms": "|".join(matched_terms),
                    }

                    line = ",".join([csv_escape(row.get(col, "")) for col in VERBOSE_COLUMNS]) + "\n"
                    verbose_rows_all.append(line)

                    if ticker_val:
                        run_tickers_all.append(ticker_val)

        except Exception:
            continue

    # Write core artifacts
    write_file_text(f"{OUTPUT_DIR}/allowed_filings.json", json.dumps(allowed_filings_all, indent=2))
    write_file_text(f"{OUTPUT_DIR}/matched_allowed_filings.json", json.dumps(matched_allowed_all, indent=2))

    # severity from events master
    events_master = update_severity_events_master(matched_allowed_all=matched_allowed_all, end_date_iso=end_date, audit=audit)
    severity_input_rows = events_to_matched_rows_for_severity(events_master)
    build_dilution_severity_by_ticker(matched_allowed_all=severity_input_rows, end_date_iso=end_date)
    write_avoid_tickers_csv_from_severity()

    verbose_header = ",".join(VERBOSE_COLUMNS) + "\n"
    write_file_text(f"{OUTPUT_DIR}/dilution_tickers_verbose.csv", verbose_header + "".join(verbose_rows_all))
    write_ticker_list(f"{OUTPUT_DIR}/dilution_tickers.csv", run_tickers_all)

    # Bootstrap / update persistent _all files so clean rebuilds recreate repo-root masters.
    existing_all = set()

    if os.path.exists(ALL_TICKERS_ROOT):
        with open(ALL_TICKERS_ROOT, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip().upper()
                if t:
                    existing_all.add(t)

    current_run_set = set([t.strip().upper() for t in run_tickers_all if t and t.strip()])
    merged_all = sorted(existing_all.union(current_run_set))

    write_file_text(ALL_TICKERS_OUT, "\n".join(merged_all) + ("\n" if merged_all else ""))
    write_file_text(ALL_TICKERS_ROOT, "\n".join(merged_all) + ("\n" if merged_all else ""))

    prior_verbose = _parse_all_verbose_csv(ALL_VERBOSE_ROOT)
    today = end_date  # use run end date so historical backfills stay historically correct

    for line in verbose_rows_all:
        parts = _split_csv_line(line.strip())
        if len(parts) < len(VERBOSE_COLUMNS):
            continue

        ticker = parts[VERBOSE_COLUMNS.index("ticker")].strip().upper()
        labels = parts[VERBOSE_COLUMNS.index("labels")] if "labels" in VERBOSE_COLUMNS else ""
        filing_url = parts[VERBOSE_COLUMNS.index("filing_url")] if "filing_url" in VERBOSE_COLUMNS else ""

        if not ticker:
            continue

        if ticker in prior_verbose:
            rec = prior_verbose[ticker]
            rec["last_seen_date"] = today
            try:
                rec["seen_count"] = str(int(rec.get("seen_count", "0")) + 1)
            except Exception:
                rec["seen_count"] = "1"
            rec["last_labels"] = labels
            rec["last_filing_url"] = filing_url
        else:
            prior_verbose[ticker] = {
                "ticker": ticker,
                "first_seen_date": today,
                "last_seen_date": today,
                "seen_count": "1",
                "last_labels": labels,
                "last_filing_url": filing_url,
            }

    _write_all_verbose_csv(ALL_VERBOSE_OUT, prior_verbose)
    _write_all_verbose_csv(ALL_VERBOSE_ROOT, prior_verbose)

    # Minimal audit output for this step
    write_file_text(f"{OUTPUT_DIR}/audit_log.json", json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
