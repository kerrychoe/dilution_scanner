import os
import json
import time
import requests

from datetime import datetime, timedelta, timezone, date
from dilution_scanner.master_idx_parser import parse_master_idx
from dilution_scanner.filings import FilingRef, fetch_primary_filing_text, filing_artifact_basename
from dilution_scanner.rules import scan_filing_text_for_labels

SYSTEM_VERSION = "1.0.0"

OUTPUT_DIR = "output"

# Deterministic cap for filings we parse/scan
MAX_SAMPLE_BYTES = 2_000_000  # 2 MB

# FLOAT GATE (LOCKED)
FLOAT_MAX_SHARES = 10_000_000
FLOAT_GATE_POLICY = "strict_tradeable_only"  # locked

# STALE PRUNE (LOCKED)
STALE_DAYS = 180

# LOCKED form allowlist (deterministic)
ALLOWED_FORMS = ["424B", "S-3", "S-1", "F-3", "8-K"]

# SEC requires a descriptive User-Agent with real contact email
SEC_USER_AGENT = "DilutionTickerScanner/1.0 (contact: kerrychoe@gmail.com)"
SEC_CONTACT_EMAIL = "kerrychoe@gmail.com"

# LOCKED verbose CSV columns (deterministic order)
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

# Float Gate artifacts
FLOAT_CACHE_PATH = f"{OUTPUT_DIR}/float_cache.json"
FLOAT_PASS_CSV = f"{OUTPUT_DIR}/float_gate_pass.csv"
FLOAT_FAIL_CSV = f"{OUTPUT_DIR}/float_gate_fail.csv"
FLOAT_UNKNOWN_CSV = f"{OUTPUT_DIR}/float_gate_unknown.csv"

# Persistent aggregate artifacts (repo root persisted; output written each run)
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


# -----------------------------
# AUDIT LOG HELPERS
# -----------------------------
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
    }


def audit_event(audit: dict, event: str, data: dict | None = None):
    audit["events"].append({"event": event, "data": (data or {})})


# -----------------------------
# CIK -> TICKER MAP
# -----------------------------
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


# -----------------------------
# TICKER LIST HELPERS
# -----------------------------
def read_ticker_list(path: str) -> list:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip().upper()
            if t:
                out.append(t)
    return out


def write_ticker_list(path: str, tickers: list):
    uniq = sorted(set([t.strip().upper() for t in tickers if t and t.strip()]))
    write_file_text(path, "\n".join(uniq) + ("\n" if uniq else ""))


# -----------------------------
# PERSISTENT ALL-VERBOSE (SOURCE OF TRUTH)
# -----------------------------
def _parse_all_verbose_csv(path: str) -> dict:
    """
    Returns dict[ticker] = record dict (columns in ALL_VERBOSE_COLUMNS)
    Deterministic: ignores unknown columns; last duplicate row wins (stable due to file order).
    """
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
        # Minimal CSV parsing (values in our file contain no commas except possibly labels/url;
        # we avoid commas by using | in labels and plain URL. Still, for safety, handle quoted commas naively.)
        parts = []
        cur = ""
        in_q = False
        i = 0
        while i < len(line):
            ch = line[i]
            if ch == '"' and (i == 0 or line[i - 1] != "\\"):
                # CSV quotes: toggle unless doubled quote
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
    """
    records: dict[ticker] -> rec
    Deterministic ordering: ticker ascending
    """
    lines = [",".join(ALL_VERBOSE_COLUMNS) + "\n"]
    for tkr in sorted(records.keys()):
        rec = records[tkr]
        row = []
        for col in ALL_VERBOSE_COLUMNS:
            row.append(csv_escape(rec.get(col, "")))
        lines.append(",".join(row) + "\n")
    write_file_text(path, "".join(lines))


def _iso_today_from_end_date(end_date_iso: str) -> str:
    return end_date_iso


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


# -----------------------------
# MASSIVE FLOAT GATE
# -----------------------------
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
        "Authorization": f"Bearer {api_key}",  # Massive supports this, per your docs
        "User-Agent": "DilutionTickerScanner/1.0",
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


def main():
    ensure_output_dir()

    run_time = datetime.now(timezone.utc).isoformat()
    start_date, end_date, date_mode = parse_dates()
    dates = iter_date_range_inclusive(start_date, end_date)

    audit = new_audit(run_time_utc=run_time, start_date=start_date, end_date=end_date, date_mode=date_mode)
    audit_event(audit, "run_start", {"date_mode": date_mode, "start_date": start_date, "end_date": end_date, "days": dates})

    # Load cik->ticker map (once)
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

    # RANGE totals
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

    error_rows_all = []

    # -----------------------------
    # PASS 1: Build candidate tickers across entire range
    # -----------------------------
    audit_event(audit, "float_gate_pass1_start", {"policy": FLOAT_GATE_POLICY, "float_max_shares": FLOAT_MAX_SHARES})

    candidate_ticker_set = set()
    candidate_ticker_sources = 0
    blank_ticker_candidates = 0

    for target_date in dates:
        url = master_idx_url_for_date(target_date)
        try:
            resp = sec_get(url)
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

    # -----------------------------
    # Float gate lookup (no persistence required; cache only within run)
    # -----------------------------
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

    audit_event(
        audit,
        "float_gate_complete",
        {
            "unique_candidate_tickers": len(candidate_tickers),
            "api_calls": float_gate_api_calls,
            "pass_tickers": len(pass_tickers),
            "fail_tickers": len(fail_rows),
            "unknown_tickers": len(unknown_rows),
        },
    )

    # -----------------------------
    # PASS 2: Scan filings ONLY for pass_tickers
    # -----------------------------
    for target_date in dates:
        url = master_idx_url_for_date(target_date)

        day_info = {
            "date": target_date,
            "master_idx": {"url": url, "ok": False, "status": None, "bytes": 0, "saved_path": None, "error": None},
            "counts": {
                "parsed_rows": 0,
                "allowed_rows": 0,
                "allowed_rows_after_float_gate": 0,
                "matched_rows": 0,
                "blank_ticker_rows": 0,
                "scan_fetch_fail": 0,
                "scan_skipped_due_to_size": 0,
                "scan_scanned": 0,
                "float_gate_skipped_rows": 0,
            },
            "label_counts": {},
        }

        audit_event(audit, "day_start", {"date": target_date, "url": url})

        try:
            resp = sec_get(url)
            status = resp.status_code
            content = resp.content or b""
            fetched_bytes_len = len(content)

            day_info["master_idx"]["status"] = status
            day_info["master_idx"]["bytes"] = fetched_bytes_len

            if status != 200 or fetched_bytes_len <= 0:
                day_info["master_idx"]["ok"] = False
                day_info["master_idx"]["error"] = f"Non-200 or empty body (status={status}, bytes={fetched_bytes_len})"
                audit_event(audit, "master_idx_fetched", {"date": target_date, "ok": False, "status": status, "bytes": fetched_bytes_len, "url": url})
                audit["by_date"].append(day_info)
                audit_event(audit, "day_end", {"date": target_date, "ok": False, "error": day_info["master_idx"]["error"]})
                continue

            master_path = f"{OUTPUT_DIR}/master_{target_date}.idx"
            write_file_bytes(master_path, content)
            day_info["master_idx"]["ok"] = True
            day_info["master_idx"]["saved_path"] = master_path

            audit_event(audit, "master_idx_fetched", {"date": target_date, "ok": True, "status": status, "bytes": fetched_bytes_len, "url": url, "saved_path": master_path})

            text = content.decode("latin-1")
            parsed_rows = parse_master_idx(text)
            parsed_row_count = len(parsed_rows)

            allowed_rows = []
            for r in parsed_rows:
                if r.form_type in ("S-1", "S-3", "F-3", "8-K") or r.form_type.startswith("424B"):
                    allowed_rows.append(r)

            allowed_row_count = len(allowed_rows)
            day_info["counts"]["parsed_rows"] = parsed_row_count
            day_info["counts"]["allowed_rows"] = allowed_row_count

            audit_event(audit, "master_idx_parsed", {"date": target_date, "parsed_rows": parsed_row_count, "allowed_rows": allowed_row_count})

            allowed_rows.sort(key=lambda r: (r.form_type, r.cik, r.filename))

            allowed_filings = []
            float_gate_skipped = 0

            for r in allowed_rows:
                cik_key = normalize_cik(r.cik)
                tkr = cik_to_ticker.get(cik_key, "")
                if not tkr:
                    float_gate_skipped += 1
                    continue
                if tkr not in pass_tickers:
                    float_gate_skipped += 1
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

            day_info["counts"]["float_gate_skipped_rows"] = float_gate_skipped
            day_info["counts"]["allowed_rows_after_float_gate"] = len(allowed_filings)

            allowed_filings_all.extend(allowed_filings)

            label_counts_day = {}
            matched_rows_day = 0
            blank_ticker_day = 0

            scan_fetch_fail_day = 0
            scan_skipped_due_to_size_day = 0
            scan_scanned_day = 0

            for item in allowed_filings:
                filing = FilingRef(
                    cik=item["cik"],
                    company=item["company"],
                    form_type=item["form_type"],
                    date_filed=item["date_filed"],
                    filename=item["filename"],
                    index_url=item["index_url"],
                )

                ok, content_bytes, err_str, http_status = fetch_primary_filing_text(
                    filing=filing,
                    user_agent=SEC_USER_AGENT,
                )

                bytes_len = (len(content_bytes) if content_bytes is not None else 0)
                skipped_due_to_size = False
                labels = []
                matched_terms = []

                if ok and content_bytes is not None:
                    skipped_due_to_size = bytes_len > MAX_SAMPLE_BYTES
                    if not skipped_due_to_size:
                        filing_text = content_bytes.decode("utf-8", errors="replace")
                        labels, matched_terms = scan_filing_text_for_labels(filing_text)

                if not ok:
                    scan_fetch_fail_day += 1
                    error_rows_all.append(
                        {
                            "date": target_date,
                            "cik": filing.cik,
                            "form_type": filing.form_type,
                            "filename": filing.filename,
                            "http_status": http_status,
                            "error": err_str,
                        }
                    )

                if skipped_due_to_size:
                    scan_skipped_due_to_size_day += 1
                elif ok and content_bytes is not None:
                    scan_scanned_day += 1

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
                        "fetch_ok": ok,
                        "http_status": http_status,
                        "bytes": bytes_len,
                        "skipped_due_to_size": skipped_due_to_size,
                        "labels": labels,
                        "matched_terms": matched_terms,
                        "error": err_str,
                    }
                )

                if labels:
                    matched_rows_day += 1
                    if not ticker_val:
                        blank_ticker_day += 1

                    for lab in labels:
                        label_counts_day[lab] = label_counts_day.get(lab, 0) + 1
                        label_counts_total[lab] = label_counts_total.get(lab, 0) + 1

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

            day_info["counts"]["matched_rows"] = matched_rows_day
            day_info["counts"]["blank_ticker_rows"] = blank_ticker_day
            day_info["counts"]["scan_fetch_fail"] = scan_fetch_fail_day
            day_info["counts"]["scan_skipped_due_to_size"] = scan_skipped_due_to_size_day
            day_info["counts"]["scan_scanned"] = scan_scanned_day
            day_info["label_counts"] = {k: label_counts_day[k] for k in sorted(label_counts_day.keys())}

            audit_event(
                audit,
                "scan_complete_day",
                {
                    "date": target_date,
                    "matched_rows": matched_rows_day,
                    "blank_ticker_rows": blank_ticker_day,
                    "fetch_fail": scan_fetch_fail_day,
                    "skipped_due_to_size": scan_skipped_due_to_size_day,
                    "scanned": scan_scanned_day,
                    "float_gate_skipped_rows": float_gate_skipped,
                    "allowed_rows_after_float_gate": len(allowed_filings),
                },
            )

            audit["by_date"].append(day_info)
            audit_event(audit, "day_end", {"date": target_date, "ok": True})

            total_parsed_rows += parsed_row_count
            total_allowed_rows += allowed_row_count
            total_matched_rows += matched_rows_day
            total_blank_ticker_rows += blank_ticker_day
            total_scan_fetch_fail += scan_fetch_fail_day
            total_scan_skipped_due_to_size += scan_skipped_due_to_size_day
            total_scan_scanned += scan_scanned_day

        except Exception as e:
            day_info["master_idx"]["ok"] = False
            day_info["master_idx"]["error"] = str(e)
            audit_event(audit, "day_error", {"date": target_date, "error": str(e)})
            audit["by_date"].append(day_info)
            audit_event(audit, "day_end", {"date": target_date, "ok": False, "error": str(e)})
            continue

    # -----------------------------
    # WRITE RANGE OUTPUTS
    # -----------------------------
    write_file_text(f"{OUTPUT_DIR}/allowed_filings.json", json.dumps(allowed_filings_all, indent=2))
    write_file_text(f"{OUTPUT_DIR}/matched_allowed_filings.json", json.dumps(matched_allowed_all, indent=2))

    verbose_header = ",".join(VERBOSE_COLUMNS) + "\n"
    write_file_text(f"{OUTPUT_DIR}/dilution_tickers_verbose.csv", verbose_header + "".join(verbose_rows_all))

    write_ticker_list(f"{OUTPUT_DIR}/dilution_tickers.csv", run_tickers_all)

    # -----------------------------
    # PERSISTENT MASTER (VERBOSE) + STALE PRUNE
    # Source of truth: dilution_tickers_all_verbose.csv (repo root)
    # Derived: dilution_tickers_all.csv (ticker-only, pruned)
    # -----------------------------
    # Load existing verbose master from repo root if present; else from output; else empty
    prior_verbose = {}
    if os.path.exists(ALL_VERBOSE_ROOT):
        prior_verbose = _parse_all_verbose_csv(ALL_VERBOSE_ROOT)
        prior_source = "repo_root"
    elif os.path.exists(ALL_VERBOSE_OUT):
        prior_verbose = _parse_all_verbose_csv(ALL_VERBOSE_OUT)
        prior_source = "output"
    else:
        prior_verbose = {}
        prior_source = "none"

    # Build run-level per-ticker updates from matched_allowed_all (deterministic)
    # We only consider rows with labels and non-empty ticker (strict policy ensures this)
    run_updates = {}  # ticker -> {last_seen_date, last_labels, last_filing_url, seen_increment}
    for r in matched_allowed_all:
        labels = r.get("labels") or []
        tkr = str(r.get("ticker") or "").strip().upper()
        filing_date = str(r.get("date") or "").strip()
        filing_url = str(r.get("index_url") or "").strip()
        if not tkr or not labels:
            continue
        lab_str = "|".join(labels)

        u = run_updates.get(tkr)
        if u is None:
            run_updates[tkr] = {
                "last_seen_date": filing_date,
                "last_labels": lab_str,
                "last_filing_url": filing_url,
                "seen_increment": 1,
            }
        else:
            # last_seen_date = max date (ISO sortable)
            if filing_date and filing_date > u["last_seen_date"]:
                u["last_seen_date"] = filing_date
                u["last_labels"] = lab_str
                u["last_filing_url"] = filing_url
            u["seen_increment"] += 1

    # Merge into master
    merged = dict(prior_verbose)
    for tkr in sorted(run_updates.keys()):
        upd = run_updates[tkr]
        existing = merged.get(tkr)
        if existing is None:
            merged[tkr] = {
                "ticker": tkr,
                "first_seen_date": upd["last_seen_date"],
                "last_seen_date": upd["last_seen_date"],
                "seen_count": str(upd["seen_increment"]),
                "last_labels": upd["last_labels"],
                "last_filing_url": upd["last_filing_url"],
            }
        else:
            first_seen = existing.get("first_seen_date", "") or upd["last_seen_date"]
            last_seen = existing.get("last_seen_date", "")
            if not last_seen or upd["last_seen_date"] > last_seen:
                last_seen = upd["last_seen_date"]
                existing["last_labels"] = upd["last_labels"]
                existing["last_filing_url"] = upd["last_filing_url"]

            sc = _safe_int(existing.get("seen_count"))
            if sc is None:
                sc = 0
            sc += int(upd["seen_increment"])
            merged[tkr] = {
                "ticker": tkr,
                "first_seen_date": first_seen,
                "last_seen_date": last_seen,
                "seen_count": str(sc),
                "last_labels": existing.get("last_labels", ""),
                "last_filing_url": existing.get("last_filing_url", ""),
            }

    # Prune stale: last_seen_date < (END_DATE - STALE_DAYS)
    end_obj = _date_to_obj(end_date)
    cutoff_obj = (end_obj - timedelta(days=STALE_DAYS)) if end_obj else None
    cutoff_iso = cutoff_obj.isoformat() if cutoff_obj else ""

    before_count = len(merged)
    removed = []
    kept = {}

    for tkr in sorted(merged.keys()):
        rec = merged[tkr]
        last_seen = rec.get("last_seen_date", "").strip()
        last_obj = _date_to_obj(last_seen)
        if cutoff_obj and last_obj and last_obj < cutoff_obj:
            removed.append(tkr)
        elif cutoff_obj and (not last_obj):
            # deterministic: if last_seen_date is missing/unparseable, drop (strict hygiene)
            removed.append(tkr)
        else:
            kept[tkr] = rec

    after_count = len(kept)

    # Write pruned verbose master to output
    _write_all_verbose_csv(ALL_VERBOSE_OUT, kept)

    # Derived pruned ticker-only list
    active_tickers = sorted(kept.keys())
    write_ticker_list(ALL_TICKERS_OUT, active_tickers)

    audit["stale_prune"] = {
        "stale_days": STALE_DAYS,
        "end_date": end_date,
        "cutoff_date_inclusive": cutoff_iso,
        "prior_source": prior_source,
        "prior_master_count": len(prior_verbose),
        "merged_master_count": before_count,
        "removed_stale_count": len(removed),
        "active_master_count": after_count,
    }

    audit_event(
        audit,
        "all_tickers_master_updated",
        {
            "stale_days": STALE_DAYS,
            "cutoff_date": cutoff_iso,
            "prior_source": prior_source,
            "prior_master_count": len(prior_verbose),
            "merged_master_count": before_count,
            "removed_stale_count": len(removed),
            "active_master_count": after_count,
        },
    )

    # -----------------------------
    # LABEL SUMMARY + AUDIT
    # -----------------------------
    summary = {
        "start_date": start_date,
        "end_date": end_date,
        "matched_rows": total_matched_rows,
        "blank_ticker_rows": total_blank_ticker_rows,
        "label_counts": {k: label_counts_total[k] for k in sorted(label_counts_total.keys())},
        "float_gate": audit.get("float_gate", {}),
        "stale_prune": audit.get("stale_prune", {}),
    }
    write_file_text(f"{OUTPUT_DIR}/label_summary.json", json.dumps(summary, indent=2))

    summary_csv_lines = ["label,count\n"]
    for k in sorted(label_counts_total.keys()):
        summary_csv_lines.append(f"{csv_escape(k)},{label_counts_total[k]}\n")
    write_file_text(f"{OUTPUT_DIR}/label_summary.csv", "".join(summary_csv_lines))

    audit["counts"] = {
        "parsed_rows": total_parsed_rows,
        "allowed_rows": total_allowed_rows,
        "matched_rows": total_matched_rows,
        "blank_ticker_rows": total_blank_ticker_rows,
        "scan_fetch_fail": total_scan_fetch_fail,
        "scan_skipped_due_to_size": total_scan_skipped_due_to_size,
        "scan_scanned": total_scan_scanned,
    }

    audit_event(
        audit,
        "range_scan_complete",
        {
            "start_date": start_date,
            "end_date": end_date,
            "days": dates,
            "matched_rows": total_matched_rows,
            "blank_ticker_rows": total_blank_ticker_rows,
            "fetch_fail": total_scan_fetch_fail,
            "skipped_due_to_size": total_scan_skipped_due_to_size,
            "scanned": total_scan_scanned,
            "float_gate": audit.get("float_gate", {}),
            "stale_prune": audit.get("stale_prune", {}),
        },
    )

    write_file_text(f"{OUTPUT_DIR}/audit_log.json", json.dumps(audit, indent=2))

    # Sample fetch kept (unchanged)
    os.makedirs(f"{OUTPUT_DIR}/filings_raw", exist_ok=True)
    sample_n = 3
    sample = []
    seen_ciks = set()
    for item in allowed_filings_all:
        cik = item["cik"]
        if cik in seen_ciks:
            continue
        seen_ciks.add(cik)
        sample.append(item)
        if len(sample) >= sample_n:
            break

    sample_results = []
    for item in sample:
        filing = FilingRef(
            cik=item["cik"],
            company=item["company"],
            form_type=item["form_type"],
            date_filed=item["date_filed"],
            filename=item["filename"],
            index_url=item["index_url"],
        )

        ok, content_bytes, err_str, http_status = fetch_primary_filing_text(filing=filing, user_agent=SEC_USER_AGENT)
        out_name = filing_artifact_basename(filing)
        out_path = f"{OUTPUT_DIR}/filings_raw/{out_name}"

        bytes_len = (len(content_bytes) if content_bytes is not None else 0)
        skipped_due_to_size = False
        saved_path = None
        labels = []
        matched_terms = []

        if ok and content_bytes is not None:
            skipped_due_to_size = bytes_len > MAX_SAMPLE_BYTES
            if not skipped_due_to_size:
                write_file_bytes(out_path, content_bytes)
                saved_path = out_path
                filing_text = content_bytes.decode("utf-8", errors="replace")
                labels, matched_terms = scan_filing_text_for_labels(filing_text)

        sample_results.append(
            {
                "cik": filing.cik,
                "company": filing.company,
                "form_type": filing.form_type,
                "date_filed": filing.date_filed,
                "filename": filing.filename,
                "index_url": filing.index_url,
                "fetch_ok": ok,
                "http_status": http_status,
                "bytes": bytes_len,
                "skipped_due_to_size": skipped_due_to_size,
                "labels": labels,
                "matched_terms": matched_terms,
                "error": err_str,
                "saved_path": (saved_path if saved_path else None),
            }
        )

    write_file_text(f"{OUTPUT_DIR}/sample_filing_fetch.json", json.dumps(sample_results, indent=2))

    run_meta = {
        "system_version": SYSTEM_VERSION,        
        "run_timestamp_utc": run_time,
        "scan_start_date": start_date,
        "scan_end_date": end_date,
        "date_mode": date_mode,
        "scan_days": dates,
        "allowed_forms": ALLOWED_FORMS,
        "sec_user_agent": SEC_USER_AGENT,
        "float_gate": audit.get("float_gate", {}),
        "stale_prune": audit.get("stale_prune", {}),
        "range_totals": audit.get("counts", {}),
        "status": "stale_prune_v1_all_verbose",
    }
    write_file_text(f"{OUTPUT_DIR}/run_metadata.json", json.dumps(run_meta, indent=2))

    print(f"Scan range: {start_date} .. {end_date} (days={len(dates)})")
    print(f"Float gate: policy={FLOAT_GATE_POLICY}, max_shares={FLOAT_MAX_SHARES}")
    sp = audit.get("stale_prune", {})
    print(f"Stale prune: stale_days={STALE_DAYS}, cutoff={sp.get('cutoff_date_inclusive')}, active_master={sp.get('active_master_count')}, removed={sp.get('removed_stale_count')}")


if __name__ == "__main__":
    main()
