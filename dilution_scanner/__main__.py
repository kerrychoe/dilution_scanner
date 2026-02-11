import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

from dilution_scanner.master_idx_parser import parse_master_idx

OUTPUT_DIR = "output"

# LOCKED form allowlist (deterministic)
ALLOWED_FORMS = [
    "424B",
    "S-3",
    "S-1",
    "F-3",
    "8-K",
]

# SEC requires a descriptive User-Agent with real contact email
SEC_USER_AGENT = "DilutionTickerScanner/1.0 (contact: kerrychoe@gmail.com)"
SEC_CONTACT_EMAIL = "kerrychoe@gmail.com"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_file_bytes(path, content_bytes: bytes):
    with open(path, "wb") as f:
        f.write(content_bytes)


def write_file_text(path, content_text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content_text)


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


def sec_get(url: str, timeout_sec: int = 30) -> requests.Response:
    """
    Deterministic SEC GET:
    - Fixed identifying headers (User-Agent + From)
    - Identity encoding (no compression)
    - Basic retry with fixed backoff
    """
    headers = {
        "User-Agent": SEC_USER_AGENT,
        "From": SEC_CONTACT_EMAIL,
        "Accept": "text/plain,application/octet-stream,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Connection": "close",
        "Referer": "https://www.sec.gov/",
    }

    last_exc = None
    for attempt in range(1, 4):  # 3 attempts max
        try:
            resp = requests.get(url, headers=headers, timeout=timeout_sec)
            return resp
        except Exception as e:
            last_exc = e
            # fixed backoff: 1s, 2s, 3s
            time.sleep(attempt)

    raise RuntimeError(f"SEC GET failed after 3 attempts: {url}") from last_exc


def master_idx_url_for_date(date_iso: str) -> str:
    """
    date_iso: YYYY-MM-DD
    SEC daily index path format:
      https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTR{1-4}/master.YYYYMMDD.idx
    """
    year, month, day = date_iso.split("-")
    y = int(year)
    m = int(month)
    qtr = (m - 1) // 3 + 1
    yyyymmdd = f"{year}{month}{day}"
    return f"https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{qtr}/master.{yyyymmdd}.idx"


def main():
    ensure_output_dir()

    run_time = datetime.now(timezone.utc).isoformat()
    start_date, end_date, date_mode = parse_dates()

    # For now: fetch ONLY the start_date master.idx
    target_date = start_date
    url = master_idx_url_for_date(target_date)

    fetch_status = None
    fetch_ok = False
    fetched_bytes_len = 0
    error = None

    parsed_row_count = 0
    allowed_row_count = 0

    try:
        resp = sec_get(url)
        fetch_status = resp.status_code
        content = resp.content
        fetched_bytes_len = len(content)

        if resp.status_code != 200:
            # Save body for debugging (deterministic)
            write_file_bytes(f"{OUTPUT_DIR}/master_idx_error_body.bin", content)

            # Also save a UTF-8 preview (first 2000 chars) for easy inspection
            try:
                preview = content.decode("utf-8", errors="replace")
            except Exception:
                preview = "<decode_failed>"
            preview = preview[:2000]
            write_file_text(f"{OUTPUT_DIR}/master_idx_error_body.txt", preview)

        if resp.status_code == 200 and fetched_bytes_len > 0:
            write_file_bytes(f"{OUTPUT_DIR}/master.idx", content)
            fetch_ok = True

            text = content.decode("latin-1")
            parsed_rows = parse_master_idx(text)
            parsed_row_count = len(parsed_rows)

            # Deterministic allowlist filtering (NO other logic yet)
            allowed_rows = []
            for r in parsed_rows:
                # - exact match for S-1, S-3, F-3, 8-K
                # - prefix match for 424B* via "424B"
                if r.form_type in ("S-1", "S-3", "F-3", "8-K") or r.form_type.startswith("424B"):
                    allowed_rows.append(r)

            allowed_row_count = len(allowed_rows)
        else:
            error = f"Non-200 or empty body (status={resp.status_code}, bytes={fetched_bytes_len})"
    except Exception as e:
        error = str(e)

    # Placeholder outputs still created
    write_file_text(
        f"{OUTPUT_DIR}/dilution_tickers_verbose.csv",
        "date,ticker,cik,company,form_type,accession,filing_url,free_float_shares,labels,matched_terms\n",
    )
    write_file_text(f"{OUTPUT_DIR}/dilution_tickers.csv", "")
    write_file_text(f"{OUTPUT_DIR}/dilution_tickers_all.csv", "")
    write_file_text(f"{OUTPUT_DIR}/audit_log.json", json.dumps([], indent=2))

    run_meta = {
        "run_timestamp_utc": run_time,
        "scan_start_date": start_date,
        "scan_end_date": end_date,
        "date_mode": date_mode,
        "allowed_forms": ALLOWED_FORMS,
        "sec_user_agent": SEC_USER_AGENT,
        "master_idx_parsed_rows": parsed_row_count,
        "master_idx_allowed_rows": allowed_row_count,
        "master_idx_fetch": {
            "date": target_date,
            "url": url,
            "ok": fetch_ok,
            "status": fetch_status,
            "bytes": fetched_bytes_len,
            "error": error,
            "saved_path": "output/master.idx" if fetch_ok else None,
            "error_body_preview_path": "output/master_idx_error_body.txt" if not fetch_ok else None,
        },
        "status": "step25_form_allowlist_count",
    }

    write_file_text(f"{OUTPUT_DIR}/run_metadata.json", json.dumps(run_meta, indent=2))

    print(f"Master idx URL: {url}")
    print(f"Fetch ok={fetch_ok}, status={fetch_status}, bytes={fetched_bytes_len}")
    print(f"Parsed rows={parsed_row_count}, Allowed rows={allowed_row_count}")
    if error:
        print(f"Error: {error}")


if __name__ == "__main__":
    main()
