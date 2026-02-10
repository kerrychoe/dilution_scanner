import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

OUTPUT_DIR = "output"

# LOCKED form allowlist (deterministic)
ALLOWED_FORMS = [
    "424B",
    "S-3",
    "S-1",
    "F-3",
    "8-K",
]

# SEC requires a descriptive User-Agent (include email)
SEC_USER_AGENT = "DilutionTickerScanner/1.0 (contact: kerrychoe@gmail.com)"

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

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
    - Fixed User-Agent
    - Basic retry with fixed backoff
    - No heuristics, no random jitter
    """
    headers = {
        "User-Agent": SEC_USER_AGENT,
        "Accept-Encoding": "identity",
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

def main():
    ensure_output_dir()

    run_time = datetime.now(timezone.utc).isoformat()
    start_date, end_date, date_mode = parse_dates()

    # Placeholder outputs
    write_file(
        f"{OUTPUT_DIR}/dilution_tickers_verbose.csv",
        "date,ticker,cik,company,form_type,accession,filing_url,free_float_shares,labels,matched_terms\n",
    )
    write_file(f"{OUTPUT_DIR}/dilution_tickers.csv", "")
    write_file(f"{OUTPUT_DIR}/dilution_tickers_all.csv", "")
    write_file(f"{OUTPUT_DIR}/audit_log.json", json.dumps([], indent=2))

    write_file(
        f"{OUTPUT_DIR}/run_metadata.json",
        json.dumps(
            {
                "run_timestamp_utc": run_time,
                "scan_start_date": start_date,
                "scan_end_date": end_date,
                "date_mode": date_mode,
                "allowed_forms": ALLOWED_FORMS,
                "sec_user_agent": SEC_USER_AGENT,
                "status": "placeholder",
            },
            indent=2,
        ),
    )

    print(f"Scan window: {start_date} → {end_date} ({date_mode})")
    print(f"Allowed forms: {', '.join(ALLOWED_FORMS)}")
    print("SEC HTTP helper installed (not used yet).")
    print("Placeholder output files written successfully.")

if __name__ == "__main__":
    main()
