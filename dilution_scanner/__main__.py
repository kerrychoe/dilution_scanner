import os
import json
from datetime import datetime, timedelta, timezone

OUTPUT_DIR = "output"

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def parse_dates():
    """
    Deterministic date handling:
    - If START_DATE and END_DATE are provided, use them.
    - Otherwise, default to yesterday (UTC).
    """
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

def main():
    ensure_output_dir()

    run_time = datetime.now(timezone.utc).isoformat()
    start_date, end_date, date_mode = parse_dates()

    # Placeholder outputs (deterministic structure)
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
                "status": "placeholder",
            },
            indent=2,
        ),
    )

    print(f"Scan window: {start_date} → {end_date} ({date_mode})")
    print("Placeholder output files written successfully.")

if __name__ == "__main__":
    main()
