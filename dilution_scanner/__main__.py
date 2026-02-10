import os
import json
from datetime import datetime, timezone

OUTPUT_DIR = "output"

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def main():
    ensure_output_dir()

    run_time = datetime.now(timezone.utc).isoformat()

    # Placeholder outputs (deterministic structure)
    write_file(f"{OUTPUT_DIR}/dilution_tickers_verbose.csv", "date,ticker,cik,company,form_type,accession,filing_url,free_float_shares,labels,matched_terms\n")
    write_file(f"{OUTPUT_DIR}/dilution_tickers.csv", "")
    write_file(f"{OUTPUT_DIR}/dilution_tickers_all.csv", "")
    write_file(f"{OUTPUT_DIR}/audit_log.json", json.dumps([], indent=2))
    write_file(
        f"{OUTPUT_DIR}/run_metadata.json",
        json.dumps(
            {
                "run_timestamp_utc": run_time,
                "status": "placeholder",
            },
            indent=2,
        ),
    )

    print("Placeholder output files written successfully.")

if __name__ == "__main__":
    main()
