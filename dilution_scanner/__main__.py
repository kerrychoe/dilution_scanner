# dilution_scanner/__main__.py

from __future__ import annotations

import datetime as dt
import json
import os
import time
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .filings import (
    FilingRef,
    fetch_primary_filing_text,
    filing_artifact_basename,
)
from .master_idx_parser import (
    fetch_master_idx_text,
    parse_master_idx_rows,
)

# -----------------------------
# LOCKED / DETERMINISTIC CONSTANTS
# -----------------------------

OUTPUT_DIR = "output"

# SEC-compliant identity encoding is handled in filings.py and master_idx_parser.py.
# Deterministic retry/backoff also handled there.

# Step 25 requirement:
MAX_SAMPLE_BYTES = 2_000_000  # 2 MB hard cap for sample filings


# -----------------------------
# SMALL UTILITIES
# -----------------------------

def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def write_file_text(path: str, content: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write(content)


def utc_today() -> dt.date:
    return dt.datetime.utcnow().date()


def parse_date_env(name: str) -> Optional[dt.date]:
    v = os.getenv(name)
    if not v:
        return None
    # Expected: YYYY-MM-DD
    return dt.date.fromisoformat(v.strip())


def resolve_date_range() -> Tuple[dt.date, dt.date]:
    """
    Deterministic date handling (LOCKED):
    - If START_DATE / END_DATE provided => use inclusive range
    - Else default to yesterday UTC (single day)
    """
    start = parse_date_env("START_DATE")
    end = parse_date_env("END_DATE")

    if start and end:
        if end < start:
            raise ValueError("END_DATE must be >= START_DATE")
        return start, end

    if start and not end:
        return start, start

    if end and not start:
        return end, end

    yday = utc_today() - dt.timedelta(days=1)
    return yday, yday


def daterange_inclusive(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def load_json(path: str) -> object:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------
# MAIN
# -----------------------------

def main() -> int:
    started_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    run_id = started_utc.replace(":", "").replace("-", "")

    ensure_dir(OUTPUT_DIR)

    start_date, end_date = resolve_date_range()

    # Phase: Fetch + parse master.idx (LOCKED behavior)
    # NOTE: Current verified behavior fetches one master.idx per run (start_date).
    # We keep that behavior as stated in your repo state.
    scan_date = start_date

    master_idx_text = fetch_master_idx_text(scan_date)

    rows = parse_master_idx_rows(master_idx_text)

    # Persist full parsed count for audits
    run_metadata = {
        "run_id": run_id,
        "started_utc": started_utc,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "scan_date": scan_date.isoformat(),
        "rows_parsed_total": len(rows),
    }

    # Apply allowlist filter (LOCKED)
    allowed_prefixes = ("424B",)
    allowed_exact = {"S-1", "S-3", "F-3", "8-K"}

    allowed_rows = []
    for r in rows:
        ft = r["form_type"]
        if ft in allowed_exact or any(ft.startswith(p) for p in allowed_prefixes):
            allowed_rows.append(r)

    run_metadata["rows_allowed_total"] = len(allowed_rows)

    # Build allowed_filings.json in deterministic order
    # Sort deterministically by (form_type, cik, filename)
    allowed_rows_sorted = sorted(
        allowed_rows,
        key=lambda x: (x["form_type"], x["cik"], x["filename"]),
    )

    allowed_filings = []
    for r in allowed_rows_sorted:
        allowed_filings.append(
            {
                "cik": r["cik"],
                "company": r["company"],
                "form_type": r["form_type"],
                "date_filed": r["date_filed"],
                "filename": r["filename"],
                "index_url": r["index_url"],
            }
        )

    write_file_text(
        f"{OUTPUT_DIR}/allowed_filings.json",
        json.dumps(allowed_filings, indent=2),
    )

    # -----------------------------
    # Step 23 (existing): Sample filing fetch (first 3 unique CIKs)
    # Step 25 (new): Size cap + skip large files but record them
    # -----------------------------
    sample_results: List[Dict] = []

    # Determine first 3 unique CIKs in deterministic order from allowed_filings
    unique_ciks = []
    seen = set()
    for r in allowed_filings:
        cik = r["cik"]
        if cik not in seen:
            seen.add(cik)
            unique_ciks.append(cik)
        if len(unique_ciks) >= 3:
            break

    # Fetch sample filings for those CIKs (deterministic: first occurrence per CIK)
    for cik in unique_ciks:
        # pick the first allowed filing for that cik in allowed_filings list
        filing_row = None
        for r in allowed_filings:
            if r["cik"] == cik:
                filing_row = r
                break

        if not filing_row:
            continue

        filing_ref = FilingRef(
            cik=filing_row["cik"],
            company=filing_row["company"],
            form_type=filing_row["form_type"],
            date_filed=filing_row["date_filed"],
            filename=filing_row["filename"],
            index_url=filing_row["index_url"],
        )

        output_path = Path(
            OUTPUT_DIR,
            "filings_raw",
            filing_artifact_basename(filing_ref),
        )

        # Fetch the filing text
        fetch_ok = False
        http_status = None
        error = None
        content_text = ""

        try:
            content_text = fetch_primary_filing_text(filing_ref)
            fetch_ok = True
        except Exception as e:
            error = str(e)

        # Deterministic bytes accounting (Step 25)
        # If fetch failed, bytes=0 and we won't write any file.
        content_bytes = 0
        skipped_due_to_size = False
        saved_path: Optional[str] = None

        if fetch_ok:
            content_bytes = len(content_text.encode("utf-8"))
            skipped_due_to_size = content_bytes > MAX_SAMPLE_BYTES

            if not skipped_due_to_size:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(content_text)
                saved_path = str(output_path)

        sample_results.append(
            {
                "cik": filing_ref.cik,
                "company": filing_ref.company,
                "form_type": filing_ref.form_type,
                "date_filed": filing_ref.date_filed,
                "filename": filing_ref.filename,
                "index_url": filing_ref.index_url,
                "fetch_ok": fetch_ok,
                "http_status": http_status,
                "bytes": content_bytes,
                "skipped_due_to_size": skipped_due_to_size,
                "error": error,
                "saved_path": saved_path,
            }
        )

        # Fixed deterministic delay between fetches (Step 23 behavior)
        time.sleep(0.25)

    write_file_text(
        f"{OUTPUT_DIR}/sample_filing_fetch.json",
        json.dumps(sample_results, indent=2),
    )

    # Audit artifacts
    write_file_text(
        f"{OUTPUT_DIR}/run_metadata.json",
        json.dumps(run_metadata, indent=2),
    )
    write_file_text(
        f"{OUTPUT_DIR}/audit_log.json",
        json.dumps(
            {
                "run_id": run_id,
                "started_utc": started_utc,
                "notes": [
                    "allowed_filings.json written",
                    "sample_filing_fetch.json written",
                    "run_metadata.json written",
                    "audit_log.json written",
                ],
            },
            indent=2,
        ),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
