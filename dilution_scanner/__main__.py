import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone, date

from dilution_scanner.master_idx_parser import parse_master_idx
from dilution_scanner.filings import FilingRef, fetch_primary_filing_text, filing_artifact_basename
from dilution_scanner.rules import scan_filing_text_for_labels

OUTPUT_DIR = "output"

# Deterministic cap for filings we parse/scan
MAX_SAMPLE_BYTES = 2_000_000  # 2 MB

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
    """
    Deterministic inclusive date iteration.
    Returns list of YYYY-MM-DD strings in ascending order.
    """
    s = date.fromisoformat(start_iso)
    e = date.fromisoformat(end_iso)
    if e < s:
        # Deterministic behavior: swap
        s, e = e, s

    out = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


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
        "Accept": "text/plain,application/json,application/octet-stream,*/*",
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
    """
    Stable CSV escaping.
    - Convert to string
    - Quote if contains comma, quote, CR or LF
    - Escape quotes by doubling
    """
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
# AUDIT LOG HELPERS (RANGE-AWARE)
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
    }


def audit_event(audit: dict, event: str, data: dict | None = None):
    audit["events"].append(
        {
            "event": event,
            "data": (data or {}),
        }
    )


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


def main():
    ensure_output_dir()

    run_time = datetime.now(timezone.utc).isoformat()
    start_date, end_date, date_mode = parse_dates()
    dates = iter_date_range_inclusive(start_date, end_date)

    # Initialize audit (range-aware)
    audit = new_audit(run_time_utc=run_time, start_date=start_date, end_date=end_date, date_mode=date_mode)
    audit_event(audit, "run_start", {"date_mode": date_mode, "start_date": start_date, "end_date": end_date, "days": dates})

    # Dual-source cik->ticker map (once per run)
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
        {
            "ok": cik_map_ok,
            "error": cik_map_error,
            "meta": cik_map_meta,
            "count": (len(cik_to_ticker) if cik_map_ok else 0),
        },
    )

    # Range totals
    total_parsed_rows = 0
    total_allowed_rows = 0
    total_matched_rows = 0
    total_blank_ticker_rows = 0

    total_scan_fetch_fail = 0
    total_scan_skipped_due_to_size = 0
    total_scan_scanned = 0

    # Per-label totals (matched rows only)
    label_counts_total = {}

    # Aggregated artifacts across all days
    verbose_rows_all = []
    run_tickers_all = []

    # Optional debug artifacts (aggregated)
    allowed_filings_all = []
    matched_allowed_all = []

    error_rows_all = []
    ERROR_SAMPLE_LIMIT = 25

    # Process each day deterministically
    for target_date in dates:
        url = master_idx_url_for_date(target_date)

        day_info = {
            "date": target_date,
            "master_idx": {
                "url": url,
                "ok": False,
                "status": None,
                "bytes": 0,
                "saved_path": None,
                "error": None,
                "error_body_preview_path": None,
            },
            "counts": {
                "parsed_rows": 0,
                "allowed_rows": 0,
                "matched_rows": 0,
                "blank_ticker_rows": 0,
                "scan_fetch_fail": 0,
                "scan_skipped_due_to_size": 0,
                "scan_scanned": 0,
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

            if status != 200:
                # Save deterministic error artifacts per day
                err_bin = f"{OUTPUT_DIR}/master_idx_error_body_{target_date}.bin"
                err_txt = f"{OUTPUT_DIR}/master_idx_error_body_{target_date}.txt"
                write_file_bytes(err_bin, content)
                try:
                    preview = content.decode("utf-8", errors="replace")
                except Exception:
                    preview = "<decode_failed>"
                preview = preview[:2000]
                write_file_text(err_txt, preview)

                day_info["master_idx"]["ok"] = False
                day_info["master_idx"]["error"] = f"Non-200 (status={status})"
                day_info["master_idx"]["error_body_preview_path"] = err_txt

                audit_event(
                    audit,
                    "master_idx_fetched",
                    {"date": target_date, "ok": False, "status": status, "bytes": fetched_bytes_len, "url": url},
                )

                # Record day and continue (deterministic)
                audit["by_date"].append(day_info)
                audit_event(audit, "day_end", {"date": target_date, "ok": False, "error": day_info["master_idx"]["error"]})
                continue

            if fetched_bytes_len <= 0:
                day_info["master_idx"]["ok"] = False
                day_info["master_idx"]["error"] = "Empty body"
                audit_event(
                    audit,
                    "master_idx_fetched",
                    {"date": target_date, "ok": False, "status": status, "bytes": fetched_bytes_len, "url": url},
                )
                audit["by_date"].append(day_info)
                audit_event(audit, "day_end", {"date": target_date, "ok": False, "error": "Empty body"})
                continue

            # Save master idx per day (don’t overwrite across range)
            master_path = f"{OUTPUT_DIR}/master_{target_date}.idx"
            write_file_bytes(master_path, content)
            day_info["master_idx"]["ok"] = True
            day_info["master_idx"]["saved_path"] = master_path

            audit_event(
                audit,
                "master_idx_fetched",
                {"date": target_date, "ok": True, "status": status, "bytes": fetched_bytes_len, "url": url, "saved_path": master_path},
            )

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

            audit_event(
                audit,
                "master_idx_parsed",
                {"date": target_date, "parsed_rows": parsed_row_count, "allowed_rows": allowed_row_count},
            )

            # Build allowed filings (stable)
            allowed_filings = []
            for r in allowed_rows:
                allowed_filings.append(
                    {
                        "date": target_date,
                        "cik": r.cik,
                        "company": r.company,
                        "form_type": r.form_type,
                        "date_filed": r.date_filed,
                        "filename": r.filename,
                        "index_url": f"https://www.sec.gov/Archives/{r.filename}",
                    }
                )

            # Deterministic ordering within date
            allowed_filings.sort(key=lambda x: (x["form_type"], x["cik"], x["filename"]))

            # Add to aggregated debug artifact list (keep deterministic by appending day in ascending date order)
            allowed_filings_all.extend(allowed_filings)

            # Scan each allowed filing (sequential)
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

                # Per-day scan counters
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

                cik_key = normalize_cik(filing.cik)
                ticker_val = cik_to_ticker.get(cik_key, "")

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

                    row = {
                        "date": target_date,
                        "ticker": ticker_val,
                        "cik": filing.cik,
                        "company": filing.company,
                        "form_type": filing.form_type,
                        "accession": accession_from_filename(filing.filename),
                        "filing_url": filing.index_url,
                        "free_float_shares": "",
                        "labels": "|".join(labels),
                        "matched_terms": "|".join(matched_terms),
                    }

                    line = ",".join([csv_escape(row.get(col, "")) for col in VERBOSE_COLUMNS]) + "\n"
                    verbose_rows_all.append(line)

                    if ticker_val:
                        run_tickers_all.append(ticker_val)

            # Update day counts
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
                },
            )

            # Add day into audit summary list (deterministic order: loop order is ascending)
            audit["by_date"].append(day_info)
            audit_event(audit, "day_end", {"date": target_date, "ok": True})

            # Add to range totals
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
    # WRITE AGGREGATED OUTPUTS
    # -----------------------------

    # Debug artifacts (aggregated, deterministic ordering due to date loop + stable sorts)
    write_file_text(f"{OUTPUT_DIR}/allowed_filings.json", json.dumps(allowed_filings_all, indent=2))
    write_file_text(f"{OUTPUT_DIR}/matched_allowed_filings.json", json.dumps(matched_allowed_all, indent=2))

    # Verbose CSV
    verbose_header = ",".join(VERBOSE_COLUMNS) + "\n"
    write_file_text(f"{OUTPUT_DIR}/dilution_tickers_verbose.csv", verbose_header + "".join(verbose_rows_all))

    # Run tickers (deduped sorted by write_ticker_list)
    write_ticker_list(f"{OUTPUT_DIR}/dilution_tickers.csv", run_tickers_all)

    # Persistent master list (updated from entire range)
    all_path = f"{OUTPUT_DIR}/dilution_tickers_all.csv"
    prior_all = read_ticker_list(all_path)
    write_ticker_list(all_path, prior_all + run_tickers_all)

    # Combined label summary
    summary = {
        "start_date": start_date,
        "end_date": end_date,
        "matched_rows": total_matched_rows,
        "blank_ticker_rows": total_blank_ticker_rows,
        "label_counts": {k: label_counts_total[k] for k in sorted(label_counts_total.keys())},
    }
    write_file_text(f"{OUTPUT_DIR}/label_summary.json", json.dumps(summary, indent=2))

    summary_csv_lines = ["label,count\n"]
    for k in sorted(label_counts_total.keys()):
        summary_csv_lines.append(f"{csv_escape(k)},{label_counts_total[k]}\n")
    write_file_text(f"{OUTPUT_DIR}/label_summary.csv", "".join(summary_csv_lines))

    # Audit log (range totals + deterministic error sample)
    error_rows_sorted = sorted(error_rows_all, key=lambda x: (x.get("date", ""), x.get("form_type", ""), x.get("cik", ""), x.get("filename", "")))
    audit["error_samples"] = error_rows_sorted[:ERROR_SAMPLE_LIMIT]

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
        },
    )

    write_file_text(f"{OUTPUT_DIR}/audit_log.json", json.dumps(audit, indent=2))

    # Keep sample fetch for debugging (deterministic: first 3 unique CIKs from aggregated allowed list)
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

        ok, content_bytes, err_str, http_status = fetch_primary_filing_text(
            filing=filing,
            user_agent=SEC_USER_AGENT,
        )

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
        "run_timestamp_utc": run_time,
        "scan_start_date": start_date,
        "scan_end_date": end_date,
        "date_mode": date_mode,
        "scan_days": dates,
        "allowed_forms": ALLOWED_FORMS,
        "sec_user_agent": SEC_USER_AGENT,
        "cik_ticker_map": {
            "ok": cik_map_ok,
            "error": cik_map_error,
            "meta": cik_map_meta,
        },
        "range_totals": {
            "master_idx_parsed_rows": total_parsed_rows,
            "master_idx_allowed_rows": total_allowed_rows,
            "matched_rows": total_matched_rows,
            "blank_ticker_rows": total_blank_ticker_rows,
            "scan_fetch_fail": total_scan_fetch_fail,
            "scan_skipped_due_to_size": total_scan_skipped_due_to_size,
            "scan_scanned": total_scan_scanned,
        },
        "status": "step33_multiday_range_scan",
    }
    write_file_text(f"{OUTPUT_DIR}/run_metadata.json", json.dumps(run_meta, indent=2))

    print(f"Scan range: {start_date} .. {end_date} (days={len(dates)})")
    print(f"Parsed rows={total_parsed_rows}, Allowed rows={total_allowed_rows}")
    print(f"CIK->Ticker map ok={cik_map_ok}, combined_count={len(cik_to_ticker) if cik_map_ok else 0}")
    print(f"Matched rows={total_matched_rows}, Blank ticker rows={total_blank_ticker_rows}")
    print(f"Scan fetch fail={total_scan_fetch_fail}, skipped_due_to_size={total_scan_skipped_due_to_size}, scanned={total_scan_scanned}")
    if cik_map_meta:
        print(f"Filled from exchange: {cik_map_meta.get('filled_from_exchange')}")
    if cik_map_error:
        print(f"CIK map error: {cik_map_error}")


if __name__ == "__main__":
    main()
