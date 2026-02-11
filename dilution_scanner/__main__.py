import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

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

# Step 31: locked verbose CSV columns (deterministic order)
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
    Step 31: stable CSV escaping.
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

    target_date = start_date
    url = master_idx_url_for_date(target_date)

    fetch_status = None
    fetch_ok = False
    fetched_bytes_len = 0
    error = None

    parsed_row_count = 0
    allowed_row_count = 0

    # Step 30: dual-source cik->ticker map
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

    # Step 31: per-label summary counts (for matched rows only)
    label_counts = {}
    matched_rows_count = 0
    blank_ticker_in_matched_rows = 0

    try:
        resp = sec_get(url)
        fetch_status = resp.status_code
        content = resp.content
        fetched_bytes_len = len(content)

        if resp.status_code != 200:
            write_file_bytes(f"{OUTPUT_DIR}/master_idx_error_body.bin", content)
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

            allowed_rows = []
            for r in parsed_rows:
                if r.form_type in ("S-1", "S-3", "F-3", "8-K") or r.form_type.startswith("424B"):
                    allowed_rows.append(r)
            allowed_row_count = len(allowed_rows)

            allowed_filings = []
            for r in allowed_rows:
                allowed_filings.append(
                    {
                        "cik": r.cik,
                        "company": r.company,
                        "form_type": r.form_type,
                        "date_filed": r.date_filed,
                        "filename": r.filename,
                        "index_url": f"https://www.sec.gov/Archives/{r.filename}",
                    }
                )
            allowed_filings.sort(key=lambda x: (x["form_type"], x["cik"], x["filename"]))

            write_file_text(f"{OUTPUT_DIR}/allowed_filings.json", json.dumps(allowed_filings, indent=2))

            matched_allowed = []
            verbose_rows = []
            run_tickers = []

            # Step 31: deterministic header from locked columns
            verbose_header = ",".join(VERBOSE_COLUMNS) + "\n"

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

                cik_key = normalize_cik(filing.cik)
                ticker_val = cik_to_ticker.get(cik_key, "")

                matched_allowed.append(
                    {
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

                # Only emit verbose CSV rows when there is at least one label match
                if labels:
                    matched_rows_count += 1
                    if not ticker_val:
                        blank_ticker_in_matched_rows += 1

                    for lab in labels:
                        label_counts[lab] = label_counts.get(lab, 0) + 1

                    date_val = target_date
                    accession = accession_from_filename(filing.filename)
                    filing_url = filing.index_url
                    free_float_shares = ""
                    labels_str = "|".join(labels)
                    terms_str = "|".join(matched_terms)

                    row = {
                        "date": date_val,
                        "ticker": ticker_val,
                        "cik": filing.cik,
                        "company": filing.company,
                        "form_type": filing.form_type,
                        "accession": accession,
                        "filing_url": filing_url,
                        "free_float_shares": free_float_shares,
                        "labels": labels_str,
                        "matched_terms": terms_str,
                    }

                    # Step 31: deterministic column order + stable escaping
                    line = ",".join([csv_escape(row.get(col, "")) for col in VERBOSE_COLUMNS]) + "\n"
                    verbose_rows.append(line)

                    if ticker_val:
                        run_tickers.append(ticker_val)

            write_file_text(f"{OUTPUT_DIR}/matched_allowed_filings.json", json.dumps(matched_allowed, indent=2))
            write_file_text(f"{OUTPUT_DIR}/dilution_tickers_verbose.csv", verbose_header + "".join(verbose_rows))

            write_ticker_list(f"{OUTPUT_DIR}/dilution_tickers.csv", run_tickers)

            all_path = f"{OUTPUT_DIR}/dilution_tickers_all.csv"
            prior_all = read_ticker_list(all_path)
            write_ticker_list(all_path, prior_all + run_tickers)

            # Step 31: per-label summary artifacts (optional but deterministic)
            summary = {
                "date": target_date,
                "matched_rows": matched_rows_count,
                "blank_ticker_rows": blank_ticker_in_matched_rows,
                "label_counts": {k: label_counts[k] for k in sorted(label_counts.keys())},
            }
            write_file_text(f"{OUTPUT_DIR}/label_summary.json", json.dumps(summary, indent=2))

            # CSV version (deterministic order)
            summary_csv_lines = ["label,count\n"]
            for k in sorted(label_counts.keys()):
                summary_csv_lines.append(f"{csv_escape(k)},{label_counts[k]}\n")
            write_file_text(f"{OUTPUT_DIR}/label_summary.csv", "".join(summary_csv_lines))

            # Keep sample fetch for debugging (unchanged behavior)
            os.makedirs(f"{OUTPUT_DIR}/filings_raw", exist_ok=True)
            sample_n = 3
            sample = []
            seen_ciks = set()
            for item in allowed_filings:
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

        else:
            error = f"Non-200 or empty body (status={resp.status_code}, bytes={fetched_bytes_len})"
    except Exception as e:
        error = str(e)

    # Placeholder outputs if master fetch fails
    if not os.path.exists(f"{OUTPUT_DIR}/dilution_tickers_verbose.csv"):
        write_file_text(f"{OUTPUT_DIR}/dilution_tickers_verbose.csv", ",".join(VERBOSE_COLUMNS) + "\n")
    if not os.path.exists(f"{OUTPUT_DIR}/dilution_tickers.csv"):
        write_file_text(f"{OUTPUT_DIR}/dilution_tickers.csv", "")
    if not os.path.exists(f"{OUTPUT_DIR}/dilution_tickers_all.csv"):
        write_file_text(f"{OUTPUT_DIR}/dilution_tickers_all.csv", "")
    if not os.path.exists(f"{OUTPUT_DIR}/audit_log.json"):
        write_file_text(f"{OUTPUT_DIR}/audit_log.json", json.dumps([], indent=2))

    run_meta = {
        "run_timestamp_utc": run_time,
        "scan_start_date": start_date,
        "scan_end_date": end_date,
        "date_mode": date_mode,
        "allowed_forms": ALLOWED_FORMS,
        "sec_user_agent": SEC_USER_AGENT,
        "cik_ticker_map": {
            "ok": cik_map_ok,
            "error": cik_map_error,
            "meta": cik_map_meta,
        },
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
        "step31_summary": {
            "matched_rows": matched_rows_count,
            "blank_ticker_rows": blank_ticker_in_matched_rows,
            "label_counts": {k: label_counts[k] for k in sorted(label_counts.keys())},
            "label_summary_json": "output/label_summary.json" if fetch_ok else None,
            "label_summary_csv": "output/label_summary.csv" if fetch_ok else None,
        },
        "status": "step31_deterministic_columns_stable_escaping_label_summary",
    }
    write_file_text(f"{OUTPUT_DIR}/run_metadata.json", json.dumps(run_meta, indent=2))

    print(f"Master idx URL: {url}")
    print(f"Fetch ok={fetch_ok}, status={fetch_status}, bytes={fetched_bytes_len}")
    print(f"Parsed rows={parsed_row_count}, Allowed rows={allowed_row_count}")
    print(f"CIK->Ticker map ok={cik_map_ok}, combined_count={len(cik_to_ticker) if cik_map_ok else 0}")
    print(f"Matched rows={matched_rows_count}, Blank ticker rows={blank_ticker_in_matched_rows}")
    if cik_map_meta:
        print(f"Filled from exchange: {cik_map_meta.get('filled_from_exchange')}")
    if cik_map_error:
        print(f"CIK map error: {cik_map_error}")
    if error:
        print(f"Error: {error}")


if __name__ == "__main__":
    main()

