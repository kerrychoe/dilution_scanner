from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple
import time

import requests


@dataclass(frozen=True)
class FilingRef:
    cik: str
    company: str
    form_type: str
    date_filed: str  # YYYYMMDD
    filename: str    # e.g. "edgar/data/1234567/0001234567-26-000001.txt"
    index_url: str   # full https URL to primary filing text


def sec_get(url: str, user_agent: str, timeout_s: int = 60) -> requests.Response:
    """
    Deterministic SEC GET:
    - Always uses same headers
    - Always sleeps a fixed duration before request (SEC-friendly)
    """
    time.sleep(0.2)  # fixed delay: deterministic + polite

    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "identity",  # avoid gzip variability
        "Accept": "*/*",
    }
    return requests.get(url, headers=headers, timeout=timeout_s)


def fetch_primary_filing_text(
    filing: FilingRef,
    user_agent: str,
) -> Tuple[bool, Optional[bytes], Optional[str], Optional[int]]:
    """
    Fetch the primary filing text (bytes) deterministically.
    Returns: (ok, content_bytes, error_str, http_status)
    """
    try:
        resp = sec_get(filing.index_url, user_agent=user_agent)
        status = resp.status_code
        content = resp.content or b""
        if status == 200 and len(content) > 0:
            return True, content, None, status
        return False, content, f"Non-200 or empty body (status={status}, bytes={len(content)})", status
    except Exception as e:
        return False, None, str(e), None


def filing_artifact_basename(filing: FilingRef) -> str:
    """
    Deterministic, filesystem-safe name derived from filename.
    Example input filename:
      edgar/data/1682472/0001918704-26-002743.txt
    Output basename:
      1682472__0001918704-26-002743.txt
    """
    parts = filing.filename.split("/")
    if len(parts) >= 4:
        cik = parts[2]
        leaf = parts[-1]
        return f"{cik}__{leaf}"
    # fallback
    return filing.filename.replace("/", "__")


def filing_to_dict(f: FilingRef) -> Dict[str, Any]:
    return {
        "cik": f.cik,
        "company": f.company,
        "form_type": f.form_type,
        "date_filed": f.date_filed,
        "filename": f.filename,
        "index_url": f.index_url,
    }
