from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class MasterIdxRow:
    cik: str
    company: str
    form_type: str
    date_filed: str
    filename: str  # EDGAR relative path, e.g. "edgar/data/....txt"


def parse_master_idx(text: str) -> List[MasterIdxRow]:
    """
    Deterministic parser for SEC master.idx content.

    Format after header:
      CIK|Company Name|Form Type|Date Filed|Filename

    We:
      - find the first line that starts with "CIK|Company Name|Form Type|Date Filed|Filename"
      - parse subsequent non-empty lines that contain 4 pipe separators
      - do NOT do any filtering or inference here
    """
    lines = text.splitlines()

    header_idx = -1
    header = "CIK|Company Name|Form Type|Date Filed|Filename"
    for i, line in enumerate(lines):
        if line.strip() == header:
            header_idx = i
            break

    if header_idx == -1:
        raise ValueError("master.idx header line not found")

    rows: List[MasterIdxRow] = []
    for line in lines[header_idx + 1 :]:
        line = line.strip()
        if not line:
            continue

        # Expect exactly 5 fields
        parts = line.split("|")
        if len(parts) != 5:
            # Deterministic: ignore malformed lines
            continue

        cik, company, form_type, date_filed, filename = [p.strip() for p in parts]
        rows.append(
            MasterIdxRow(
                cik=cik,
                company=company,
                form_type=form_type,
                date_filed=date_filed,
                filename=filename,
            )
        )

    return rows
