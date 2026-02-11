# dilution_scanner/rules.py
#
# Deterministic literal-string rules for identifying dilution-bank / PIPE / convert financing
# involvement from SEC filing text.
#
# - NO inference, NO heuristics.
# - Case-insensitive substring matching only.
# - Deterministic outputs: same inputs => same outputs.
# - Matched terms are de-duped and sorted deterministically.

from __future__ import annotations

from typing import Dict, List, Tuple


def _normalize(text: str) -> str:
    # Deterministic normalization: lowercase only.
    # (We intentionally do NOT strip punctuation or collapse whitespace to avoid "fuzzy" behavior.)
    return text.lower()


# -----------------------------
# LITERAL TERMS (LOCKED STYLE)
# -----------------------------
#
# Keep these lists explicit and stable.
# Add terms only by explicit request + commit history.

DILUTION_BANK_NAMES: List[str] = [
    # Common small-cap / microcap deal counsel & placement agents / “dilution desks”
    "aegis capital",
    "maxim group",
    "maxim",
    "h.c. wainwright",
    "hc wainwright",
    "roth capital",
    "roth mkms",
    "ladder capital",  # (if you don't want this here later, remove explicitly)
    "thinkequity",
    "boustead securities",
    "westpark capital",
    "benjamin securities",
    "ef hutton",
    "a.g.p.",
    "agp",
    "alliance global partners",
    "warrants",
    # Note: Keep bank list conservative; expand later with explicit additions.
]

PIPE_TERMS: List[str] = [
    "pipe financing",
    "private investment in public equity",
    "private investment in public equities",
    "private placement",
    "registered direct",
    "at-the-market",
    "at the market",
    "atm offering",
    "equity line of credit",
    "eLOC",
    "eliquid",  # leave out if undesired; placeholder example
]

CONVERT_TERMS: List[str] = [
    "convertible note",
    "convertible notes",
    "convertible debenture",
    "convertible debentures",
    "senior convertible",
    "conversion price",
    "conversion feature",
    "convertible preferred",
    "variable rate",
    "reset price",
    "price reset",
]

# Label -> terms map (deterministic label ordering enforced below)
LABEL_TERMS: Dict[str, List[str]] = {
    "dilution_bank": DILUTION_BANK_NAMES,
    "pipe_financing": PIPE_TERMS,
    "convert_financing": CONVERT_TERMS,
}


def scan_filing_text_for_labels(text: str) -> Tuple[List[str], List[str]]:
    """
    Returns:
      labels: list[str] (deterministic sorted order)
      matched_terms: list[str] (deterministic sorted order)

    Matching rule:
      - Case-insensitive substring match (normalized lowercase).
      - If any term from a label matches => label is included.
      - matched_terms includes the literal term strings that matched.
    """
    if not text:
        return [], []

    haystack = _normalize(text)

    matched_terms_set = set()
    labels_set = set()

    # Deterministic iteration order: sort labels and terms
    for label in sorted(LABEL_TERMS.keys()):
        terms = LABEL_TERMS[label]
        label_matched = False

        for term in sorted(terms):
            if term.lower() in haystack:
                label_matched = True
                matched_terms_set.add(term)

        if label_matched:
            labels_set.add(label)

    labels = sorted(labels_set)
    matched_terms = sorted(matched_terms_set)
    return labels, matched_terms
