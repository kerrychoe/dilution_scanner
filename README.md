# DilutionTicker Scanner

**Version:** `v1.1.2`
**Git Tag:** `v1.1.2`

Deterministic SEC filing scanner designed to identify U.S.-listed companies with explicit dilution-related activity and maintain a rolling severity model over a 180-day window.

---

# System Objective (Locked)

Identify U.S.-listed companies with explicit dilution-related SEC filings using:

* Literal substring matching only
* No inference
* No ML / LLM classification
* Float ≤ 10,000,000 shares (Massive API)
* Rolling 180-day window
* Fully deterministic behavior (same inputs → same outputs)

---

# Architecture Overview

## 1. Master Index Parsing

For each scan date:

* Fetch EDGAR `master.YYYYMMDD.idx`
* Parse rows deterministically
* Filter allowed forms:

  * `424B*`
  * `S-1`
  * `S-3`
  * `F-3`
  * `8-K`

No heuristics. Literal form matching only.

---

## 2. Float Gate (Locked Behavior)

Strict float filter using Massive API:

* Skip if no ticker
* Skip if float unknown
* Skip if float > 10M
* 3 fixed retries
* No concurrency
* Deterministic ordering

Artifacts:

```
output/float_gate_pass.csv
output/float_gate_fail.csv
output/float_gate_unknown.csv
```

---

## 3. Literal Detection Rules (Locked)

Detection labels (substring only):

* `dilution_bank`
* `pipe_financing`
* `convert_financing`

No semantic interpretation.

---

# Severity Model (v1.1.x)

Severity is derived strictly from matched filings.

Rolling windows (inclusive of END_DATE):

* 90-day
* 180-day

---

## Label Weights (Locked)

```
dilution_bank      = 5
pipe_financing     = 3
convert_financing  = 3
```

---

## Bank Tier Weights (Locked)

Hardcoded tier mapping (1–5).

Bank multipliers:

```
0: 100
1: 105
2: 110
3: 120
4: 135
5: 150
```

---

## Filing Score Formula (Option C Multiplier)

Per filing:

```
label_score = sum(unique label weights)
bank_score  = max matched bank weight
term_score  = sum term weights

term_component = (term_score * BANK_MULTIPLIER[bank_score]) // 100
final_filing_score = label_score + term_component + bank_score
```

Integer math only.

---

# Avoid Flag Logic (Locked)

Thresholds:

```
BANK_BACKSTOP_MIN   = 4
TERM_BACKSTOP_MIN   = 8
FINAL_SEVERITY_MIN  = 20
```

Rule:

```
avoid_flag = 1 if:
    (max_bank_score_180d >= 4 AND severity_score_90d >= 20)
    OR
    (term_score_90d >= 8)
else 0
```

---

# NEW in v1.1.2 — Persistent Severity Events Master

## Problem Solved

In v1.1.1, severity outputs were recomputed only from the current run.

On quiet days:

* `dilution_severity_by_ticker.csv` could become empty
* `avoid_tickers.csv` could become empty

This is now fixed.

---

## Severity Events Master (Source of Truth)

New persistent file (repo root):

```
dilution_severity_events_all.csv
```

Each row represents one matched filing (“severity event”).

### Stored Fields

* ticker
* date
* filing_url / filename
* label_score
* bank_score
* term_score
* final_filing_score

---

## Deterministic Event Handling

Each run:

1. Load prior events master (repo root)
2. Append new matched events
3. Deduplicate by stable key:

```
event_key = ticker|date|filename
```

4. Prune events older than:

```
END_DATE - 179 days
```

5. Recompute severity from the pruned master

---

## Output Stability Guarantee

On days with zero new matches:

* Severity remains intact
* Avoid universe remains intact
* No wipe-out behavior

---

# Output Files

Under `/output`:

```
dilution_tickers_verbose.csv
dilution_tickers.csv
dilution_tickers_all_verbose.csv
dilution_tickers_all.csv
dilution_severity_events_all.csv
dilution_severity_by_ticker.csv
avoid_tickers.csv
float_gate_pass.csv
float_gate_fail.csv
float_gate_unknown.csv
label_summary.json
label_summary.csv
audit_log.json
run_metadata.json
sample_filing_fetch.json
```

---

## avoid_tickers.csv (v1.1.2)

* Contains only ticker symbols
* No header row
* Deterministically sorted
* Empty file if no avoid flags

Example:

```
ABC
XYZ
LMNO
```

---

# Running the Scanner

## Daily Mode (cron)

If no START_DATE / END_DATE provided:

* END_DATE = yesterday (UTC)
* START_DATE = END_DATE

---

## Explicit Range

```
START_DATE=2026-02-10
END_DATE=2026-02-14
```

GitHub Actions supports manual workflow_dispatch.

---

# Backfill Guidance

To seed a full 180-day window:

```
START_DATE = END_DATE - 179 days
END_DATE   = desired end date
```

Example:

```
START_DATE = 2025-08-25
END_DATE   = 2026-02-20
```

---

# Determinism Guarantees

* Stable sorting everywhere
* No concurrency
* No randomness
* No inference
* No ML
* No floating-point math
* Integer scoring only
* Same inputs → same outputs

---

# Tech Stack

* Python 3.11
* requests (only dependency)
* GitHub Actions
* Deterministic CSV + JSON artifacts

---

# Version History

## v1.1.2

* Added persistent severity events master
* Fixed quiet-day severity wipe
* Added deterministic dedupe logic
* Added rolling 180-day event-level prune
* Made avoid_tickers.csv headerless
* Added SEC request throttle for stability

## v1.1.1

* Rolling severity scoring
* Avoid flag logic
* Persistent ticker master with 180-day prune

