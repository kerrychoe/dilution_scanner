# Dilution Scanner

## Version 1.1.0

Deterministic SEC EDGAR dilution scanner for U.S. small-cap traders.

---

## 🎯 System Objective

Identify U.S.-listed companies with explicit dilution-related SEC filings, filtered to:

* Literal dilution language only (no inference, no LLM)
* Share float ≤ 10,000,000 (Massive API)
* Active within the last 180 days
* Fully deterministic behavior
* GitHub Actions automated daily runs

Same inputs → Same outputs.

---

## 🧠 What This Scanner Detects

The system scans SEC EDGAR daily master index files and identifies filings containing literal references to:

* `dilution_bank`
* `pipe_financing`
* `convert_financing`

Rules are strict substring matches — no heuristics.

---

## ⚙️ Architecture Overview

### 1️⃣ Multi-Day Deterministic Scan

For each date in:

```

START_DATE .. END_DATE

```

The system:

1. Fetches that day’s `master.idx`
2. Parses all filings
3. Filters by allowed forms:

   * 424B*
   * S-1
   * S-3
   * F-3
   * 8-K
4. Applies float gate
5. Fetches and scans filing text
6. Aggregates matched rows

Weekend and holiday handling is automatic (non-200 responses are logged and skipped).

---

### 2️⃣ 10M Share Float Gate (Massive API)

Only tickers with:

```

free_float_shares <= 10,000,000

```

are scanned.

Configuration:

* Deterministic call order
* No parallelism
* Fixed retries
* Identity encoding
* Strict policy: skip if no ticker or float unknown

Massive API is authenticated via:

```

Authorization: Bearer <API_KEY>

```

Environment variables:

```

MASSIVE_FLOAT_URL_TEMPLATE
MASSIVE_API_KEY

```

---

### 3️⃣ Persistent Master List (Self-Pruning)

The system maintains:

```

dilution_tickers_all_verbose.csv   (source of truth)
dilution_tickers_all.csv           (derived ticker-only list)

```

Verbose master tracks per ticker:

* ticker
* first_seen_date
* last_seen_date
* seen_count
* last_labels
* last_filing_url

On each run:

* New matches update the master
* Tickers with no dilution filings in the last **180 days** are automatically removed
* Derived ticker-only file is regenerated

This creates a rolling, self-cleaning dilution universe.

---

## 🧠 v1.1.0 — Dilution Severity Intelligence (Additive)

v1.1.0 adds **deterministic severity scoring** per ticker, without changing:

* detection rules
* float gate
* pruning behavior
* determinism constraints

### Severity output

A new artifact ranks tickers by severity across two fixed windows:

* **90-day window**
* **180-day window**

The scoring is computed from **existing matched filing data only** (no additional API calls).

### Deterministic scoring concept (high level)

Each matched filing contributes a deterministic score derived from:

* **Label weights** (fixed)
* **Bank tier weights** (fixed mapping of literal bank-name terms)
* **Toxic financing term weights** (fixed mapping of literal terms)
* A **fixed bank multiplier** on term score (implemented deterministically)

The per-filing scores are summed by ticker within each window and sorted deterministically.

---

## 📁 Output Artifacts

Generated under `/output`:

| File                               | Description                          |
| ---------------------------------- | ------------------------------------ |
| `dilution_tickers_verbose.csv`     | Matched filings (per filing)         |
| `dilution_tickers.csv`             | Unique tickers from current scan     |
| `dilution_tickers_all_verbose.csv` | Persistent master with state         |
| `dilution_tickers_all.csv`         | Active ticker-only list (≤180 days)  |
| `float_gate_pass.csv`              | Tickers passing float gate           |
| `float_gate_fail.csv`              | Tickers failing float gate           |
| `float_gate_unknown.csv`           | Tickers without usable float         |
| `label_summary.json`               | Aggregate label counts               |
| `audit_log.json`                   | Full deterministic execution trace   |
| `run_metadata.json`                | Run-level metadata including version |
| `dilution_severity_by_ticker.csv`  | Severity ranking by ticker (90d/180d)|

---

## 📈 Severity Output Details (v1.1.0)

### `dilution_severity_by_ticker.csv` schema (stable order)

* `ticker`
* `severity_score_90d`
* `severity_score_180d`
* `match_count_90d`
* `match_count_180d`
* `last_seen_date`
* `last_labels`
* `top_terms`
* `top_banks`

### Deterministic sort order

1. `severity_score_90d` (desc)
2. `severity_score_180d` (desc)
3. `ticker` (asc)

---

## 🔍 Determinism Guarantees

* No randomness
* No concurrency
* Stable sorting of all lists
* Fixed retry strategy
* Fixed form allowlist
* Fixed float threshold
* Literal string rules only
* Stable CSV column order
* Stable audit structure

Every run is reproducible from identical inputs.

---

## 🚀 GitHub Actions Automation

The scanner runs:

* Daily via cron
* Manually via `workflow_dispatch`
* Supports explicit date ranges

Example backfill:

```

START_DATE = 2025-11-14
END_DATE   = 2026-02-11

```

Secrets required:

```

MASSIVE_API_KEY

```

---

## 📊 Current Trading Filters (v1.1.0)

* Float ≤ 10,000,000 shares
* Dilution-related filing language
* Allowed SEC forms only
* Active within 180 days
* U.S.-listed tickers only
* Deterministic severity ranking (90d + 180d)

---

## 🔒 Versioning

Current version: **1.1.0**

To create a new version tag:

```

git tag -a v1.1.0 -m "Version 1.1.0 – Severity Intelligence"
git push origin v1.1.0

```

Each run writes:

```

"system_version": "1.1.0"

```

into `run_metadata.json`.

---

## 🧩 Future Roadmap (Not in v1.1)

* “Avoid watchlist” deterministic flags + thresholds (derived from severity components)
* Lightspeed export formatting
* Rule versioning system
* Performance guardrails
* Optional rolling-window mode
* Additional deterministic summary outputs (e.g., top tickers by day/week)

---

## ⚠️ Disclaimer

This tool identifies filings containing dilution-related language.
It does not provide investment advice.

---

## 🏁 Status

v1.1.0 represents a stable, production-ready deterministic dilution scanner with:

* Multi-day scanning
* 10M float filter
* Persistent rolling master list
* 180-day pruning
* Full audit traceability
* Automated daily execution
* Deterministic severity ranking output (90d/180d)
```
