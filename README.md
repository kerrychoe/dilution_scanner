# DilutionTicker Scanner

This repository contains a **deterministic, scheduled stock-scanning pipeline**
that identifies and labels U.S. stock tickers associated with:

- dilution banks
- PIPE financing
- convertible / variable-price instruments

## Core Properties (Locked)

- Runs daily via **GitHub Actions**
- Implemented in **Python**
- Uses **SEC EDGAR filings only**
- Deterministic:
  - same inputs → same outputs
  - no heuristics
  - no inference
  - no LLMs
- Outputs CSV artifacts suitable for ingestion into trading scanners

## Outputs

Daily:
- `dilution_tickers_verbose.csv`
- `dilution_tickers.csv`

Running master:
- `dilution_tickers_all.csv` (committed back to repo each run)

Audit & metadata:
- `audit_log.json`
- `run_metadata.json`

## Scope

This system **labels** tickers associated with dilution risk.
It does **not** automatically exclude any ticker.
Downstream tools decide how the data is used.

## Status

🚧 Initial scaffolding
