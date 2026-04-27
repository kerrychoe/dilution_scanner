# New-Repo Rewrite & Rollback Plan — DilutionTicker Scanner v2

## Context

The user pushed back on in-place refactoring as too risky, and they're right: the existing repo has an auto-commit bot pushing to `main` daily, persistent state CSVs versioned in git, no test coverage, and a 1,366-line `__main__.py`. Any refactor on `main` fights the bot, and any bug ships to production the next morning.

The agreed approach is: **leave `dilution_scanner` completely alone, build a clean v2 in a new repo, run both in parallel for 2 weeks, then cut over once outputs match.** Rollback becomes trivial — just keep using the old repo.

User decisions (confirmed):
- **Clean-room rewrite** — port only the locked policy (forms, weights, thresholds, term lists). Rebuild the pipeline with proper module structure and tests.
- **Seed persistent state from the old repo** — copy `dilution_severity_events_all.csv` on day 1 so the 180-day rolling window is populated immediately. New repo is operationally useful from the first run.
- **2-week shadow period** — both run daily, outputs diffed each morning.
- **Rename "avoid" → "dilution watchlist"** — these tickers aren't being avoided, they're being identified for inclusion in a Lightspeed premarket-scalping watchlist. `avoid_tickers.csv` → `dilution_tickers_watchlist.csv`; `avoid_flag` column → `dilution_flag`. All code identifiers, constants, function names, and README sections referencing "avoid" get renamed to "watchlist" or "dilution_flag" as appropriate. The threshold constants (`BANK_BACKSTOP_MIN`, `TERM_BACKSTOP_MIN`, `FINAL_SEVERITY_MIN`) keep their names — they describe thresholds, not the act of avoiding.

The intended outcome: a maintainable v2 with tests, tighter rules, and surfaced operability gaps, deployed with zero risk to the working v1.

---

## New repo: `dilution_scanner_v2`

### Structure (day 1)

```
dilution_scanner_v2/
├── pyproject.toml              # package metadata, pytest, ruff, mypy
├── README.md                   # "v2 of dilution_scanner — clean-room rewrite"
├── .github/workflows/
│   ├── ci.yml                  # pytest + ruff + mypy on every PR
│   └── daily_scan.yml          # ported from v1 with concurrency: group
├── src/dilution_scanner/
│   ├── __init__.py
│   ├── __main__.py             # 50 lines: parse env, call cli.run()
│   ├── cli.py                  # orchestration; the old main()'s job
│   ├── sec_http.py             # single sec_get; replaces both v1 copies
│   ├── master_index.py         # ported from master_idx_parser.py (clean)
│   ├── filings.py              # ported (drop the duplicate sec_get)
│   ├── rules.py                # ported terms + WORD-BOUNDARY regex matching
│   ├── float_gate.py           # Massive client + cache + pass/fail/unknown
│   ├── severity.py             # weights, scoring, event-master, avoid flag
│   ├── persistence.py          # CSV read/write using stdlib csv module
│   └── audit.py                # structured audit log
├── tests/
│   ├── conftest.py
│   ├── fixtures/               # captured master.idx, filing samples, Massive responses
│   ├── test_rules.py           # golden tests + new word-boundary cases
│   ├── test_severity.py        # all scoring branches, dedupe, prune
│   ├── test_master_index.py
│   ├── test_float_gate.py
│   └── test_end_to_end.py      # frozen date range, byte-identical CSVs
├── scripts/
│   ├── compare_with_v1.py      # diff v2 outputs against v1 outputs daily
│   └── seed_from_v1.py         # one-shot: copy event master from v1 repo
└── data/                       # persistent CSVs (committed, like v1)
    ├── dilution_severity_events_all.csv
    ├── dilution_tickers_all.csv
    └── dilution_tickers_all_verbose.csv
```

### What gets ported from v1 (exact, unchanged)

The "locked" policy must be byte-identical for the shadow comparison to be meaningful:

- `ALLOWED_FORMS` — `["424B", "S-3", "S-1", "F-3", "8-K"]`
- `FLOAT_MAX_SHARES = 10_000_000`
- `LABEL_WEIGHT`, `BANK_WEIGHT`, `TERM_WEIGHT`, `BANK_MULTIPLIER_BPS` (`__main__.py:557–612`)
- `BANK_BACKSTOP_MIN`, `TERM_BACKSTOP_MIN`, `FINAL_SEVERITY_MIN` (`__main__.py:614–616`)
- Term lists in `rules.py:29–73`
- Severity formula `_severity_final_filing_score` (`__main__.py:643`)
- Event key `ticker|date|filename` (`__main__.py:843`)
- 180-day prune cutoff `END_DATE − 179 days` (`__main__.py:907`)

### What changes in v2 (intentional improvements)

1. **Tests from day 1** — every weight, threshold, dedupe key, and prune boundary covered.
2. **Single `sec_get`** — replaces v1's two divergent copies (`__main__.py:143` and `filings.py:20`).
3. **Stdlib `csv`** — replaces v1's hand-rolled `_split_csv_line` and `csv_escape`.
4. **Word-boundary regex matching** in `rules.py` — fixes the "maxim" / "agp" substring false positives. *Gated behind a `STRICT_WORD_BOUNDARIES` env var initially set to `false` so shadow comparison stays clean; flip to `true` after cutover.*
5. **Surface the 2 MB skip** — new `output/oversized_filings.csv` and `audit["counts"]["skipped_oversize"]`.
6. **No silent `except Exception`** — every per-date failure logged into `audit["error_samples"]`.
7. **Single-pass over master.idx** — parse once, use for both candidate-building and scanning (v1 fetches twice at lines 1066 and 1177).
8. **Named window constants** — `WINDOW_180D_DAYS = 180`, `WINDOW_90D_DAYS = 90` in one place.
9. **Workflow `concurrency:` group** — manual + cron can't race the auto-commit.
10. **CI workflow** — `pytest`, `ruff`, `mypy` on every PR.
11. **Watchlist renaming** — output file `dilution_tickers_watchlist.csv` (replaces `avoid_tickers.csv`); column `dilution_flag` (replaces `avoid_flag`); constants `DILUTION_WATCHLIST_OUT`, function `write_watchlist_csv_from_severity`, etc. Format remains headerless one-ticker-per-line for direct Lightspeed import.

---

## Phased rollout

### Phase 0 — set up the new repo (day 0)
- Create empty `dilution_scanner_v2` repo on GitHub
- Add `pyproject.toml`, `ci.yml`, `.gitignore`, `README.md`
- Copy Massive API secret to new repo's GitHub Actions secrets
- Tag old repo's current `main` as `v1.1.2` so there's a real anchor

### Phase 1 — port locked policy + scaffold (days 1–2)
- Build module skeleton (empty modules, types, signatures)
- Port `rules.py` term lists exactly (substring matching, no regex yet)
- Port all weight/threshold constants into `severity.py`
- Tests: assert constants match v1 byte-for-byte (literal value asserts)

### Phase 2 — implement core pipeline (days 3–6)
- `sec_http.py`, `master_index.py`, `filings.py` (one-pass design)
- `float_gate.py` with Massive client + cache
- `severity.py` with full scoring + event master + prune
- `persistence.py` using stdlib `csv`
- Tests: golden-file tests using captured v1 fixtures from `output/`. Every public function tested.
- **Acceptance gate:** `pytest -q` green, `mypy --strict` clean, `ruff` clean

### Phase 3 — seed and dry-run (day 7)
- Run `scripts/seed_from_v1.py` to copy `dilution_severity_events_all.csv` from `kerrychoe/dilution_scanner@v1.1.2` into v2's `data/`
- Manual `workflow_dispatch` with a fixed 5-day historical date range
- Compare v2 `output/` against v1 `output/` for the same range using `scripts/compare_with_v1.py`
- **Acceptance gate:** zero diffs in `dilution_severity_events_all.csv`, `avoid_tickers.csv`, `dilution_severity_by_ticker.csv` (with `STRICT_WORD_BOUNDARIES=false`)

### Phase 4 — parallel run (days 8–21, the 2-week shadow)
- Enable cron in v2's `daily_scan.yml`
- Both repos run daily on the same schedule
- `scripts/compare_with_v1.py` runs as a separate step in v2's workflow each day, posts a summary to the workflow log (and optionally a GitHub Issue if diffs appear)
- Daily review: any diff is investigated; either it's a v2 bug (fix it) or a v1 bug v2 fixed (document it)
- **Acceptance gate:** ≥10 of 14 days with zero diffs, and 100% of remaining diffs are explained and approved improvements

### Phase 5 — cutover (day 22)
- Disable cron in v1's `daily_scan.yml` (comment out `schedule:` block, leave `workflow_dispatch` enabled as escape hatch)
- Flip v2's `STRICT_WORD_BOUNDARIES=true` to enable the bank-name regex tightening
- Tag v2 as `v2.0.0`
- v1 repo remains intact as fallback for ≥30 days

### Phase 6 — decommission (day 52+)
- Archive v1 repo via GitHub repo settings (read-only)
- Update any external consumers pointing to v1's CSVs

---

## Rollback strategy (now trivial)

The whole point of the new-repo approach is that rollback is just "stop using v2." There are three rollback layers, ordered by speed:

| Trigger | Action | Time to recover |
|---|---|---|
| **v2 produces wrong output during shadow (Phase 4)** | Do nothing operationally — v1 is still authoritative. Fix v2 on a branch, re-run shadow comparison, extend Phase 4 if needed. | 0 minutes (no impact) |
| **v2 breaks after cutover (Phase 5+)** | Re-enable cron in v1's `daily_scan.yml` (uncomment the `schedule:` block). Disable cron in v2. v1 picks up where it left off — its persistent CSVs were never stopped. | ~5 minutes |
| **v2's persistent CSVs corrupted** | Re-run `scripts/seed_from_v1.py` against the latest v1 commit. v2's 180-day prune self-heals on next run. | ~10 minutes |
| **Catastrophic v2 failure during cutover window** | `git revert` the cron-disable commit on v1; v1 resumes daily runs. v2 stays disabled until investigated. | ~5 minutes |

### Why this is safer than in-place refactoring

- **No bot collisions** — v1's auto-committer keeps running undisturbed throughout development.
- **No test prerequisite** — v2 builds tests from day 1 instead of retrofitting them onto a 1,366-line file.
- **Real comparison harness** — 2 weeks of daily diffs is a stronger correctness signal than any unit test suite.
- **Zero coupling between development and production** — bugs in v2's repo cannot affect v1's output.
- **Tagged v1.1.2 as anchor** — even if v2 is abandoned, v1 has a real release tag for the first time.

---

## Critical files (in the new repo)

All paths are relative to `dilution_scanner_v2/`:

- `pyproject.toml` — package + dev tooling config
- `src/dilution_scanner/cli.py` — orchestration
- `src/dilution_scanner/severity.py` — scoring, event master, avoid flag (port from `__main__.py:557–992` with structure)
- `src/dilution_scanner/rules.py` — term lists + word-boundary regex (gated by env var)
- `src/dilution_scanner/sec_http.py` — single `sec_get` (consolidates `__main__.py:143` + `filings.py:20`)
- `src/dilution_scanner/persistence.py` — stdlib `csv` (replaces `_split_csv_line`, `csv_escape`)
- `tests/fixtures/` — captured from v1's `output/` directory
- `scripts/seed_from_v1.py` — one-shot CSV copy from v1 repo
- `scripts/compare_with_v1.py` — daily diff harness for Phase 4
- `.github/workflows/ci.yml` — pytest + ruff + mypy
- `.github/workflows/daily_scan.yml` — ported from v1 with `concurrency:` group

## Functions/utilities to port unchanged

These are already clean in v1 — copy verbatim:

- `parse_master_idx` (`master_idx_parser.py:16`) → `master_index.py`
- `MasterIdxRow`, `FilingRef` dataclasses → respective new modules
- `_severity_final_filing_score` formula (`__main__.py:643`)
- `_severity_event_key` (`__main__.py:843`)
- `accession_from_filename`, `normalize_cik` helpers
- The full Massive response parsing logic in `pick_float_from_massive_response` (`__main__.py:488`)

## Verification

1. **Phase 2 acceptance:** `pytest -q` green, ≥85% coverage on `severity.py` and `rules.py`, `mypy --strict` clean.
2. **Phase 3 acceptance:** Frozen 5-day historical run produces byte-identical `dilution_severity_events_all.csv`, `avoid_tickers.csv`, and `dilution_severity_by_ticker.csv` to v1's outputs for the same range, with `STRICT_WORD_BOUNDARIES=false`.
3. **Phase 4 daily check:** `scripts/compare_with_v1.py` exits 0 (no diffs) on ≥10 of 14 days. Any diff is documented in `docs/shadow_diffs.md` with a v1-bug or v2-bug verdict.
4. **Phase 5 cutover:** v2's first run with `STRICT_WORD_BOUNDARIES=true` produces a strict superset of v1's avoid-list minus the documented false positives (e.g., tickers that only matched on "maxim" inside "Maxim Integrated").
5. **Rollback drill (Phase 4, week 2):** Deliberately disable v2's cron for one day, confirm v1 still runs cleanly, then re-enable v2. Documents the runbook works.
