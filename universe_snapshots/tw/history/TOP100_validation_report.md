# TW Electronics Top100 Historical Reconstruction — Validation Report

> **Research-only extension. Does not supersede canonical v3 Top60 and does not alter
> the production universe. Ranks 1–60 are validated 38/38 by exact canonical match;
> ranks 61–100 are methodology-consistent extensions without an independent
> golden-match reference.**

Research-only. Builder: `scripts/tw_hist_build_v3.py` (unmodified from Step 3 smoke-test version).
Hard gate: 38/38 dates must be 60/60 exact match vs canonical v3 to PASS overall.

## Per-date results

| # | shares_date | rebuilt count | Top60 match | 60/60 | first mismatch | truncation flag | status |
|---|---|---|---|---|---|---|---|
| 1 | 20070831 | 100 | 60/60 | YES | — | no | **PASS** |
| 2 | 20080229 | 100 | 60/60 | YES | — | no | **PASS** |
| 3 | 20080829 | 100 | 60/60 | YES | — | no | **PASS** |
| 4 | 20090227 | 100 | 60/60 | YES | — | no | **PASS** |
| 5 | 20090831 | 100 | 60/60 | YES | — | no | **PASS** |
| 6 | 20100226 | 100 | 60/60 | YES | — | no | **PASS** |
| 7 | 20100831 | 100 | 60/60 | YES | — | no | **PASS** |
| 8 | 20110225 | 100 | 60/60 | YES | — | no | **PASS** |
| 9 | 20110831 | 100 | 60/60 | YES | — | no | **PASS** |
| 10 | 20120229 | 100 | 60/60 | YES | — | no | **PASS** |
| 11 | 20120831 | 100 | 60/60 | YES | — | no | **PASS** |
| 12 | 20130227 | 100 | 60/60 | YES | — | no | **PASS** |
| 13 | 20130830 | 100 | 60/60 | YES | — | no | **PASS** |
| 14 | 20140227 | 100 | 60/60 | YES | — | no | **PASS** |
| 15 | 20140829 | 100 | 60/60 | YES | — | no | **PASS** |
| 16 | 20150226 | 100 | 60/60 | YES | — | no | **PASS** |
| 17 | 20150831 | 100 | 60/60 | YES | — | no | **PASS** |
| 18 | 20160225 | 100 | 60/60 | YES | — | no | **PASS** |
| 19 | 20160831 | 100 | 60/60 | YES | — | no | **PASS** |
| 20 | 20170224 | 100 | 60/60 | YES | — | no | **PASS** |
| 21 | 20170831 | 100 | 60/60 | YES | — | no | **PASS** |
| 22 | 20180227 | 100 | 60/60 | YES | — | no | **PASS** |
| 23 | 20180831 | 100 | 60/60 | YES | — | no | **PASS** |
| 24 | 20190227 | 100 | 60/60 | YES | — | no | **PASS** |
| 25 | 20190830 | 100 | 60/60 | YES | — | no | **PASS** |
| 26 | 20200227 | 100 | 60/60 | YES | — | no | **PASS** |
| 27 | 20200831 | 100 | 60/60 | YES | — | no | **PASS** |
| 28 | 20210226 | 100 | 60/60 | YES | — | no | **PASS** |
| 29 | 20210831 | 100 | 60/60 | YES | — | no | **PASS** |
| 30 | 20220225 | 100 | 60/60 | YES | — | no | **PASS** |
| 31 | 20220831 | 100 | 60/60 | YES | — | no | **PASS** |
| 32 | 20230224 | 100 | 60/60 | YES | — | no | **PASS** |
| 33 | 20230831 | 100 | 60/60 | YES | — | no | **PASS** |
| 34 | 20240229 | 100 | 60/60 | YES | — | no | **PASS** |
| 35 | 20240830 | 100 | 60/60 | YES | — | no | **PASS** |
| 36 | 20250227 | 100 | 60/60 | YES | — | no | **PASS** |
| 37 | 20250829 | 100 | 60/60 | YES | — | no | **PASS** |
| 38 | 20260226 | 100 | 60/60 | YES | — | no | **PASS** |

## Summary

**38/38 PASS**

- 0 dates with suspected API truncation (per-industry rowcount heuristic: any single industry code returning <5 rows on a date is flagged; minimum ever observed across all 38 dates x 8 industries = 8 rows, industry code 30 資訊服務)
- 0 dates required MI_QFIIS to walk back from the requested as-of-date (shares data was available exactly on the resolved trading day every time)
- 0 mismatches anywhere in any date's Top60 prefix
- Cross-checked with an independent earlier run (diagnostics-only, before ranked_list persistence was added): identical top60_match result for all 38 dates

## Scope note on ranks 61-100

The hard gate only checks ranks 1-60 against canonical v3, because that's the only depth canonical v3 itself covers — there is no existing independent reference to golden-match ranks 61-100 against. Their correctness rests on: (a) identical build mechanism as ranks 1-60 within the same run (same industry fetch, same shares fetch, same ranking step, just not truncated as early), and (b) the same per-date API-integrity checks (industry rowcount, truncation heuristic) applied to the full run, not just the first 60. This is not the same strength of evidence as a direct golden-match, and is stated as such in the artifact's own `status` field.

## Deliverable filenames / suggested placement

- Artifact: `tw_ranked_universe_history_2007H2-2026H1_TOP100.json` → suggested path `antifragile-data/universe_snapshots/tw/history/` (alongside v1/v2/v3, not replacing any of them)
- This report: `TOP100_validation_report.md` → same directory, or wherever v3's own `BUILD_NOTES_v3.md` lives, for consistency