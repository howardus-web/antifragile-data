# Universe snapshots

This folder stores **deterministic universe snapshots** (制度時間) for TW.

- `tw/universe_2025-04-30_v0_bootstrap.json`: bootstrap universe used since launch.
- `tw/universe_2026-02-28_v1_rotation.json`: first rotation universe (effective 2026-02 month-end, based on 2025-08 snapshot).
- `tw/universe_2026-08-31_v2_rotation.json`: second rotation universe (effective 2026-08 month-end, based on 2026-02 month-end ranking, trading day 2026-02-26). Built via `tw_universe_builder.py` (TWSE official data: t187ap03_L industry codes 24-31 + MI_INDEX close, ex-telecom 2412/3045/4904; methodology validated 20/20 vs official electronics index page same-day 2026-06-30).

Rule (from 2026-02): update frequency = 6 months, lag = 6 months.

Rotation procedure (from 2026-08): run `tw_universe_builder.py` (Project files) with `--as-of-date` = ranking month-end trading day (check `matrices/tw/calendar_month_ends.csv`), commit the new snapshot json **together with** the new tickers' price CSVs in `tw/` and the updated `tickers_tw` list in `update.py` — the engine requires price files for the union of ALL snapshots' tickers immediately upon commit, regardless of effective date. Old tickers stay in `update.py` (historical snapshots still need them).
