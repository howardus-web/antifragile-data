"""
tw_pit_price_coverage_audit.py — read-only TW PIT price-coverage auditor.

What this is
-------------
A narrow, read-only diagnostic utility. It answers one question: for the
canonical TW_CANONICAL_60 PIT roster history, does every (month, required
ticker) pair have the price coverage the production D-signal (RS - SMA12(RS))
and one-month-forward-return computation actually need?

It does three things, all read-only:
  1. Reports PITRosterProvider.coverage_gap ticker-months — rank-eligible
     names that were dropped from the roster entirely because tw/<code>.TW.csv
     does not exist (or existed but the provider's priced-ticker detection
     could not see it).
  2. Reports, for every ticker that DID make it into a month's roster,
     whether that month's own price, D-signal, or one-month-forward price
     is missing — classified into:
       - STRUCTURAL_GLOBAL_WARMUP  (month is within the first 12 calendar
         months of the master calendar; no ticker can have a defined D yet)
       - STRUCTURAL_OWN_WARMUP     (ticker's own price history is <12 months
         old as of this month; same SMA12 mathematics, just ticker-local)
       - GENUINE_GAP               (price/D/forward-return missing despite
         the ticker having >=12 months of its own prior history and this
         being outside the first global warmup year — needs a human/GPT
         look, this script does NOT try to explain it further)
  3. Prints a one-line summary suitable for a monthly/ad-hoc health check.

What this is explicitly NOT
----------------------------
  - It does not repair anything. No network calls, no writes to tw/*.csv,
    no writes to matrices/. Repair is a separate, deliberate, reviewed step
    (see the WTC TW Historical Price Coverage Repair trigger and its
    OWNER_ACTIONS.md handoff for the established repair convention).
  - It does not reimplement roster/eligibility/D-signal logic. It imports
    tw_pit_roster.PITRosterProvider and tw_rs_engine_v331.TWRSDeterministicEngine
    verbatim and only reads their outputs.
  - It is not wired into update.py / build_matrix.py / the daily GitHub
    Actions pipeline. It is a standalone CLI for ad-hoc or scheduled
    (Owner-triggered) auditing, not a new automatic production dependency.

Note on module location
------------------------
tw_pit_roster.py and tw_rs_engine_v331.py are MAS project engine modules,
not part of this (antifragile-data) repo — this repo is the price/matrix
data layer only. This script therefore takes --engine-path pointing at
wherever those two files live on the machine running the audit (the same
directory MAS's own render_config_report.py / mas_run.py resolve them
from); it does not assume they are importable by default.

Usage
-----
    python scripts/tw_pit_price_coverage_audit.py --repo . \
        --engine-path /path/to/mas/engine/modules [--json out.json]

Exit code is always 0 (this is a report, not a gate) unless an unexpected
exception occurs while loading canonical modules — in which case it exits 1
and prints the exception, since that would mean the audit itself could not
run, not that a data gap was found.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def _load_canonical(repo_root: Path, engine_path: Path | None):
    sys.path.insert(0, str(repo_root))
    if engine_path is not None:
        sys.path.insert(0, str(engine_path))
    import tw_pit_roster as pit  # type: ignore
    import tw_rs_engine_v331 as eng_mod  # type: ignore
    return pit, eng_mod


def _master_calendar(repo_root: Path) -> pd.Series:
    """Independent of matrices/tw (the artifact under audit): derive month-ends
    from 0050.TW's own source CSV, same convention build_matrix.py uses to
    pick its master ticker when 0050.TW is present."""
    px = pd.read_csv(repo_root / "tw" / "0050.TW.csv", skiprows=3, header=None,
                      names=["Date", "AdjClose"])
    px["Date"] = pd.to_datetime(px["Date"])
    month_ends = (
        px.groupby([px["Date"].dt.year, px["Date"].dt.month])["Date"]
        .max()
        .sort_values()
        .reset_index(drop=True)
    )
    today = pd.Timestamp.today()
    month_ends = month_ends[~((month_ends.dt.year == today.year) &
                               (month_ends.dt.month == today.month))]
    return month_ends


def run_audit(repo_root: Path, engine_path: Path | None = None) -> dict:
    pit, eng_mod = _load_canonical(repo_root, engine_path)

    provider = pit.PITRosterProvider(
        repo_root=repo_root, universe_mode=pit.UNIVERSE_CANONICAL_60,
        apply_eligibility=True,
    )
    engine = eng_mod.TWRSDeterministicEngine(repo_root=repo_root)

    D = engine.monthly_D_matrix()
    month_px = engine.month_end_prices()
    calendar = _master_calendar(repo_root)

    global_warmup_end = calendar.iloc[0] + pd.DateOffset(months=12)

    coverage_gap_rows = []
    required_gap_rows = []
    depth_stats = []

    for i, t in enumerate(calendar):
        snap = provider.snapshot_for_date(t)
        depth_stats.append(len(snap.tickers))
        for tk in snap.coverage_gap:
            coverage_gap_rows.append({"month": str(t.date()), "ticker": tk,
                                       "reason": "NO_CSV_FILE_OR_NOT_DETECTED_PRICED"})

        has_next = (i + 1) < len(calendar)
        t_next = calendar.iloc[i + 1] if has_next else None
        for tk in snap.tickers:
            px_now = month_px.loc[t, tk] if tk in month_px.columns else float("nan")
            d_now = D.loc[t, tk] if tk in D.columns else float("nan")
            px_next = (month_px.loc[t_next, tk]
                       if (has_next and tk in month_px.columns) else float("nan"))
            missing_px = pd.isna(px_now)
            missing_d = pd.isna(d_now)
            missing_fwd = has_next and pd.isna(px_next)
            if not (missing_px or missing_d or missing_fwd):
                continue

            if tk in month_px.columns:
                own_series = month_px[tk].dropna()
                own_first = own_series.index.min() if len(own_series) else None
            else:
                own_first = None
            own_warmup_end = (own_first + pd.DateOffset(months=12)
                               if own_first is not None else None)

            if t <= global_warmup_end:
                cat = "STRUCTURAL_GLOBAL_WARMUP"
            elif own_warmup_end is not None and t <= own_warmup_end:
                cat = "STRUCTURAL_OWN_WARMUP"
            else:
                cat = "GENUINE_GAP"

            required_gap_rows.append({
                "month": str(t.date()), "ticker": tk, "category": cat,
                "missing_current_price": bool(missing_px),
                "missing_D_signal": bool(missing_d),
                "missing_forward_price": bool(missing_fwd),
            })

    genuine = [r for r in required_gap_rows if r["category"] == "GENUINE_GAP"]
    genuine_tickers = sorted({r["ticker"] for r in genuine})
    coverage_gap_tickers = sorted({r["ticker"] for r in coverage_gap_rows})

    return {
        "repo_root": str(repo_root),
        "calendar_months": len(calendar),
        "calendar_range": [str(calendar.iloc[0].date()), str(calendar.iloc[-1].date())],
        "roster_depth": {"min": min(depth_stats), "max": max(depth_stats),
                          "mean": round(sum(depth_stats) / len(depth_stats), 2),
                          "target": pit.CURRENT_PRODUCTION_N if hasattr(pit, "CURRENT_PRODUCTION_N") else 60},
        "coverage_gap_tickers_ever": coverage_gap_tickers,
        "coverage_gap_row_count": len(coverage_gap_rows),
        "required_surface_gap_row_count": len(required_gap_rows),
        "genuine_gap_row_count": len(genuine),
        "genuine_gap_tickers": genuine_tickers,
        "genuine_gap_detail": genuine,
        "coverage_gap_detail": coverage_gap_rows,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", type=Path, default=Path("."),
                     help="antifragile-data repo root (default: current directory)")
    ap.add_argument("--engine-path", type=Path, default=None,
                     help="directory containing tw_pit_roster.py / tw_rs_engine_v331.py "
                          "(the MAS engine modules) if not already importable")
    ap.add_argument("--json", type=Path, default=None,
                     help="optional path to write the full machine-readable report")
    args = ap.parse_args()

    try:
        result = run_audit(args.repo, args.engine_path)
    except Exception as e:  # audit itself failed to run — this is exit 1, not a finding
        print(f"AUDIT COULD NOT RUN: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    print(f"TW PIT price-coverage audit — {result['repo_root']}")
    print(f"  calendar: {result['calendar_range'][0]} .. {result['calendar_range'][1]} "
          f"({result['calendar_months']} months)")
    rd = result["roster_depth"]
    print(f"  roster depth: min={rd['min']} max={rd['max']} mean={rd['mean']} "
          f"(target={rd['target']})")
    print(f"  coverage_gap (roster-eligible but no priced CSV at all): "
          f"{len(result['coverage_gap_tickers_ever'])} distinct tickers, "
          f"{result['coverage_gap_row_count']} ticker-month instances")
    if result["coverage_gap_tickers_ever"]:
        print(f"    -> {', '.join(result['coverage_gap_tickers_ever'])}")
    print(f"  required-surface gaps (ticker in roster, price/D/forward-return "
          f"missing): {result['required_surface_gap_row_count']} instances, of which "
          f"{result['genuine_gap_row_count']} are GENUINE_GAP "
          f"(not explained by global/own-ticker SMA12 warmup)")
    if result["genuine_gap_tickers"]:
        print(f"    -> {', '.join(result['genuine_gap_tickers'])}")
    else:
        print("    -> none: every required-surface gap is a structural warmup artifact")

    if args.json:
        args.json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  full report written to {args.json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
