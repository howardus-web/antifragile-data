import argparse
import json
import hashlib
from pathlib import Path

import pandas as pd


# ---------------------------
# Config (market-specific)
# ---------------------------
MARKETS = {
    "us": {
        "in_dir": Path("us"),
        "out_dir": Path("matrices/us"),
        "preferred_master": ["QQQ", "SPY"],
    },
    "tw": {
        "in_dir": Path("tw"),
        "out_dir": Path("matrices/tw"),
        # TW master calendar preference: broad market proxy first
        "preferred_master": ["0050.TW", "0055.TW", "2330.TW"],
    },
}

OUT_FILES = {
    "prices": "prices.parquet",
    "month_ends": "calendar_month_ends.csv",
    "health": "health_report.csv",
    "summary": "summary.txt",
    "manifest": "manifest.json",
}
# ---------------------------


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_single_csv_fixed_format(path: Path) -> pd.Series:
    """
    Fixed CSV format (your long-term contract):
    - first 3 rows are junk header (Price/Ticker/Date)
    - real data starts from row 4
    - columns: Date, AdjClose
    - Date format: YYYY/M/D (may not be zero-padded)
    """
    df = pd.read_csv(path, skiprows=3, header=None, names=["Date", "AdjClose"])
    df["Date"] = pd.to_datetime(df["Date"], errors="raise").dt.normalize()

    # Sort + deterministic de-dup
    df = df.sort_values("Date")
    df = df.drop_duplicates("Date", keep="last")

    df["AdjClose"] = pd.to_numeric(df["AdjClose"], errors="raise")

    s = df.set_index("Date")["AdjClose"]
    # Keep stem as-is (e.g., 0050.TW); uppercase for consistency
    s.name = path.stem.upper()
    return s


def pick_master_ticker(preferred_master: list[str], available: set[str]) -> str:
    for t in preferred_master:
        if t in available:
            return t
    return sorted(list(available))[0]


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def build_market(market: str) -> None:
    if market not in MARKETS:
        raise ValueError(f"未知 market: {market}. 可用: {sorted(MARKETS.keys())}")

    in_dir: Path = MARKETS[market]["in_dir"]
    out_dir: Path = MARKETS[market]["out_dir"]
    preferred_master: list[str] = MARKETS[market]["preferred_master"]

    if not in_dir.exists():
        raise FileNotFoundError(f"找不到資料夾：{in_dir}（repo 根目錄應該要有 {in_dir}/）")

    csv_files = sorted([p for p in in_dir.glob("*.csv") if p.is_file()])
    if not csv_files:
        raise FileNotFoundError(f"{in_dir} 裡找不到任何 .csv 檔")

    # Load series
    series_map: dict[str, pd.Series] = {}
    file_meta: dict[str, dict] = {}

    for p in csv_files:
        t = p.stem.upper()
        s = clean_single_csv_fixed_format(p)
        series_map[t] = s
        file_meta[t] = {
            "file": str(p.as_posix()),
            "sha256": sha256_file(p),
            "rows": int(s.shape[0]),
            "min_date": str(s.index.min().date()),
            "max_date": str(s.index.max().date()),
        }

    tickers = sorted(series_map.keys())
    master = pick_master_ticker(preferred_master, set(tickers))
    master_dates = series_map[master].index

    # Build FULL matrix aligned to master calendar
    df = pd.DataFrame(index=master_dates)
    for t in tickers:
        df[t] = series_map[t].reindex(master_dates)

    out_dir.mkdir(parents=True, exist_ok=True)

    out_prices = out_dir / OUT_FILES["prices"]
    out_month_ends = out_dir / OUT_FILES["month_ends"]
    out_health = out_dir / OUT_FILES["health"]
    out_summary = out_dir / OUT_FILES["summary"]
    out_manifest = out_dir / OUT_FILES["manifest"]

    # Save matrix (parquet)
    df.to_parquet(out_prices)

    # True month-end trading days from master calendar
    month_end_dates = (
        pd.DataFrame({"MonthEndDate": master_dates})
        .assign(Year=master_dates.year, Month=master_dates.month)
        .groupby(["Year", "Month"], as_index=False)["MonthEndDate"]
        .max()
    )
    month_end_dates["MonthEndDate"] = month_end_dates["MonthEndDate"].dt.strftime("%Y-%m-%d")
    month_end_dates.to_csv(out_month_ends, index=False, encoding="utf-8-sig")

    # Health report
    master_max = master_dates.max().date()
    rows = []
    for t in tickers:
        s = series_map[t]
        rows.append({
            "Ticker": t,
            "MinDate": str(s.index.min().date()),
            "MaxDate": str(s.index.max().date()),
            "Rows": int(s.shape[0]),
            "LagToMasterMax_Days": int((master_max - s.index.max().date()).days),
        })
    health = pd.DataFrame(rows).sort_values(["LagToMasterMax_Days", "Ticker"], ascending=[False, True])

    # Missing stats on the FULL matrix
    missing_counts = df.isna().sum().astype(int)
    health["MissingInFullMatrix"] = health["Ticker"].map(missing_counts.to_dict()).astype(int)
    health["MissingPctInFullMatrix"] = (health["MissingInFullMatrix"] / len(master_dates) * 100.0).round(3)
    health.to_csv(out_health, index=False, encoding="utf-8-sig")

    # Manifest
    manifest = {
        "scope": f"{market.upper()}_FULL_MATRIX",
        "in_dir": str(in_dir.as_posix()),
        "master_ticker": master,
        "master_min_date": str(master_dates.min().date()),
        "master_max_date": str(master_dates.max().date()),
        "master_trading_days": int(len(master_dates)),
        "tickers": tickers,
        "matrix": {
            "path": str(out_prices.as_posix()),
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "min_date": str(df.index.min().date()),
            "max_date": str(df.index.max().date()),
            "nan_cells": int(df.isna().sum().sum()),
        },
        "month_ends": {
            "path": str(out_month_ends.as_posix()),
            "months": int(month_end_dates.shape[0]),
        },
        "health_report": {"path": str(out_health.as_posix())},
        "files": file_meta,
    }
    out_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    # Summary
    nan_cells = int(df.isna().sum().sum())
    summary = []
    summary.append(f"Data Layer Summary (FULL matrix) — {market.upper()}\n")
    summary.append("================================\n\n")
    summary.append(f"Master calendar : {master}\n")
    summary.append(f"Trading days    : {len(master_dates)}\n")
    summary.append(f"Master min date : {master_dates.min().date()}\n")
    summary.append(f"Master max date : {master_dates.max().date()}\n")
    summary.append(f"Matrix shape    : {df.shape[0]} x {df.shape[1]}\n")
    summary.append(f"NaN cells (info): {nan_cells}\n")
    summary.append("\nNOTE: NaN here is often normal (different tickers start dates). "
                   "Backtest layer should decide the valid common-start window.\n\n")
    summary.append("Per-ticker MaxDate lag to master max (days) [larger = more stale]:\n")
    lag_sorted = health[["Ticker", "LagToMasterMax_Days"]].sort_values("LagToMasterMax_Days", ascending=False)
    for _, r in lag_sorted.iterrows():
        summary.append(f"- {r['Ticker']}: {int(r['LagToMasterMax_Days'])}\n")
    summary.append("\nTop missing ratios in FULL matrix (often just late inception):\n")
    miss_sorted = health[["Ticker", "MissingPctInFullMatrix"]].sort_values("MissingPctInFullMatrix", ascending=False).head(8)
    for _, r in miss_sorted.iterrows():
        summary.append(f"- {r['Ticker']}: {float(r['MissingPctInFullMatrix'])}%\n")

    write_text(out_summary, "".join(summary))

    print(f"✅ 完成 {market.upper()}（FULL matrix + health + summary）：")
    print(f" - {out_prices}")
    print(f" - {out_month_ends}")
    print(f" - {out_health}")
    print(f" - {out_summary}")
    print(f" - {out_manifest}")
    print(f"Master = {master}, days = {len(master_dates)}, shape = {df.shape[0]} x {df.shape[1]}, NaN cells = {nan_cells}")


def main():
    parser = argparse.ArgumentParser(description="Build FULL price matrices (US/TW) into matrices/<market>/")
    parser.add_argument("--market", choices=sorted(MARKETS.keys()), help="Only build a single market (default: build all).")
    args = parser.parse_args()

    if args.market:
        build_market(args.market)
    else:
        for m in sorted(MARKETS.keys()):
            build_market(m)


if __name__ == "__main__":
    main()
