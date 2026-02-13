import json
import hashlib
from pathlib import Path

import pandas as pd


# ---------------------------
# Config
# ---------------------------
PREFERRED_MASTER = ["QQQ", "SPY"]  # master calendar preference
US_DIR = Path("us")               # repo structure: us/*.csv
OUT_DIR = Path("matrices/us")

OUT_PRICES_PARQUET = OUT_DIR / "prices.parquet"
OUT_MONTH_ENDS_CSV = OUT_DIR / "calendar_month_ends.csv"
OUT_HEALTH_CSV = OUT_DIR / "health_report.csv"
OUT_SUMMARY_TXT = OUT_DIR / "summary.txt"
OUT_MANIFEST_JSON = OUT_DIR / "manifest.json"

# ---------------------------


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_single_csv(path: Path) -> pd.Series:
    """
    Fixed format:
    - first 3 rows are junk header (Price/Ticker/Date)
    - real data starts from row 4
    - columns: Date, AdjClose
    - Date format: YYYY/M/D (may not be zero-padded)
    """
    df = pd.read_csv(path, skiprows=3, header=None, names=["Date", "AdjClose"])
    df["Date"] = pd.to_datetime(df["Date"], errors="raise").dt.normalize()

    # Sort + deterministic de-dup (your key guardrail)
    df = df.sort_values("Date")
    df = df.drop_duplicates("Date", keep="last")

    df["AdjClose"] = pd.to_numeric(df["AdjClose"], errors="raise")

    s = df.set_index("Date")["AdjClose"]
    s.name = path.stem.upper()
    return s


def pick_master_ticker(available: set) -> str:
    for t in PREFERRED_MASTER:
        if t in available:
            return t
    return sorted(list(available))[0]


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def main():
    if not US_DIR.exists():
        raise FileNotFoundError(f"找不到資料夾：{US_DIR}（repo 根目錄應該要有 us/）")

    csv_files = sorted([p for p in US_DIR.glob("*.csv") if p.is_file()])
    if not csv_files:
        raise FileNotFoundError(f"{US_DIR} 裡找不到任何 .csv 檔")

    # Load series
    series_map = {}
    file_meta = {}
    for p in csv_files:
        t = p.stem.upper()
        s = clean_single_csv(p)
        series_map[t] = s
        file_meta[t] = {
            "file": str(p.as_posix()),
            "sha256": sha256_file(p),
            "rows": int(s.shape[0]),
            "min_date": str(s.index.min().date()),
            "max_date": str(s.index.max().date()),
        }

    tickers = sorted(series_map.keys())
    master = pick_master_ticker(set(tickers))
    master_dates = series_map[master].index

    # Build FULL matrix aligned to master calendar
    df = pd.DataFrame(index=master_dates)
    for t in tickers:
        df[t] = series_map[t].reindex(master_dates)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save matrix (parquet)
    df.to_parquet(OUT_PRICES_PARQUET)

    # True month-end trading days from master calendar
    month_end_dates = (
        pd.DataFrame({"MonthEndDate": master_dates})
        .assign(Year=master_dates.year, Month=master_dates.month)
        .groupby(["Year", "Month"], as_index=False)["MonthEndDate"]
        .max()
    )
    month_end_dates["MonthEndDate"] = month_end_dates["MonthEndDate"].dt.strftime("%Y-%m-%d")
    month_end_dates.to_csv(OUT_MONTH_ENDS_CSV, index=False, encoding="utf-8-sig")

    # Health report (human-readable, Excel-friendly)
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

    # Missing stats on the FULL matrix (informational; not treated as error)
    missing_counts = df.isna().sum().astype(int)
    health["MissingInFullMatrix"] = health["Ticker"].map(missing_counts.to_dict()).astype(int)
    health["MissingPctInFullMatrix"] = (health["MissingInFullMatrix"] / len(master_dates) * 100.0).round(3)

    health.to_csv(OUT_HEALTH_CSV, index=False, encoding="utf-8-sig")

    # Manifest (machine-readable)
    manifest = {
        "scope": "US_FULL_MATRIX",
        "us_dir": str(US_DIR.as_posix()),
        "master_ticker": master,
        "master_min_date": str(master_dates.min().date()),
        "master_max_date": str(master_dates.max().date()),
        "master_trading_days": int(len(master_dates)),
        "tickers": tickers,
        "matrix": {
            "path": str(OUT_PRICES_PARQUET.as_posix()),
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "min_date": str(df.index.min().date()),
            "max_date": str(df.index.max().date()),
            "nan_cells": int(df.isna().sum().sum()),
        },
        "month_ends": {
            "path": str(OUT_MONTH_ENDS_CSV.as_posix()),
            "months": int(month_end_dates.shape[0]),
        },
        "health_report": {
            "path": str(OUT_HEALTH_CSV.as_posix()),
        },
        "files": file_meta,
    }
    OUT_MANIFEST_JSON.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    # Summary (the one you actually look at)
    nan_cells = int(df.isna().sum().sum())
    summary = []
    summary.append("Data Layer Summary (FULL matrix)\n")
    summary.append("================================\n\n")
    summary.append(f"Master calendar : {master}\n")
    summary.append(f"Trading days    : {len(master_dates)}\n")
    summary.append(f"Master min date : {master_dates.min().date()}\n")
    summary.append(f"Master max date : {master_dates.max().date()}\n")
    summary.append(f"Matrix shape    : {df.shape[0]} x {df.shape[1]}\n")
    summary.append(f"NaN cells (info): {nan_cells}\n")
    summary.append("\nNOTE: NaN here is often normal (different ETF start dates). "
                   "Backtest layer should decide the valid common-start window.\n\n")
    summary.append("Per-ticker MaxDate lag to master max (days) [larger = more stale]:\n")
    lag_sorted = health[["Ticker", "LagToMasterMax_Days"]].sort_values("LagToMasterMax_Days", ascending=False)
    for _, r in lag_sorted.iterrows():
        summary.append(f"- {r['Ticker']}: {int(r['LagToMasterMax_Days'])}\n")
    summary.append("\nTop missing ratios in FULL matrix (often just late inception):\n")
    miss_sorted = health[["Ticker", "MissingPctInFullMatrix"]].sort_values("MissingPctInFullMatrix", ascending=False).head(8)
    for _, r in miss_sorted.iterrows():
        summary.append(f"- {r['Ticker']}: {float(r['MissingPctInFullMatrix'])}%\n")

    write_text(OUT_SUMMARY_TXT, "".join(summary))

    print("✅ 完成（FULL matrix + health + summary）：")
    print(f" - {OUT_PRICES_PARQUET}")
    print(f" - {OUT_MONTH_ENDS_CSV}")
    print(f" - {OUT_HEALTH_CSV}")
    print(f" - {OUT_SUMMARY_TXT}")
    print(f" - {OUT_MANIFEST_JSON}")
    print(f"Master = {master}, days = {len(master_dates)}, shape = {df.shape[0]} x {df.shape[1]}, NaN cells = {nan_cells}")


if __name__ == "__main__":
    main()
