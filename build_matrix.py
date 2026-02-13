import json
import hashlib
from pathlib import Path

import pandas as pd


# ---------------------------
# Config
# ---------------------------
PREFERRED_MASTER = ["QQQ", "SPY"]  # pick first available as master calendar
US_DIR = Path("us")               # your repo structure is antifragile-data/us/*.csv
OUT_DIR = Path("matrices")

OUT_PRICES_PARQUET = OUT_DIR / "us_prices.parquet"
OUT_MONTH_ENDS_CSV = OUT_DIR / "calendar_month_ends.csv"
OUT_MANIFEST_JSON = OUT_DIR / "manifest_us.json"
# ---------------------------


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_single_csv(path: Path) -> pd.Series:
    """
    Your fixed format:
    - first 3 rows are junk header (Price/Ticker/Date)
    - real data starts from row 4
    - columns: Date, AdjClose
    - Date format: YYYY/M/D (may not be zero-padded)
    """
    df = pd.read_csv(path, skiprows=3, header=None, names=["Date", "AdjClose"])

    df["Date"] = pd.to_datetime(df["Date"], errors="raise")
    df["Date"] = df["Date"].dt.normalize()  # ensure pure date (no time)
    df = df.sort_values("Date")
    df = df.drop_duplicates("Date", keep="last")  # critical deterministic rule
    df["AdjClose"] = pd.to_numeric(df["AdjClose"], errors="raise")

    s = df.set_index("Date")["AdjClose"]
    s.name = path.stem.upper()
    return s


def pick_master_ticker(available: set) -> str:
    for t in PREFERRED_MASTER:
        if t in available:
            return t
    return sorted(list(available))[0]


def main():
    if not US_DIR.exists():
        raise FileNotFoundError(f"找不到資料夾：{US_DIR}. 你的 repo 應該要有 us/ 目錄。")

    csv_files = sorted([p for p in US_DIR.glob("*.csv") if p.is_file()])
    if not csv_files:
        raise FileNotFoundError(f"{US_DIR} 裡找不到任何 .csv 檔")

    tickers = [p.stem.upper() for p in csv_files]
    available = set(tickers)

    master = pick_master_ticker(available)

    # Load all series (cleaned)
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

    if master not in series_map:
        raise FileNotFoundError(f"Master ticker {master} not found after loading. Something is wrong with filenames.")

    # Master calendar
    master_dates = series_map[master].index

    # Build aligned matrix
    df = pd.DataFrame(index=master_dates)
    for t in sorted(series_map.keys()):
        df[t] = series_map[t].reindex(master_dates)

    # Keep NaN as-is (deterministic); do not fill here
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save matrix
    df.to_parquet(OUT_PRICES_PARQUET)

    # Compute true month-end trading days from master calendar
    month_end_dates = (
        pd.DataFrame({"MonthEndDate": master_dates})
        .assign(Year=master_dates.year, Month=master_dates.month)
        .groupby(["Year", "Month"], as_index=False)["MonthEndDate"]
        .max()
    )
    month_end_dates["MonthEndDate"] = month_end_dates["MonthEndDate"].dt.strftime("%Y-%m-%d")
    month_end_dates.to_csv(OUT_MONTH_ENDS_CSV, index=False, encoding="utf-8-sig")

    # Manifest
    manifest = {
        "scope": "US",
        "us_dir": str(US_DIR.as_posix()),
        "master_ticker": master,
        "master_min_date": str(master_dates.min().date()),
        "master_max_date": str(master_dates.max().date()),
        "master_trading_days": int(len(master_dates)),
        "tickers": sorted(list(series_map.keys())),
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
        "files": file_meta,
    }
    OUT_MANIFEST_JSON.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("✅ 完成：")
    print(f" - {OUT_PRICES_PARQUET}")
    print(f" - {OUT_MONTH_ENDS_CSV}")
    print(f" - {OUT_MANIFEST_JSON}")
    print(f"Master calendar = {master} ({len(master_dates)} trading days)")
    print(f"Matrix shape = {df.shape[0]} x {df.shape[1]}, NaN cells = {int(df.isna().sum().sum())}")


if __name__ == "__main__":
    main()
