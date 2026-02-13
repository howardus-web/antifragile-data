import os
import json
import hashlib
from pathlib import Path
import pandas as pd

# --------- Config (you can edit later if needed) ----------
PREFERRED_MASTER = ["QQQ", "SPY"]  # pick first available as master calendar
RAW_DIR_CANDIDATES = [
    Path("us"),
    Path("data/us"),
    Path("raw/us"),
    Path("raw_data/us"),
]
OUT_DIR = Path("matrices")
OUT_PRICES_PARQUET = OUT_DIR / "us_prices.parquet"
OUT_MONTH_ENDS_CSV = OUT_DIR / "calendar_month_ends.csv"
OUT_MANIFEST_JSON = OUT_DIR / "manifest_us.json"
# ---------------------------------------------------------

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def find_us_dir() -> Path:
    for d in RAW_DIR_CANDIDATES:
        if d.exists() and d.is_dir():
            return d
    raise FileNotFoundError(
        "找不到 US 資料夾。請確認 repo 裡有 us/ 或 data/us/ 或 raw_data/us/ 之類的目錄。"
    )

def clean_single_csv(path: Path) -> pd.Series:
    # Your fixed format: first 3 rows are junk header; real data starts row 4: Date, AdjClose
    df = pd.read_csv(path, skiprows=3, header=None, names=["Date", "AdjClose"])
    # parse date robustly (supports YYYY/M/D)
    df["Date"] = pd.to_datetime(df["Date"], errors="raise")
    # normalize to date only (avoid any timestamp issues)
    df["Date"] = df["Date"].dt.normalize()
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
    # fallback: pick any one deterministically
    return sorted(list(available))[0]

def main():
    us_dir = find_us_dir()
    csv_files = sorted([p for p in us_dir.glob("*.csv") if p.is_file()])
    if not csv_files:
        raise FileNotFoundError(f"{us_dir} 裡找不到任何 .csv 檔")

    tickers = [p.stem.upper() for p in csv_files]
    available = set(tickers)

    master = pick_master_ticker(available)
    master_path = us_dir / f"{master}.csv"
    if not master_path.exists():
        # in case file name casing differs
        master_path = None
        for p in csv_files:
            if p.stem.upper() == master:
                master_path = p
                break
        if master_path is None:
            raise FileNotFoundError(f"找不到 master ticker 檔案：{master}.csv")

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
            "dup_dates_after_clean": 0,  # by construction
        }

    # Master calendar
    master_s = series_map[master]
    master_dates = master_s.index

    # Build matrix aligned to master calendar
    df = pd.DataFrame(index=master_dates)
    for t in sorted(series_map.keys()):
        df[t] = series_map[t].reindex(master_dates)

    # Do NOT fill NaN here; keep raw alignment deterministic
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save prices matrix
    df.to_parquet(OUT_PRICES_PARQUET)

    # Precompute month-end trading days from master calendar (true last trading day)
    # group by year, month -> last date
    month_end_dates = (
        pd.Series(master_dates)
        .groupby([master_dates.year, master_dates.month])
        .max()
        .reset_index()
    )
    month_end_dates.columns = ["Year", "Month", "MonthEndDate"]
    month_end_dates["MonthEndDate"] = month_end_dates["MonthEndDate"].dt.strftime("%Y-%m-%d")
    month_end_dates.to_csv(OUT_MONTH_ENDS_CSV, index=False, encoding="utf-8-sig")

    # Manifest
    manifest = {
        "scope": "US",
        "us_dir": str(us_dir.as_posix()),
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
    print(f"Master calendar = {master} ({manifest['master_trading_days']} trading days)")

if __name__ == "__main__":
    main()
