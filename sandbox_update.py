"""
sandbox_update.py
=================
手動抓取 sandbox/ 實驗性標的資料。
CSV 格式與 us/ 完全一致，可直接被引擎讀取。

使用方式：
    python sandbox_update.py

新增標的：把 ticker 加進下方 TICKERS 清單，重跑即可。
不接 CI，不影響 us/ tw/ 的任何內容。
"""

import os
import yfinance as yf

# ── 實驗性標的清單（自由新增）──────────────────────────────
TICKERS = [
   
    "IEFM",  
    

    
]
# ──────────────────────────────────────────────────────────

START_DATE = "2000-01-01"
FOLDER     = "sandbox"

os.makedirs(FOLDER, exist_ok=True)

def download(ticker):
    print(f"Downloading {ticker} ...")
    df = yf.download(ticker, start=START_DATE, auto_adjust=True, progress=False)

    if df.empty:
        print(f"  ⚠ No data: {ticker}")
        return

    df = df[["Close"]].copy()
    df.rename(columns={"Close": "AdjClose"}, inplace=True)
    df.to_csv(f"{FOLDER}/{ticker}.csv")
    print(f"  ✓ Saved: {FOLDER}/{ticker}.csv  ({len(df)} rows)")

for t in TICKERS:
    download(t)

print("\nDone.")
