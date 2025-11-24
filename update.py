import yfinance as yf
import pandas as pd
import os

# 台股 tickers
tickers_tw = [
    "2301.TW","2303.TW","2308.TW","2317.TW","2327.TW",
    "2330.TW","2345.TW","2357.TW","2382.TW","2383.TW",
    "3017.TW","3231.TW","3711.TW","6669.TW","2379.TW",
    "2395.TW","2454.TW","3008.TW","3034.TW","4938.TW",
    "0050.TW","0053.TW","0055.TW"
]

# 美股 tickers（12 檔）
tickers_us = [
    "QQQ", "TLT", "GLD", "HGER", "DBMF", "CTA",
    "BTAL", "XLE", "SPY", "XLP", "XLV", "IEF"
]

# Create folders
os.makedirs("tw", exist_ok=True)
os.makedirs("us", exist_ok=True)

# Download function
def download_and_save(ticker, path_prefix):
    print(f"Downloading {ticker}...")
    df = yf.download(ticker, start="2000-01-01", auto_adjust=True)

    if df.empty:
        print(f"⚠️ {ticker} has no data!")
        return

    df.to_csv(f"{path_prefix}/{ticker}.csv")
    print(f"Saved {ticker}.csv")

# Taiwan
for t in tickers_tw:
    download_and_save(t, "tw")

# US
for t in tickers_us:
    download_and_save(t, "us")

print("Done 🎉")

