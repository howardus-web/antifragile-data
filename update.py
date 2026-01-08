
import yfinance as yf
import pandas as pd
import os

tickers_tw = [
    "2301.TW","2303.TW","2308.TW","2317.TW","2327.TW",
    "2330.TW","2345.TW","2357.TW","2382.TW","2383.TW",
    "3017.TW","3231.TW","3711.TW","6669.TW","2379.TW",
    "2395.TW","2454.TW","3008.TW","3034.TW","4938.TW",
    "0050.TW","0053.TW","0055.TW"
]

tickers_us = [
    "QQQ","TLT","GLD","HGER","DBMF","CTA",
    "BTAL","XLE","SPY","XLP","XLV","IEF",
    "ALLW","TWD=X"
]

os.makedirs("tw", exist_ok=True)
os.makedirs("us", exist_ok=True)

def download_adj_close(ticker, folder):
    print("Downloading", ticker)
    df = yf.download(ticker, start="2000-01-01", auto_adjust=True)

    if df.empty:
        print("⚠ No data:", ticker)
        return

    df = df[["Close"]]            # auto_adjust=True → Close = Adjusted Close
    df.rename(columns={"Close": "AdjClose"}, inplace=True)

    df.to_csv(f"{folder}/{ticker}.csv")
    print("Saved:", folder, ticker)

for t in tickers_tw:
    download_adj_close(t, "tw")

for t in tickers_us:
    download_adj_close(t, "us")
