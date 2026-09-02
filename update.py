import yfinance as yf
import pandas as pd
import os
import listing_guard

tickers_tw = [
    "2301.TW","2303.TW","2308.TW","2317.TW","2327.TW",
    "2330.TW","2345.TW","2357.TW","2382.TW","2383.TW",
    "3017.TW","3231.TW","3711.TW","6669.TW","2379.TW",
    "2395.TW","2454.TW","3008.TW","3034.TW","4938.TW",
    "2059.TW","3653.TW","3661.TW","0050.TW","0053.TW","0055.TW",
    "00679B.TW",
    # 2026-08 rotation 新進（見 universe_2026-08-31_v2_rotation.json）:
    "2344.TW","2360.TW","2368.TW","2408.TW","2449.TW",
    # 3037.TW／7769.TW：本輪 Owner Override／mechanical eligibility 排除，不在有效母池內，
    # 但仍在市場上、仍需要價格追蹤（3037 為 Owner Override 候選標的；7769 為未來可能滿足
    # signal eligibility 的新股），故不從此列表移除：
    "3037.TW","7769.TW",
    # 2026-09 Top60 production migration 新進（見 universe_TBD_v3_top60_migration.json，
    # rank 21-60，套用同一份 7769 listing_guard／3037 owner override 後的最終名單；
    # 32 檔，全部現存股，price_patch/backfill 已對每檔逐筆檢查過起點）:
    "3443.TW","8046.TW","6770.TW","3665.TW","2313.TW",
    "3036.TW","2404.TW","3481.TW","3044.TW","2337.TW",
    "4958.TW","3533.TW","6239.TW","6515.TW","6442.TW",
    "2356.TW","2376.TW","3189.TW","2324.TW","6139.TW",
    "2409.TW","2347.TW","3702.TW","6805.TW","8210.TW",
    "2474.TW","6415.TW","6789.TW","3706.TW","6781.TW",
    "5269.TW","2385.TW",
]

tickers_us = [
    "QQQ","TLT","GLD","HGER","DBMF","CTA",
    "BTAL","XLE","SPY","XLP","XLV","IEF",
    "ALLW","BRK-B",
    "CSNDX.SW","IGLN.L","IUES.L","DTLA.L","IUHC.L",
]
# 2026-07：新增 IUHC.L — CDE_XLV pilot execution vehicle（spec v1.5 §4.1，XLV→IUHC）。
# 註：TWD=X 已移出 yfinance 清單，改由下方 update_twd_fx() 以 CBC 增量合併更新。
# 2026-07-03 切換：yfinance TWD=X 序列有系統性壞 tick（round-trip 型 84 天 +
# 2011-10 除誤值災難級錯誤），來源換為央行公布之台北外匯經紀收盤匯率。
# 見 2026-07-03_fx_source_switch_cbc_notes.md。

# yfinance 需要交易所後綴才能下載，但存檔時統一格式，讓 PnL engine 找得到
yf_rename = {
    "CSNDX.SW":  "CSNDX",
    "IGLN.L":    "IGLN",
    "IUES.L":    "IUES",
    "DTLA.L":    "DTLA",
    "IUHC.L":    "IUHC",
    "00679B.TWO": "00679B.TW",
}

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
    # 興櫃／上市資料語意防呆：台股標的（folder=="tw"）若查得到官方上市日期，
    # 把上市日之前的列砍掉（yfinance 對興櫃轉上市股回傳的是無接縫連續序列，
    # 上市前後價格形成機制不同，不能被當成同一段「上市後歷史」）。
    # 非台股（US/UCITS）listing_guard 一律回 None，不受影響。見 listing_guard.py。
    if folder == "tw":
        before = len(df)
        df, listing_date = listing_guard.truncate_pre_listing(df, ticker)
        if listing_date:
            removed = before - len(df)
            if removed > 0:
                print(f"  ↳ 上市日 {listing_date}，砍除上市前 {removed} 筆興櫃期間資料")
    save_name = yf_rename.get(ticker, ticker)
    df.to_csv(f"{folder}/{save_name}.csv")
    print("Saved:", folder, save_name)


# ============================================================
# TWD=X：CBC（台北外匯經紀收盤匯率）增量合併
# ============================================================
# 模式：讀現有檔 → 抓 CBC 最近幾頁 → CBC 值覆蓋同日、續接新日 → 寫回。
# 歷史永遠保留，不做全量重抓（全量重抓會把已回補的 CBC 歷史洗回 yfinance）。
# CBC 全部失敗時：以 yfinance 補「檔案裡還沒有的近日」作臨時值並大聲警告；
# 臨時值在下次 CBC 成功時會被覆蓋（合併規則 = CBC 值一律優先）。

import re
import time
import urllib.request

FX_PATH = "us/TWD=X.csv"
FX_HEADER = ["Price,AdjClose", "Ticker,TWD=X", "Date,"]
CBC_PAGE_URL = "https://www.cbc.gov.tw/tw/lp-645-1-{page}-20.html"
CBC_MAX_PAGES = 10          # 增量最多回看 10 頁 = 200 個交易日
CBC_UA = "Mozilla/5.0"

def _cbc_fetch_page(page: int) -> dict:
    """抓 CBC 一頁，回傳 {date: rate}。含 retry。日期正規化為 YYYY-MM-DD
    （注意：CBC 舊頁日期不補零，如 2019/9/5，正則必須用 \\d{1,2}）。"""
    url = CBC_PAGE_URL.format(page=page)
    html = ""
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": CBC_UA})
            with urllib.request.urlopen(req, timeout=40) as r:
                html = r.read().decode("utf-8", errors="replace")
            if len(html) > 10000:
                break
        except Exception:
            pass
        time.sleep(2 + attempt)
    out = {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        cells = [re.sub(r"<[^>]+>", "", c).strip()
                 for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
        if len(cells) == 2 and re.match(r"^\d{4}/\d{1,2}/\d{1,2}$", cells[0]):
            y, m, d = cells[0].split("/")
            try:
                out[f"{y}-{int(m):02d}-{int(d):02d}"] = float(cells[1])
            except ValueError:
                pass
    return out

def _fx_read_existing() -> dict:
    if not os.path.exists(FX_PATH):
        return {}
    rows = {}
    with open(FX_PATH) as f:
        for i, line in enumerate(f.read().splitlines()):
            if i < 3:
                continue
            parts = line.split(",")
            if len(parts) >= 2 and parts[0] and parts[1]:
                rows[parts[0]] = parts[1]
    return rows

def _fx_write(rows: dict) -> None:
    with open(FX_PATH, "w") as f:
        f.write("\n".join(FX_HEADER) + "\n")
        for d in sorted(rows):
            f.write(f"{d},{rows[d]}\n")

def update_twd_fx():
    print("Updating TWD=X from CBC (台北外匯經紀收盤匯率)")
    rows = _fx_read_existing()
    last_date = max(rows) if rows else "1900-01-01"

    cbc = {}
    for page in range(1, CBC_MAX_PAGES + 1):
        got = _cbc_fetch_page(page)
        if not got:
            break
        cbc.update(got)
        if min(got) <= last_date:   # 已覆蓋到檔案尾端，增量足夠
            break
        time.sleep(1)

    if cbc:
        for d, v in cbc.items():
            rows[d] = str(v)        # CBC 一律優先：覆蓋同日（含先前臨時值）、續接新日
        _fx_write(rows)
        print(f"Saved: {FX_PATH}  (CBC merged {len(cbc)} 筆, {min(cbc)} → {max(cbc)})")
        return

    # ---- CBC 全部失敗：yfinance 臨時補位（只補檔案裡沒有的近日）----
    print("⚠⚠ CBC 抓取全部失敗，fallback yfinance（臨時值，下次 CBC 成功會自動覆蓋）")
    try:
        df = yf.download("TWD=X", period="1mo", auto_adjust=True)
        added = 0
        for idx, row in df.iterrows():
            d = idx.strftime("%Y-%m-%d")
            if d not in rows and not pd.isna(row["Close"]):
                rows[d] = str(float(row["Close"]))
                added += 1
        if added:
            _fx_write(rows)
        print(f"⚠ yfinance 臨時補 {added} 筆；來源品質未驗證，跑報告前建議先跑 price_patch 的 FX cross-check")
    except Exception as e:
        print(f"⚠ yfinance fallback 也失敗：{e}；TWD=X 本次未更新")


for t in tickers_tw:
    download_adj_close(t, "tw")
for t in tickers_us:
    download_adj_close(t, "us")
update_twd_fx()
