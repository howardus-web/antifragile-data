"""
listing_guard.py — TW 個股價格資料入庫防呆：把興櫃期間資料與正式上市後資料分開

背景：yfinance 對「興櫃轉上市」的股票，回傳的是興櫃＋上市接續在一起的單一連續序列，
沒有任何標記告訴你接縫在哪。任何 yfinance 全史下載（yf.download(..., start=...) 或
period="max"）若直接寫入 production CSV，會把興櫃期間的價格形成機制（撮合、流動性、
參與者跟正式上市完全不同）誤植為「上市後歷史」，汙染 12 個月 SMA 這類需要乾淨上市後
歷史的計算。

本模組只做一件事：查官方上市日期，把該日期以前的資料列砍掉。不判斷 IPO / listing age /
signal eligibility 這些語意——那是呼叫端（如母池建置的合格性判斷）的事，不是這個模組的事。
這個模組只回答一個問題：這一筆股價資料，是不是上市之後的資料。

資料來源：TWSE 官方 t187ap03_L（公司基本資料，含「上市日期」欄位），查詢失敗或標的非
台股（無 .TW 後綴 / 非上市電子業以外任何產業別）時一律回傳 None——保守預設，不砍動任何
既有資料，避免因為查詢失敗而誤傷正常標的。
"""
from __future__ import annotations

_LISTING_DATE_CACHE: dict[str, "str | None"] = {}
_T187_FETCHED = False
_T187_DATA: dict[str, str] = {}


def _load_t187() -> dict[str, str]:
    global _T187_FETCHED, _T187_DATA
    if _T187_FETCHED:
        return _T187_DATA
    try:
        import requests
        r = requests.get('https://openapi.twse.com.tw/v1/opendata/t187ap03_L', timeout=30)
        r.raise_for_status()
        for c in r.json():
            code = c.get('公司代號')
            d = c.get('上市日期')
            if code and d and len(d) == 8 and d.isdigit():
                _T187_DATA[code] = f"{d[:4]}-{d[4:6]}-{d[6:]}"
    except Exception:
        pass  # 查不到就留空 dict；get_listing_date 會回 None，代表「不砍」
    _T187_FETCHED = True
    return _T187_DATA


def get_listing_date(repo_name: str) -> "str | None":
    """repo_name 如 '7769.TW'。非 .TW 標的（美股／UCITS／FX）一律回 None，不砍。
    查不到（非上市電子業、TWSE API 失敗等）也回 None——保守預設。
    """
    if repo_name in _LISTING_DATE_CACHE:
        return _LISTING_DATE_CACHE[repo_name]
    result = None
    if repo_name.endswith(".TW"):
        code = repo_name[:-3]
        result = _load_t187().get(code)
    _LISTING_DATE_CACHE[repo_name] = result
    return result


def truncate_pre_listing(df, repo_name: str):
    """把 df（DatetimeIndex）裡早於官方上市日的列去掉。
    回傳 (處理後的 df, listing_date 或 None)。listing_date 為 None 時 df 原樣回傳。
    df 需已是 DatetimeIndex（呼叫端負責先轉好，這裡不做型別轉換）。
    """
    listing_date = get_listing_date(repo_name)
    if listing_date is None:
        return df, None
    import pandas as pd
    cutoff = pd.Timestamp(listing_date)
    return df[df.index >= cutoff], listing_date
