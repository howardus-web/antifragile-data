#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lab_kit / build_universes.py
=============================
重建 2026-07 CDE 研究線用的三個實驗宇宙。一次 setup、隨叫隨用。

三個宇宙：
  A. universe_real()        現役宇宙：真實六味 QQQ/TLT/XLE/GLD/BTAL/DBMF（日頻，~2019-05 起，
                            受 DBMF 真實上市日限制）。資料全部來自 production repo，無代理品。
  B. universe_13y()         13.7 年沙盒：QQQ/TLT/XLE/GLD/BTAL/AQMIX（日頻，2012-10 起，
                            AQMIX 代 DBMF，其餘真實）。窗口瓶頸 = AQMIX 需要 252 天 IV 暖機。
  C. universe_26y_monthly() 26 年月頻 proxy 宇宙：QQQ 真實、TLT→VUSTX、XLE 真實、GLD→CEF、
                            BTAL→BAB 延伸合成、γ→TSMOM（月報酬直接使用）。1999-04 起，
                            月頻 ensemble [3,6,12]。唯一涵蓋 2008 金融海嘯的版本（P-40）。

用法：
  1. setup：把 production repo 下載解壓成 antifragile-data-main/（跟平常一樣），
     然後跑 `python3 build_universes.py --setup --repo <repo路徑>`
     （會把 kit 的 session-added tickers 複製進 repo/us/，讓引擎 loader 直接可用）
  2. 之後在自己的腳本裡 `from build_universes import *`，呼叫三個 universe 函式。
  3. `python3 build_universes.py --verify --repo <repo路徑>` 重算三個錨點，
     對 expected_metrics.json 檢查是否 bit-level 一致（窗口已 clamp，repo 後續
     新增資料不影響驗證）。

依賴：/mnt/project/antifragile_v3121_deterministic_engine_v1_2.py（檔名凍結）
      用引擎自己的 DataRepo/load_us_price_matrix 讀 repo 檔案，確保跟 production
      同一條載入路徑，數字可以 bit-identical 重現。

已知陷阱（README 有完整版）：
  - 假月底：資料尾端不完整月份的最後交易日會被誤判為月底，重要計算前先 clamp
    到最後一個真正走完的月份（用 clamp_to_complete_month）。
  - 月頻取樣後算 MA3/MA6 ≠ 日頻 MA60/120 月底讀值，active frequency 差 ~6.8pp，
    production CDE 規則是後者（cde_signal_daily）。
  - TSMOM 是月報酬序列不是價格，只能用在月頻宇宙，不能轉日頻。
  - CEF 是金+銀信託不是純金；VUSTX 是基金不是 TLT 本尊。
"""
import argparse
import importlib.util
import json
import os
import sys

import numpy as np
import pandas as pd

KIT_DIR = os.path.dirname(os.path.abspath(__file__))
ENGINE_PATH = "/mnt/project/antifragile_v3121_deterministic_engine_v1_2.py"

DAILY_ENSEMBLE = [60, 120, 252]
MONTHLY_ENSEMBLE = [3, 6, 12]


# ---------------------------------------------------------------- loaders
def load_kit_ticker(name):
    """讀 kit tickers 目錄的 *_full.csv / AQMIX.csv / CEF.csv 等（yfinance 三行頭格式）。"""
    for fname in (f"{name}_full.csv", f"{name}.csv"):
        path = os.path.join(KIT_DIR, "data", "tickers", fname)
        if os.path.exists(path):
            df = pd.read_csv(path, skiprows=3, names=["Date", "Close"])
            df["Date"] = pd.to_datetime(df["Date"])
            df["Close"] = df["Close"].astype(float)
            return df.set_index("Date")["Close"].sort_index()
    raise FileNotFoundError(f"kit tickers 找不到 {name}")


def load_btal_extended():
    path = os.path.join(KIT_DIR, "data", "derived", "btal_extended.csv")
    return pd.read_csv(path, index_col=0, parse_dates=True)["Close"]


def load_tsmom_monthly():
    path = os.path.join(KIT_DIR, "data", "aqr", "tsmom_monthly.csv")
    s = pd.read_csv(path, parse_dates=["date"]).dropna(subset=["tsmom"]).set_index("date")["tsmom"]
    s.index = s.index.to_period("M").to_timestamp("M")
    return s.astype(float)


def load_engine(repo_path):
    spec = importlib.util.spec_from_file_location("af_mod", ENGINE_PATH)
    af = importlib.util.module_from_spec(spec)
    sys.modules["af_mod"] = af
    spec.loader.exec_module(af)
    return af, af.DataRepo(repo_path)


# ---------------------------------------------------------------- helpers
def month_end_mask(idx):
    grp = pd.Series(idx, index=idx).groupby([idx.year, idx.month]).transform("max")
    return [d for i, d in enumerate(idx) if grp.iloc[i] == d]


def clamp_to_complete_month(series_or_df, last_complete_day=None):
    """砍掉尾端不完整月份。last_complete_day 給定時直接 clamp；沒給時自動判斷：
    若最後一個月的資料天數 < 15 個交易日，整個月砍掉（保守估計）。"""
    idx = series_or_df.index
    if last_complete_day is not None:
        return series_or_df.loc[:pd.Timestamp(last_complete_day)]
    last_period = idx[-1].to_period("M")
    n_days_last = (idx.to_period("M") == last_period).sum()
    if n_days_last < 15:
        cutoff = (last_period.to_timestamp() - pd.Timedelta(days=1))
        return series_or_df.loc[:cutoff]
    return series_or_df


def iv_weights_daily(returns_df, pos, assets, ensemble=DAILY_ENSEMBLE):
    acc = np.zeros(len(assets))
    for lb in ensemble:
        v = returns_df[assets].iloc[max(0, pos - lb):pos].std().values
        w = 1.0 / np.maximum(v, 1e-12)
        acc += w / w.sum()
    return acc / acc.sum()


def iv_weights_monthly(returns_df, pos, assets, ensemble=MONTHLY_ENSEMBLE):
    return iv_weights_daily(returns_df, pos, assets, ensemble)


def metrics_from_returns(r, ann):
    nav = (1 + r).cumprod()
    er = float(r.mean() * ann)
    vol = float(r.std(ddof=0) * np.sqrt(ann))
    peak = nav.cummax()
    dd = nav / peak - 1
    ny = len(r) / ann
    cagr = float(nav.iloc[-1] ** (1 / ny) - 1)
    return {"cagr": cagr, "ann_vol": vol, "sharpe": er / vol,
            "max_dd": float(dd.min()), "max_dd_date": str(dd.idxmin().date()),
            "calmar": cagr / abs(float(dd.min()))}


# ---------------------------------------------------------------- signals
def cde_signal_daily(numer="XLV", denom="SPY", short=60, long=120):
    """production CDE 規則：日頻 ratio、日頻 MA60/MA120、月底讀當天布林值。
    回傳日頻布林 Series（月底流程自行取月底日的值）。"""
    ratio = (load_kit_ticker(numer) / load_kit_ticker(denom)).dropna()
    return (ratio.rolling(short).mean() < ratio.rolling(long).mean())


# ---------------------------------------------------------------- universes
def universe_real(repo_path, last_complete_day=None):
    """現役宇宙：真實六味（含 DBMF）。回傳 (prices6, returns6, month_ends)。"""
    af, repo = load_engine(repo_path)
    U = ["QQQ", "TLT", "XLE", "GLD", "BTAL", "DBMF"]
    prices = af.load_us_price_matrix(repo, U)[U].dropna(how="any")
    prices = clamp_to_complete_month(prices, last_complete_day)
    returns = prices.pct_change().dropna()
    return prices, returns, month_end_mask(returns.index)


def universe_13y(repo_path, last_complete_day=None):
    """13.7 年沙盒：AQMIX 代 DBMF。需先 --setup 把 AQMIX.csv 放進 repo/us/。"""
    af, repo = load_engine(repo_path)
    U = ["QQQ", "TLT", "XLE", "GLD", "BTAL", "AQMIX"]
    prices = af.load_us_price_matrix(repo, U)[U].dropna(how="any")
    prices = clamp_to_complete_month(prices, last_complete_day)
    returns = prices.pct_change().dropna()
    return prices, returns, month_end_mask(returns.index)


def universe_26y_monthly(last_end=None):
    """26 年月頻 proxy 宇宙。回傳 returns_m（月報酬 DataFrame，欄位沿用六味名稱：
    TLT=VUSTX、GLD=CEF、BTAL=BAB延伸、AQMIX=TSMOM）。"""
    qqq_m = load_kit_ticker("QQQ").resample("ME").last()
    xle_m = load_kit_ticker("XLE").resample("ME").last()
    vustx_m = load_kit_ticker("VUSTX").resample("ME").last()
    cef_m = load_kit_ticker("CEF").resample("ME").last()
    btal_m = load_btal_extended().resample("ME").last()
    tsmom = load_tsmom_monthly()
    prices_m = pd.DataFrame({"QQQ": qqq_m, "TLT": vustx_m, "XLE": xle_m,
                             "GLD": cef_m, "BTAL": btal_m})
    returns_m = prices_m.pct_change()
    returns_m["AQMIX"] = tsmom
    returns_m = returns_m.dropna(how="any")
    if last_end is not None:
        returns_m = returns_m.loc[:pd.Timestamp(last_end)]
    return returns_m[["QQQ", "TLT", "XLE", "GLD", "BTAL", "AQMIX"]]


# ---------------------------------------------------------------- setup / verify
def do_setup(repo_path):
    import shutil
    src = os.path.join(KIT_DIR, "data", "tickers")
    dst = os.path.join(repo_path, "us")
    copied = []
    for f in sorted(os.listdir(src)):
        if f.endswith(".csv"):
            shutil.copy2(os.path.join(src, f), os.path.join(dst, f))
            copied.append(f)
    print(f"✓ setup：{len(copied)} 個 kit ticker 檔已複製進 {dst}")


def _run_iv_walkforward(returns, month_ends, universe, ensemble, ann,
                        conditional_asset=None, signal=None):
    """通用 walk-forward：無 conditional_asset 時為純六味 IV base；
    有時為 CDE 條件式（asset 依 signal 月底讀值進出 candidate set）。"""
    idx = returns.index
    sched = {}
    n_active = 0
    for d in month_ends:
        pos = idx.get_loc(d)
        if pos < max(ensemble):
            continue
        assets = list(universe)
        if conditional_asset is not None:
            if d not in signal.index or pd.isna(signal.loc[d]):
                continue
            if bool(signal.loc[d]):
                assets.append(conditional_asset)
                n_active += 1
        w = iv_weights_daily(returns, pos, assets, ensemble)
        full = {n: 0.0 for n in list(universe) + ([conditional_asset] if conditional_asset else [])}
        for n, wi in zip(assets, w):
            full[n] = wi
        sched[d] = full
    sig_dates = sorted(sched)
    all_assets = list(universe) + ([conditional_asset] if conditional_asset else [])
    daily = {}
    for i, d in enumerate(idx):
        prevs = [s for s in sig_dates if s < d]
        if not prevs:
            continue
        w = sched[prevs[-1]]
        daily[d] = float(returns.iloc[i][all_assets].values @ np.array([w[n] for n in all_assets]))
    r = pd.Series(daily)
    m = metrics_from_returns(r, ann)
    m["n_signal_dates"] = len(sig_dates)
    m["n_active"] = n_active
    return m


def do_verify(repo_path):
    with open(os.path.join(KIT_DIR, "expected_metrics.json")) as f:
        exp = json.load(f)
    tol = 1e-6
    ok_all = True

    # A. 13.7y baseline（clamp 到本次 session 的資料尾端 2026-07-01）
    _, ret13, me13 = universe_13y(repo_path, last_complete_day="2026-07-01")
    got = _run_iv_walkforward(ret13, me13, ["QQQ", "TLT", "XLE", "GLD", "BTAL", "AQMIX"],
                              DAILY_ENSEMBLE, 252)
    e = exp["universe_13y_iv_baseline"]
    ok = abs(got["sharpe"] - e["sharpe"]) < tol and abs(got["max_dd"] - e["max_dd"]) < tol
    print(f"[{'PASS' if ok else 'FAIL'}] 13.7y baseline  Sharpe {got['sharpe']:.6f} (exp {e['sharpe']:.6f})  MaxDD {got['max_dd']:.6f}")
    ok_all &= ok

    # B. 26y monthly baseline（clamp 到 2026-05-31）
    ret26 = universe_26y_monthly(last_end="2026-05-31")
    got = _run_iv_walkforward(ret26, list(ret26.index), list(ret26.columns),
                              MONTHLY_ENSEMBLE, 12)
    e = exp["universe_26y_monthly_iv_baseline"]
    ok = abs(got["sharpe"] - e["sharpe"]) < tol and abs(got["max_dd"] - e["max_dd"]) < tol
    print(f"[{'PASS' if ok else 'FAIL'}] 26y monthly     Sharpe {got['sharpe']:.6f} (exp {e['sharpe']:.6f})  MaxDD {got['max_dd']:.6f}")
    ok_all &= ok

    # C. 現役宇宙 + CDE_XLV（daily MA60/120，clamp 到 2026-06-30，production reference）
    prices_r, ret_r, _ = universe_real(repo_path, last_complete_day="2026-06-30")
    xlv = load_kit_ticker("XLV").loc[:pd.Timestamp("2026-06-30")]
    prices7 = prices_r.copy()
    prices7["XLV"] = xlv.reindex(prices_r.index)
    prices7 = prices7.dropna(how="any")
    ret7 = prices7.pct_change().dropna()
    sig = cde_signal_daily().loc[:pd.Timestamp("2026-06-30")]
    got = _run_iv_walkforward(ret7, month_end_mask(ret7.index),
                              ["QQQ", "TLT", "XLE", "GLD", "BTAL", "DBMF"],
                              DAILY_ENSEMBLE, 252, conditional_asset="XLV", signal=sig)
    e = exp["universe_real_cde_xlv_reference"]
    ok = (abs(got["sharpe"] - e["Sharpe"]) < 1e-3 and abs(got["max_dd"] - e["MaxDD"]) < tol
          and got["n_active"] == e["active_months"])
    print(f"[{'PASS' if ok else 'FAIL'}] real+CDE_XLV    Sharpe {got['sharpe']:.4f} (exp {e['Sharpe']:.4f})  MaxDD {got['max_dd']:.6f}  active {got['n_active']}/{got['n_signal_dates']} (exp {e['active_months']}/{e['total_months']})")
    ok_all &= ok

    print("\n" + ("✓ 全部錨點通過，環境與 2026-07-06 session 一致" if ok_all else "✗ 有錨點未通過，環境或資料與原 session 不一致，檢查 README 陷阱清單"))
    return ok_all


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="/home/claude/antifragile-data-main")
    ap.add_argument("--setup", action="store_true")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()
    if args.setup:
        do_setup(args.repo)
    if args.verify:
        do_verify(args.repo)
    if not args.setup and not args.verify:
        ap.print_help()
