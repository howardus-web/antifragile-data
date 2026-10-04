"""LAB_LONG_HISTORY — isolated monthly RS6 adapter for the MF 26Y crisis extension WTC.
Does NOT import or modify research_harness / canonical RS-Long machinery. research_stats.hac_ols (a pure
statistics primitive) is reused only so the HAC convention matches A1/A1b."""
import sys, re, json, hashlib
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, "/home/claude/corpus")
import research_stats as _stats            # pure functions; no canonical machine code is touched

LABEL = "LAB_LONG_HISTORY"
REPO = Path("/home/claude/corpus/repo"); KIT = REPO / "lab/lab_kit_2026-07-07"
FROZ = Path("/home/claude/lab26/frozen_btal_proxy"); OUT = Path("/home/claude/lab26/out"); OUT.mkdir(exist_ok=True)
ASSETS = ["QQQ", "TLT", "GLD", "XLE", "BTAL", "MF"]; NONMF = ASSETS[:5]
SLOT = np.array([36.8, 23.9, 15.6, 10.1, 6.6, 4.3], dtype=float); SLOT = SLOT / SLOT.sum()   # canonical literal
C_PROD_A1 = 0.13255295578008836
HAC_LAG = 3
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()

def canonical_slot_from_corpus():
    src = Path("/home/claude/corpus/antifragile_v3121_deterministic_engine_v1_2.py").read_text()
    m = re.search(r"^SLOT_WEIGHTS = np\.array\(\[([^\]]+)\]", src, re.M)
    v = np.array([float(x) for x in m.group(1).split(",")]); return v / v.sum()

# ------------------------------------------------------------------ inputs (kit conventions: month-end last price -> pct_change)
def _kit_px(name):
    for f in (f"{name}_full.csv", f"{name}.csv"):
        p = KIT / "data/tickers" / f
        if p.exists():
            df = pd.read_csv(p, skiprows=3, names=["Date", "Close"]); df["Date"] = pd.to_datetime(df["Date"])
            return df.set_index("Date")["Close"].astype(float).sort_index(), p
    raise FileNotFoundError(name)

def _mret(px):
    m = px.resample("ME").last().pct_change(); m.index = m.index.to_period("M"); return m.dropna()

def load_inputs():
    ident = {}; cols = {}
    for role, tk in (("QQQ", "QQQ"), ("TLT", "VUSTX"), ("GLD", "CEF"), ("XLE", "XLE")):
        px, p = _kit_px(tk); cols[role] = _mret(px)
        ident[role] = {"source_series": tk, "file": str(p.relative_to(REPO)), "sha256": sha(p),
                       "first_raw_date": str(px.index[0].date()), "last_raw_date": str(px.index[-1].date())}
    p = KIT / "data/aqr/tsmom_monthly.csv"
    ts = pd.read_csv(p, parse_dates=["date"]).dropna(subset=["tsmom"]).set_index("date")["tsmom"].astype(float)
    ts.index = ts.index.to_period("M"); cols["MF"] = ts
    ident["MF"] = {"source_series": "AQR TSMOM (all-asset aggregate, monthly returns)", "file": str(p.relative_to(REPO)), "sha256": sha(p),
                   "first_raw_date": str(ts.index[0]), "last_raw_date": str(ts.index[-1]),
                   "identity": "managed-futures / time-series-momentum CATEGORY proxy; NOT a DBMF return reconstruction"}
    base = pd.concat(cols, axis=1).sort_index()
    # BTAL legs
    pure = pd.read_csv(FROZ / "btal_proxy_monthly_returns.csv", index_col=0); pure.index = pd.PeriodIndex(pure.index, freq="M"); pure = pure.sort_index()
    ext = {}
    for nm in ("F1", "S3", "MIX2"):
        e = pd.read_csv(FROZ / f"btal_extended_{nm}_monthly_returns.csv", index_col=0); e.index = pd.PeriodIndex(e.index, freq="M"); ext[nm] = e.sort_index()
    btx = pd.read_csv(KIT / "data/derived/btal_extended.csv", index_col=0, parse_dates=True)["Close"]
    bab_old = _mret(btx)
    legs = {
        "F1": ext["F1"]["ret"], "S3": ext["S3"]["ret"], "HYBRID": ext["MIX2"]["ret"], "BAB": bab_old,     # chain-linked: proxy < 2011-10, real BTAL >= 2011-10
        "TRUE_overlap": pure["BTAL"], "F1_pure": pure["F1"], "S3_pure": pure["S3"],                          # pure series for the overlap calibration
    }
    ident["BTAL_legs"] = {
        "F1": {"def": "rf + a + 0.7176*BAB - 0.6165*(Mkt-RF) before 2011-10; real BTAL from 2011-10 (chain-linked, kit convention)", "file": "btal_extended_F1_monthly_returns.csv", "sha256": sha(FROZ / "btal_extended_F1_monthly_returns.csv"), "role": "PRIMARY (fit-informed)"},
        "S3": {"def": "rf + mean_{ME3,ME4,ME5}(LoBeta - HiBeta), French size x beta, EW cells, before 2011-10; real BTAL from 2011-10", "file": "btal_extended_S3_monthly_returns.csv", "sha256": sha(FROZ / "btal_extended_S3_monthly_returns.csv"), "role": "PRIMARY (construction-informed)"},
        "HYBRID": {"def": "0.5*F1 + 0.5*S3 before 2011-10; real BTAL after", "file": "btal_extended_MIX2_monthly_returns.csv", "sha256": sha(FROZ / "btal_extended_MIX2_monthly_returns.csv"), "role": "descriptive midpoint only"},
        "BAB": {"def": "lab kit btal_extended.csv: AQR BAB daily chained back from real BTAL start (2011-09-13); real BTAL after", "file": "lab/lab_kit_2026-07-07/data/derived/btal_extended.csv", "sha256": sha(KIT / "data/derived/btal_extended.csv"), "role": "NEGATIVE CONTROL only (old 26Y proxy)"},
        "frozen_package": {"file": "BTAL_LONG_PROXY_LAB_2026-10-04.zip", "sha256": sha("/mnt/user-data/outputs/BTAL_LONG_PROXY_LAB_2026-10-04.zip")},
        "splice_month": "2011-10 (first full month of real BTAL)",
    }
    return base, legs, ident

def universe(base, leg, end=None, start=None):
    R = base.copy(); R["BTAL"] = leg; R = R[ASSETS].dropna(how="any")
    if start is not None: R = R.loc[start:]
    if end is not None: R = R.loc[:end]
    # contiguity: no missing months inside the window
    assert len(R) == (R.index[-1] - R.index[0]).n + 1, "missing month inside window"
    return R

# ------------------------------------------------------------------ RS6 monthly LAB rule
def rs_signal(R):
    """RS_t = (6M log return + 12M log return)/2 using monthly returns through month-end t (no look-ahead)."""
    lr = np.log1p(R); return ((lr.rolling(6).sum() + lr.rolling(12).sum()) / 2.0).dropna()

def run_ablation(R, c_override=None):
    S = rs_signal(R)
    rk = S.rank(axis=1, ascending=False, method="first").astype(int)
    assert (S.apply(lambda r: r.nunique(), axis=1) == 6).all(), "tie in RS signal"
    Wsig = rk.apply(lambda col: col.map(lambda k: SLOT[k - 1]))               # weights decided at signal month t
    hold = Wsig.index + 1                                                       # held during month t+1
    keep = hold.isin(R.index)
    WB = Wsig[keep].copy(); sig_idx = WB.index; WB.index = hold[keep]           # index = holding month
    RK = rk[keep].copy(); RK.index = WB.index
    Rh = R.loc[WB.index]
    c = float(WB["MF"].mean()) if c_override is None else float(c_override)     # frozen BEFORE TEST is built
    WT = WB[NONMF].mul((1.0 - c) / (1.0 - WB["MF"]), axis=0); WT["MF"] = c; WT = WT[ASSETS]
    rb = (WB * Rh).sum(axis=1); rt = (WT * Rh).sum(axis=1); D = rb - rt
    mf_direct = (WB["MF"] - c) * Rh["MF"]; funding = ((WB[NONMF] - WT[NONMF]) * Rh[NONMF]).sum(axis=1)
    df = pd.DataFrame({"signal_month": sig_idx.astype(str), "mf_rank": RK["MF"], "w_mf_bench": WB["MF"], "c": c, "mf_excess_w": WB["MF"] - c,
                       "r_mf": Rh["MF"], "bench_ret": rb, "test_ret": rt, "D": D, "MF_DIRECT": mf_direct, "NON_MF_FUNDING": funding})
    for a in ASSETS: df[f"wB_{a}"] = WB[a]; df[f"wT_{a}"] = WT[a]; df[f"r_{a}"] = Rh[a]; df[f"rank_{a}"] = RK[a]
    df.index.name = "holding_month"
    return df, c, S

def hac_mean(y):
    yy = pd.Series(np.asarray(y, float), index=pd.RangeIndex(len(y))); X = pd.DataFrame({"const": np.ones(len(yy))}, index=yy.index)
    r = _stats.hac_ols(yy, X, lag=HAC_LAG, add_const=False); m, se = r["params"]["const"], r["se"]["const"]
    return {"mean_m": m, "ann": 12 * m, "t": r["t"]["const"], "p": r["p"]["const"], "ci_lo_ann": 12 * (m - 1.96 * se), "ci_hi_ann": 12 * (m + 1.96 * se), "N": r["N"]}

def perf(r):
    nav = (1 + r).cumprod(); yrs = len(r) / 12.0
    return {"CAGR": float(nav.iloc[-1] ** (1 / yrs) - 1), "vol": float(r.std(ddof=0) * np.sqrt(12)),
            "sharpe": float(r.mean() / r.std(ddof=0) * np.sqrt(12)), "maxdd": float((nav / nav.cummax() - 1).min()), "terminal": float(nav.iloc[-1])}

def stage0_checks(df, c, R, S, tag):
    WB = df[[f"wB_{a}" for a in ASSETS]].values; WT = df[[f"wT_{a}" for a in ASSETS]].values
    ratio_b = WB[:, :5] / WB[:, :5].sum(axis=1, keepdims=True); ratio_t = WT[:, :5] / WT[:, :5].sum(axis=1, keepdims=True)
    # look-ahead: recompute the signal for every signal month from data truncated at that month
    la = 0.0
    for t in S.index:
        la = max(la, float(np.abs(rs_signal(R.loc[:t]).iloc[-1].values - S.loc[t].values).max()))
    sig = pd.PeriodIndex(df["signal_month"], freq="M")
    chk = {
        "universe": tag,
        "1_max_abs_weight_sum_minus_1": float(max(np.abs(WB.sum(axis=1) - 1).max(), np.abs(WT.sum(axis=1) - 1).max())),
        "2_min_weight": float(min(WB.min(), WT.min())),
        "3_max_abs_nonMF_relative_ratio_diff": float(np.abs(ratio_b - ratio_t).max()),
        "4_abs_mean_test_mf_minus_c": float(abs(df["wT_MF"].mean() - c)),
        "5_abs_mean_bench_mf_minus_c": float(abs(df["wB_MF"].mean() - c)),
        "6_lookahead_max_abs_signal_diff_truncated_vs_full": la,
        "7_holding_minus_signal_months_all_equal_1": bool(((df.index - sig).map(lambda x: x.n) == 1).all()),
        "8_max_abs_slot_minus_canonical": float(np.abs(SLOT - canonical_slot_from_corpus()).max()),
        "8b_every_bench_row_is_slot_permutation": bool(all(np.allclose(np.sort(r)[::-1], SLOT) for r in WB)),
        "9_max_abs_attribution_identity_error": float(np.abs(df["D"] - (df["MF_DIRECT"] + df["NON_MF_FUNDING"])).max()),
        "9b_max_abs_D_minus_weightdiff_dot_returns": float(np.abs(df["D"].values - ((WB - WT) * df[[f"r_{a}" for a in ASSETS]].values).sum(axis=1)).max()),
    }
    ok = (chk["1_max_abs_weight_sum_minus_1"] < 1e-12 and chk["2_min_weight"] > 0 and chk["3_max_abs_nonMF_relative_ratio_diff"] < 1e-12
          and chk["4_abs_mean_test_mf_minus_c"] < 1e-15 and chk["5_abs_mean_bench_mf_minus_c"] < 1e-15
          and chk["6_lookahead_max_abs_signal_diff_truncated_vs_full"] < 1e-12 and chk["7_holding_minus_signal_months_all_equal_1"]
          and chk["8_max_abs_slot_minus_canonical"] < 1e-15 and chk["8b_every_bench_row_is_slot_permutation"]
          and chk["9_max_abs_attribution_identity_error"] < 1e-14 and chk["9b_max_abs_D_minus_weightdiff_dot_returns"] < 1e-14)
    chk["ALL_PASS"] = bool(ok); return chk

def turnover(df, pref):
    W = df[[f"{pref}_{a}" for a in ASSETS]]; return float(W.diff().abs().sum(axis=1).iloc[1:].mean() / 2)

def summarize(df, c):
    out = {"n_months": int(len(df)), "first_holding_month": str(df.index[0]), "last_holding_month": str(df.index[-1]), "first_signal_month": df["signal_month"].iloc[0],
           "c": c, "bench": perf(df["bench_ret"]), "test": perf(df["test_ret"]), "D": hac_mean(df["D"]),
           "cagr_diff_bench_minus_test": perf(df["bench_ret"])["CAGR"] - perf(df["test_ret"])["CAGR"],
           "TE_ann": float(df["D"].std(ddof=1) * np.sqrt(12)), "corr_bench_test": float(np.corrcoef(df["bench_ret"], df["test_ret"])[0, 1]),
           "turnover_bench": turnover(df, "wB"), "turnover_test": turnover(df, "wT"),
           "MF_DIRECT_ann": float(df["MF_DIRECT"].mean() * 12), "NON_MF_FUNDING_ann": float(df["NON_MF_FUNDING"].mean() * 12),
           "hit_rate_D_gt_0": float((df["D"] > 0).mean())}
    h = len(df) // 2
    out["H1"] = {"start": str(df.index[0]), "end": str(df.index[h - 1]), **hac_mean(df["D"].iloc[:h])}
    out["H2"] = {"start": str(df.index[h]), "end": str(df.index[-1]), **hac_mean(df["D"].iloc[h:])}
    return out
