"""WL-006 — TW listing_guard reliability repair：確定性回歸測試（不連網）。

Run:  python -m pytest -q test_listing_guard_reliability.py

位置：
  MAS corpus（扁平）：listing_guard.py 與本檔同目錄；update.py 不在 corpus → update.py 相關測試 skip。
  data repo：tests/test_listing_guard_reliability.py，listing_guard.py／update.py 在上一層。
  也可用環境變數 LG_SRC_DIR 指定 listing_guard.py（與 update.py）所在目錄。
"""
from __future__ import annotations

import datetime as dt
import importlib
import json
import os
import runpy
import subprocess
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

HERE = Path(__file__).resolve().parent
_CANDIDATES = [Path(os.environ["LG_SRC_DIR"])] if os.environ.get("LG_SRC_DIR") else [HERE, HERE.parent]
SRC = next(p for p in _CANDIDATES if (p / "listing_guard.py").exists())
UPDATE_PY = SRC / "update.py"
WORKFLOW = SRC / ".github" / "workflows" / "update-and-build.yml"
sys.path.insert(0, str(SRC))

import listing_guard as lg  # noqa: E402

NOW = dt.datetime(2026, 10, 17, 3, 36, 7, tzinfo=dt.timezone.utc)
URL2SID = {v: k for k, v in lg.SOURCES.items()}


# ───────────────────────── synthetic official payloads ─────────────────────────

def _roster(extra=None, drop=(), n=850):
    recs = [{"公司代號": str(1000 + i), "上市日期": "19990105", "出表日期": "1151016"} for i in range(n)]
    recs += [{"公司代號": "7769", "上市日期": "20251127", "出表日期": "1151016"},
             {"公司代號": "2330", "上市日期": "19940905", "出表日期": "1151016"},
             {"公司代號": "6770", "上市日期": "20211206", "出表日期": "1151016"}]
    for code, d in (extra or {}).items():
        recs.append({"公司代號": code, "上市日期": d, "出表日期": "1151016"})
    return [r for r in recs if r["公司代號"] not in drop]


def _funds(n=60):
    return [{"基金代號": f"00{900 + i}"} for i in range(n)] + [{"基金代號": c} for c in ("0050", "0053", "0055")]


def _tpex(n=25):
    return {"data": [{"stockNo": f"00{700 + i}B"} for i in range(n)] + [{"stockNo": "00679B"}], "status": "ok"}


def payloads(roster=None, funds=None, tpex=None):
    enc = lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8")
    return {"twse_t187ap03_L": enc(_roster() if roster is None else roster),
            "twse_t187ap47_L": enc(_funds() if funds is None else funds),
            "tpex_etf_filter": enc(_tpex() if tpex is None else tpex)}


def fetcher_from(pl=None, fail=None, raw=None):
    pl = pl or payloads()

    def f(url):
        sid = URL2SID[url]
        if fail == "timeout":
            raise TimeoutError("timed out (mock)")
        if fail == "http":
            raise OSError("HTTP Error 503: Service Unavailable (mock)")
        if raw and sid in raw:
            return raw[sid]
        return pl[sid]
    return f


TRACKED = ["7769.TW", "2330.TW", "6770.TW", "0050.TW", "0053.TW", "0055.TW", "00679B.TW"]


@pytest.fixture
def cache(tmp_path):
    p = tmp_path / lg.CACHE_FILENAME
    rep = lg.refresh_cache(path=p, tracked=TRACKED, fetcher=fetcher_from(), now=NOW)
    assert rep.status == "OK_UPDATED" and rep.cache_written
    lg.configure(cache_path=p)
    yield p
    lg.configure()


def _series(start="2024-11-01", end="2026-10-16"):
    idx = pd.bdate_range(start, end)
    return pd.DataFrame({"AdjClose": [float(i) for i in range(len(idx))]}, index=idx)


# ───────────────────────── 1. official success ─────────────────────────

def test_twse_success_builds_verified_cache(cache):
    r = lg.resolve("7769.TW")
    assert r.status == lg.VERIFIED_LISTED_EQUITY and r.listing_date == "2025-11-27"
    c = lg.load_cache(cache)
    assert c["sources"]["twse_t187ap03_L"]["retrieved_at"] == "2026-10-17T03:36:07Z"
    assert len(c["sources"]["twse_t187ap03_L"]["sha256"]) == 64
    assert c["listed_equities"]["7769"] == {"d": "2025-11-27", "src": "twse_t187ap03_L", "at": "2026-10-17T03:36:07Z"}
    assert c["checksum"] == lg.compute_checksum(c)


# ───────────────────────── 2/3. network failure with / without trusted cache ─────────────────────────

@pytest.mark.parametrize("fail", ["timeout", "http"])
def test_network_failure_with_trusted_cache_keeps_cache(cache, fail):
    before = cache.read_bytes()
    rep = lg.refresh_cache(path=cache, tracked=TRACKED, fetcher=fetcher_from(fail=fail), now=NOW)
    assert rep.status == lg.SOURCE_FETCH_FAILURE and rep.cache_usable and not rep.cache_written
    assert cache.read_bytes() == before
    assert lg.ingestion_cutoff("7769.TW") == "2025-11-27"


def test_network_failure_without_cache_refuses(tmp_path):
    p = tmp_path / lg.CACHE_FILENAME
    rep = lg.refresh_cache(path=p, tracked=TRACKED, fetcher=fetcher_from(fail="timeout"), now=NOW)
    assert rep.status == lg.SOURCE_FETCH_FAILURE and not rep.cache_usable and not p.exists()
    lg.configure(cache_path=p)
    try:
        r = lg.resolve("7769.TW")
        assert r.status == lg.UNRESOLVED_IDENTITY_OR_DATE and r.reason.startswith("NO_TRUSTED_CACHE")
        with pytest.raises(lg.ListingGuardRefusal):
            lg.ingestion_cutoff("7769.TW")
        with pytest.raises(lg.ListingGuardRefusal):
            lg.truncate_pre_listing(_series(), "7769.TW")
        with pytest.raises(lg.ListingGuardError):      # v1 會靜默回 None
            lg.get_listing_date("7769.TW")
    finally:
        lg.configure()


# ───────────────────────── 4/5. malformed / partial / invalid date ─────────────────────────

@pytest.mark.parametrize("name,kw", [
    ("empty_list", {"raw": {"twse_t187ap03_L": b"[]"}}),
    ("waf_html", {"raw": {"twse_t187ap03_L": b"<html>WAF error page</html>"}}),
    ("not_a_list", {"raw": {"twse_t187ap03_L": b'{"error": "x"}'}}),
    ("partial_roster", {"pl": payloads(roster=_roster(n=100))}),
    ("tracked_record_malformed", {"pl": payloads(roster=_roster(extra={"9998": "2025/11/27"}, drop=("7769",))
                                                 + [{"公司代號": "7769", "上市日期": "2025/11/27"}])}),
    ("tracked_invalid_calendar_date", {"pl": payloads(roster=_roster(drop=("7769",))
                                                      + [{"公司代號": "7769", "上市日期": "20251332"}])}),
    ("tracked_future_date", {"pl": payloads(roster=_roster(drop=("7769",))
                                            + [{"公司代號": "7769", "上市日期": "20301127"}])}),
    ("funds_missing", {"pl": payloads(funds=[])}),
    ("tpex_shape", {"raw": {"tpex_etf_filter": b'{"rows": []}'}}),
])
def test_invalid_official_response_never_overwrites_good_cache(cache, name, kw):
    before = cache.read_bytes()
    rep = lg.refresh_cache(path=cache, tracked=TRACKED, fetcher=fetcher_from(**kw), now=NOW)
    assert rep.status == lg.INVALID_OFFICIAL_RESPONSE, (name, rep)
    assert rep.cache_usable and not rep.cache_written
    assert cache.read_bytes() == before
    assert lg.ingestion_cutoff("7769.TW") == "2025-11-27"


def test_invalid_response_without_cache_leaves_nothing(tmp_path):
    p = tmp_path / lg.CACHE_FILENAME
    rep = lg.refresh_cache(path=p, fetcher=fetcher_from(raw={"twse_t187ap03_L": b"[]"}), now=NOW)
    assert rep.status == lg.INVALID_OFFICIAL_RESPONSE and not rep.cache_usable and not p.exists()


def test_single_untracked_malformed_record_is_skipped_not_fatal(tmp_path):
    p = tmp_path / lg.CACHE_FILENAME
    pl = payloads(roster=_roster() + [{"公司代號": "4444", "上市日期": "19990231"}])
    rep = lg.refresh_cache(path=p, tracked=TRACKED, fetcher=fetcher_from(pl), now=NOW)
    assert rep.status == "OK_UPDATED"
    c = lg.load_cache(p)
    assert "4444" not in c["listed_equities"] and c["sources"]["twse_t187ap03_L"]["malformed_records"] == 1


def test_roster_shrink_is_invalid_response_and_does_not_block(cache):
    before = cache.read_bytes()
    rep = lg.refresh_cache(path=cache, tracked=TRACKED, fetcher=fetcher_from(payloads(roster=_roster(n=700))), now=NOW)
    assert rep.status == lg.INVALID_OFFICIAL_RESPONSE and rep.cache_usable
    assert rep.pending_tracked_exits == () and not rep.blocks_publication   # 殘缺回應：沿用可信 cache
    assert cache.read_bytes() == before


# ── v2.1 regression（GPT 2026-10-17 重現）：追蹤股票退出「通過完整度檢查」的官方名冊 ──

def test_tracked_exit_from_valid_roster_is_pending_and_blocks(cache):
    before = cache.read_bytes()
    rep = lg.refresh_cache(path=cache, tracked=TRACKED, fetcher=fetcher_from(payloads(roster=_roster(drop=("7769",)))), now=NOW)
    assert rep.status == lg.PENDING_TRACKED_EXIT                        # 不再是 INVALID_OFFICIAL_RESPONSE
    assert rep.pending_tracked_exits == ("7769",)
    assert rep.cache_usable and rep.blocks_publication and not rep.cache_written
    assert cache.read_bytes() == before                                 # 不覆寫 cache、不把它記成下市
    assert "7769" in rep.summary() and "待 Owner 確認" in rep.summary()
    assert lg.ingestion_cutoff("2330.TW") == "1994-09-05"               # 其他標的的 cache 仍可信


def test_tracked_exit_with_two_codes_lists_both(cache):
    rep = lg.refresh_cache(path=cache, tracked=TRACKED,
                           fetcher=fetcher_from(payloads(roster=_roster(drop=("7769", "6770")))), now=NOW)
    assert rep.status == lg.PENDING_TRACKED_EXIT and rep.pending_tracked_exits == ("6770", "7769")


def test_untracked_exit_is_not_pending(cache):
    rep = lg.refresh_cache(path=cache, tracked=["7769.TW", "2330.TW"],
                           fetcher=fetcher_from(payloads(roster=_roster(drop=("6770",)))), now=NOW)
    assert rep.status == "OK_UPDATED" and rep.retired == ("6770",) and not rep.blocks_publication


@pytest.mark.parametrize("fail", ["timeout", "http"])
def test_network_failure_does_not_block_publication(cache, fail):
    rep = lg.refresh_cache(path=cache, tracked=TRACKED, fetcher=fetcher_from(fail=fail), now=NOW)
    assert rep.status == lg.SOURCE_FETCH_FAILURE and rep.cache_usable and not rep.blocks_publication


# ───────────────────────── 6. ETF / non-applicable / identity ─────────────────────────

@pytest.mark.parametrize("t,cls", [("0050.TW", "TWSE_LISTED_FUND"), ("0053.TW", "TWSE_LISTED_FUND"),
                                   ("0055.TW", "TWSE_LISTED_FUND"), ("00679B.TW", "TPEX_ETF"),
                                   ("00679B.TWO", "TPEX_ETF")])
def test_etf_is_verified_not_applicable_and_untouched(cache, t, cls):
    r = lg.resolve(t)
    assert r.status == lg.VERIFIED_NOT_APPLICABLE and r.instrument_class == cls
    df = _series("2005-01-03")
    out, ld = lg.truncate_pre_listing(df, t)
    assert ld is None and out.equals(df)
    assert lg.get_listing_date(t) is None


def test_unknown_tw_code_is_not_automatically_not_applicable(cache):
    r = lg.resolve("1234X.TW")
    assert r.status == lg.UNRESOLVED_IDENTITY_OR_DATE
    with pytest.raises(lg.ListingGuardRefusal):
        lg.ingestion_cutoff("1234X.TW")
    # 但 v1 介面對「不在已驗證現行名冊」維持 None（已下市歷史代號；tw_pit_roster 凍結契約）
    assert lg.get_listing_date("1234X.TW") is None


def test_ambiguous_identity_refused(tmp_path):
    p = tmp_path / lg.CACHE_FILENAME
    lg.refresh_cache(path=p, fetcher=fetcher_from(), now=NOW)
    c = lg.load_cache(p)
    c["not_applicable"]["2330"] = {"class": "TWSE_LISTED_FUND", "src": "twse_t187ap47_L", "at": "x"}
    c["checksum"] = lg.compute_checksum(c)
    lg.write_cache(c, p)
    try:
        assert lg.resolve("2330.TW").reason.startswith("AMBIGUOUS_IDENTITY")
        with pytest.raises(lg.ListingGuardError):
            lg.get_listing_date("2330.TW")
    finally:
        lg.configure()


@pytest.mark.parametrize("t", ["QQQ", "DBMF", "DBMFE.PA", "QBTL.TO", "IGLN.L", "CSNDX.SW", "EURUSD=X", "CADUSD=X", "TWD=X"])
def test_us_ucits_fx_symbols_outside_guard(t, tmp_path):
    lg.configure(cache_path=tmp_path / "absent.json")       # 即使沒有 cache 也不受影響
    try:
        r = lg.resolve(t)
        assert r.status == lg.VERIFIED_NOT_APPLICABLE and r.instrument_class == "NON_TW_MARKET"
        assert lg.ingestion_cutoff(t) is None
    finally:
        lg.configure()


# ───────────────────────── 7. newly listed security ─────────────────────────

def test_new_listing_unresolved_until_official_admission(cache):
    with pytest.raises(lg.ListingGuardRefusal):
        lg.ingestion_cutoff("9999.TW")
    rep = lg.refresh_cache(path=cache, tracked=TRACKED + ["9999.TW"], fetcher=fetcher_from(fail="http"), now=NOW)
    assert rep.status == lg.SOURCE_FETCH_FAILURE
    with pytest.raises(lg.ListingGuardRefusal):
        lg.ingestion_cutoff("9999.TW")
    rep = lg.refresh_cache(path=cache, tracked=TRACKED + ["9999.TW"],
                           fetcher=fetcher_from(payloads(roster=_roster(extra={"9999": "20261015"}))), now=NOW)
    assert rep.status == "OK_UPDATED" and rep.admitted == ("9999",)
    assert lg.ingestion_cutoff("9999.TW") == "2026-10-15"


# ───────────────────────── 8/9. truncation ─────────────────────────

def test_known_equity_with_pre_listing_history_truncated(cache):
    out, ld = lg.truncate_pre_listing(_series("2024-11-01"), "7769.TW")
    assert ld == "2025-11-27" and out.index.min() == pd.Timestamp("2025-11-27") and len(out) < len(_series())


def test_known_equity_clean_post_listing_unchanged(cache):
    df = _series("2000-01-03")
    out, ld = lg.truncate_pre_listing(df, "2330.TW")
    assert ld == "1994-09-05" and out.equals(df)


def test_v2_truncation_equals_v1_successful_fetch_semantics(cache):
    """v1 在 TWSE 抓取成功時：上市日前砍掉。v2 對同一個官方日期必須給出逐列相同的結果。"""
    df = _series("2020-06-01")
    v1 = df[df.index >= pd.Timestamp("2021-12-06")]
    out, _ = lg.truncate_pre_listing(df, "6770.TW")
    assert out.equals(v1)


# ───────────────────────── 10. determinism ─────────────────────────

def test_same_input_identical_cache_and_results(tmp_path):
    a, b = tmp_path / "a.json", tmp_path / "b.json"
    lg.refresh_cache(path=a, tracked=TRACKED, fetcher=fetcher_from(), now=NOW)
    lg.refresh_cache(path=b, tracked=TRACKED, fetcher=fetcher_from(), now=NOW)
    assert a.read_bytes() == b.read_bytes()
    lg.configure(cache_path=a)
    try:
        r1 = [lg.resolve(t) for t in TRACKED]
        lg.configure(cache_path=b)
        r2 = [lg.resolve(t) for t in TRACKED]
        assert [x.__dict__ | {"cache_checksum": None} for x in r1] == [x.__dict__ | {"cache_checksum": None} for x in r2]
    finally:
        lg.configure()


def test_unchanged_official_roster_does_not_rewrite_cache(cache):
    before = cache.read_bytes()
    later = NOW + dt.timedelta(days=1)
    rep = lg.refresh_cache(path=cache, tracked=TRACKED, fetcher=fetcher_from(), now=later)
    assert rep.status == "OK_UNCHANGED" and not rep.cache_written and cache.read_bytes() == before


# ───────────────────────── 11. cache integrity / conflicts ─────────────────────────

def test_tampered_cache_is_untrusted(cache):
    c = json.loads(cache.read_text(encoding="utf-8"))
    c["listed_equities"]["7769"]["d"] = "2024-11-01"           # 手改，未重算 checksum
    cache.write_text(json.dumps(c, ensure_ascii=False), encoding="utf-8")
    lg.configure(cache_path=cache)
    r = lg.resolve("7769.TW")
    assert r.status == lg.UNRESOLVED_IDENTITY_OR_DATE and "checksum" in r.reason
    with pytest.raises(lg.ListingGuardRefusal):
        lg.ingestion_cutoff("7769.TW")


def test_conflicting_listing_date_flagged_refused_then_owner_accepts(cache):
    rep = lg.refresh_cache(path=cache, tracked=TRACKED,
                           fetcher=fetcher_from(payloads(roster=_roster(drop=("7769",)) + [
                               {"公司代號": "7769", "上市日期": "20251201"}])), now=NOW)
    assert rep.status == "OK_UPDATED" and rep.new_conflicts == ("7769",)
    c = lg.load_cache(cache)
    assert c["listed_equities"]["7769"]["d"] == "2025-11-27"              # 不靜默覆寫
    assert c["conflicts"]["7769"]["official"] == "2025-12-01"
    with pytest.raises(lg.ListingGuardRefusal):
        lg.ingestion_cutoff("7769.TW")
    with pytest.raises(lg.ListingGuardError):
        lg.get_listing_date("7769.TW")
    lg.accept_official("7769", cache)
    assert lg.ingestion_cutoff("7769.TW") == "2025-12-01"
    assert lg.load_cache(cache)["listed_equities"]["7769"]["owner_accepted_conflict"]["cached"] == "2025-11-27"


def test_retired_code_returns_none_like_v1(cache):
    rep = lg.refresh_cache(path=cache, tracked=["7769.TW"],
                           fetcher=fetcher_from(payloads(roster=_roster(drop=("6770",)))), now=NOW)
    assert rep.status == "OK_UPDATED" and rep.retired == ("6770",)
    assert lg.get_listing_date("6770.TW") is None                        # 已下市：與 v1 成功抓取時同義
    with pytest.raises(lg.ListingGuardRefusal):                          # 但價格入庫不放行
        lg.ingestion_cutoff("6770.TW")


# ───────────────────────── 12–14. update.py integration ─────────────────────────

needs_update = pytest.mark.skipif(not UPDATE_PY.exists(), reason="update.py 不在此目錄（MAS corpus）")


def _fake_yf(series):
    m = types.ModuleType("yfinance")

    def download(ticker, *a, **k):
        idx = series.index
        return pd.DataFrame({"Close": series["AdjClose"].values}, index=idx)
    m.download = download
    return m


def _import_update(monkeypatch, workdir, cache_path, series):
    monkeypatch.chdir(workdir)
    monkeypatch.setitem(sys.modules, "yfinance", _fake_yf(series))
    sys.modules.pop("update", None)
    lg.configure(cache_path=cache_path)
    spec = importlib.util.spec_from_file_location("update", UPDATE_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@needs_update
def test_update_refusal_leaves_existing_csv_untouched(cache, tmp_path, monkeypatch):
    wd = tmp_path / "repo"
    (wd / "tw").mkdir(parents=True)
    existing = wd / "tw" / "9999.TW.csv"
    existing.write_text("Price,AdjClose\nTicker,9999.TW\nDate,\n2026-10-15,10.0\n")
    before = existing.read_bytes()
    upd = _import_update(monkeypatch, wd, cache, _series("2024-01-02"))
    upd.download_adj_close("9999.TW", "tw")
    assert existing.read_bytes() == before
    assert upd.LISTING_REFUSALS and upd.LISTING_REFUSALS[0][0] == "9999.TW"
    upd.download_adj_close("7769.TW", "tw")                                # 已驗證標的照常寫、照常砍
    csv = pd.read_csv(wd / "tw" / "7769.TW.csv", index_col=0)
    dates = [d for d in csv.index if isinstance(d, str) and d[:2] == "20"]
    assert min(dates) == "2025-11-27"
    lg.configure()


@needs_update
def test_update_us_ucits_paths_unchanged_vs_original(cache, tmp_path, monkeypatch):
    """美股／UCITS／FX 路徑：新 update.py 寫出的 us/*.csv 與原版逐位元組相同。"""
    orig = os.environ.get("LG_ORIG_UPDATE")
    if not orig:
        pytest.skip("LG_ORIG_UPDATE 未設定（原版 update.py 路徑）")
    s = _series("2026-09-01")
    outs = []
    for path in (Path(orig), UPDATE_PY):
        wd = tmp_path / path.parent.name / "w"
        wd.mkdir(parents=True)
        monkeypatch.chdir(wd)
        monkeypatch.setitem(sys.modules, "yfinance", _fake_yf(s))
        sys.modules.pop("update", None)
        spec = importlib.util.spec_from_file_location("update", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for t in ("DBMFE.PA", "QBTL.TO", "QQQ", "IGLN.L"):
            mod.download_adj_close(t, "us")
        outs.append({f.name: f.read_bytes() for f in sorted((wd / "us").glob("*.csv"))})
    assert outs[0] == outs[1] and set(outs[0]) == {"DBMFE.csv", "QBTL.csv", "QQQ.csv", "IGLN.csv"}
    lg.configure()


@needs_update
def test_update_fx_contract_code_identical_to_original():
    orig = os.environ.get("LG_ORIG_UPDATE")
    if not orig:
        pytest.skip("LG_ORIG_UPDATE 未設定")
    import ast
    def defs(p):
        t = ast.parse(Path(p).read_text(encoding="utf-8"))
        keep = {}
        for n in t.body:
            if isinstance(n, ast.FunctionDef) and n.name != "download_adj_close":
                keep[n.name] = ast.dump(n)
            if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and n.targets[0].id in (
                    "tickers_tw", "tickers_us", "yf_rename", "FX_CONTRACT", "FX_PROVENANCE_PATH",
                    "FX_CONTRACT_HISTORY_START", "FX_OFFICIAL_LOOKBACK_DAYS", "FX_PATH", "CBC_PAGE_URL"):
                keep[n.targets[0].id] = ast.dump(n)
        return keep
    assert defs(orig) == defs(UPDATE_PY)


_RUNNER = r'''
import sys, types, runpy, urllib.request, json, time
time.sleep = lambda s: None
import pandas as pd
sys.path.insert(0, {src!r})
import listing_guard as lg
lg.configure(cache_path={cache!r})
def _no_net(*a, **k): raise OSError("network disabled in test")
_PAY = {payload_dir!r}
if _PAY:
    import os
    _SID = {{v: k for k, v in lg.SOURCES.items()}}
    lg._default_fetcher = lambda url: open(os.path.join(_PAY, _SID[url] + ".json"), "rb").read()
else:
    lg._default_fetcher = lambda url: (_ for _ in ()).throw(OSError("TWSE unreachable (mock)"))
urllib.request.urlopen = _no_net
m = types.ModuleType("yfinance")
def download(t, *a, **k):
    idx = pd.bdate_range("2024-11-01", "2024-11-08")
    return pd.DataFrame({{"Close": [1.0]*len(idx)}}, index=idx)
m.download = download
sys.modules["yfinance"] = m
runpy.run_path({upd!r}, run_name="__main__")
'''


def _run_main(tmp_path, cache, official=None):
    wd = tmp_path / "run"
    wd.mkdir()
    payload_dir = None
    if official is not None:                      # 模擬官方回應（通過完整度檢查的版本）
        payload_dir = tmp_path / "official"
        payload_dir.mkdir()
        for sid, blob in official.items():
            (payload_dir / f"{sid}.json").write_bytes(blob)
        payload_dir = str(payload_dir)
    script = wd / "runner.py"
    script.write_text(_RUNNER.format(src=str(SRC), cache=str(cache), upd=str(UPDATE_PY), payload_dir=payload_dir),
                      encoding="utf-8")
    return subprocess.run([sys.executable, str(script)], cwd=wd, capture_output=True, text=True, timeout=300)


@needs_update
def test_update_main_exits_nonzero_on_refusal_and_zero_when_resolved(tmp_path):
    # 用真實 tickers_tw 建 cache（全部可解析）→ TWSE 掛掉仍 exit 0
    src = UPDATE_PY.read_text(encoding="utf-8")
    import ast, re
    tw = ast.literal_eval(re.search(r"tickers_tw = (\[.*?\n\])", src, re.S).group(1))
    codes = [t[:-3] for t in tw if not t.startswith("00")]
    roster = _roster(n=850) + [{"公司代號": c, "上市日期": "20000105"} for c in codes if c not in ("7769", "2330", "6770")]
    good = tmp_path / "good.json"
    assert lg.refresh_cache(path=good, tracked=tw, fetcher=fetcher_from(payloads(roster=roster)), now=NOW).cache_usable
    (tmp_path / "a").mkdir()
    r = _run_main(tmp_path / "a", good)
    assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-2000:]
    assert "SOURCE_FETCH_FAILURE" in r.stdout and "沿用既有 cache" in r.stdout
    # 同一份 cache 拿掉一檔追蹤中的股票 → 該檔被拒 → exit 2
    bad = tmp_path / "bad.json"
    c = lg.load_cache(good)
    c["listed_equities"].pop("2330")
    c["checksum"] = lg.compute_checksum(c)
    lg.write_cache(c, bad)
    lg.configure()
    (tmp_path / "b").mkdir()
    r = _run_main(tmp_path / "b", bad)
    assert r.returncode == 2, r.stdout[-2000:]
    assert "REFUSED 2330.TW" in r.stdout and not (tmp_path / "b" / "run" / "tw" / "2330.TW.csv").exists()
    assert (tmp_path / "b" / "run" / "us" / "DBMFE.csv").exists()          # 其餘照跑（log 完整），但整體非零


def _real_tracked_roster():
    import ast, re
    src = UPDATE_PY.read_text(encoding="utf-8")
    tw = ast.literal_eval(re.search(r"tickers_tw = (\[.*?\n\])", src, re.S).group(1))
    codes = [t[:-3] for t in tw if not t.startswith("00")]
    roster = _roster(n=850) + [{"公司代號": c, "上市日期": "20000105"} for c in codes if c not in ("7769", "2330", "6770")]
    return tw, roster


@needs_update
def test_update_main_tracked_exit_fails_closed_without_commit(tmp_path):
    """v2.1 regression：官方名冊通過完整度檢查但少了追蹤中的 2330 → 不靠舊 cache 放行，exit 2。"""
    tw, roster = _real_tracked_roster()
    good = tmp_path / "good.json"
    assert lg.refresh_cache(path=good, tracked=tw, fetcher=fetcher_from(payloads(roster=roster)), now=NOW).cache_usable
    before = good.read_bytes()
    lg.configure()
    official = payloads(roster=[r for r in roster if r["公司代號"] != "2330"])
    (tmp_path / "p").mkdir()
    r = _run_main(tmp_path / "p", good, official=official)
    assert r.returncode == 2, r.stdout[-3000:] + r.stderr[-2000:]
    assert "PENDING_TRACKED_EXIT" in r.stdout and "REFUSED 2330.TW" in r.stdout
    run = tmp_path / "p" / "run"
    assert not (run / "tw" / "2330.TW.csv").exists()                 # 舊 cache 沒有替它放行
    assert (run / "tw" / "2317.TW.csv").exists()                     # 其他標的照跑（log 完整），但整體非零
    assert good.read_bytes() == before                               # cache 不覆寫


@needs_update
def test_update_main_valid_official_roster_exits_zero(tmp_path):
    tw, roster = _real_tracked_roster()
    good = tmp_path / "good.json"
    assert lg.refresh_cache(path=good, tracked=tw, fetcher=fetcher_from(payloads(roster=roster)), now=NOW).cache_usable
    lg.configure()
    (tmp_path / "q").mkdir()
    r = _run_main(tmp_path / "q", good, official=payloads(roster=roster))
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-2000:]
    assert "OK_UNCHANGED" in r.stdout and "REFUSED" not in r.stdout


# ───────────────────────── 15. GitHub Action failure propagation ─────────────────────────

@pytest.mark.skipif(not WORKFLOW.exists(), reason="workflow 不在此目錄")
def test_action_nonzero_update_blocks_commit():
    import yaml
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = wf["jobs"]["update-and-build"]["steps"]
    names = [s.get("name") for s in steps]
    i_upd, i_commit = names.index("Update price data"), names.index("Commit and push")
    assert steps[i_upd]["run"].strip() == "python update.py"
    assert not steps[i_upd].get("continue-on-error") and not wf["jobs"]["update-and-build"].get("continue-on-error")
    assert "if" not in steps[i_commit]                                    # 預設 success()：前面任一步失敗即跳過
    # GitHub runner 語意的最小模擬：step 非零 → 後續沒有 if: always()/failure() 的 step 全部跳過
    executed = []
    rc = {"Update price data": 2}
    failed = False
    for s in steps:
        cond = str(s.get("if", "success()"))
        if failed and not any(k in cond for k in ("always()", "failure()")):
            continue
        executed.append(s.get("name"))
        if rc.get(s.get("name"), 0) != 0 and not s.get("continue-on-error"):
            failed = True
    assert "Commit and push" not in executed and "Update price data" in executed
