"""
listing_guard.py — TW 個股價格資料入庫防呆：把興櫃期間資料與正式上市後資料分開

背景：yfinance 對「興櫃轉上市」的股票，回傳的是興櫃＋上市接續在一起的單一連續序列，
沒有任何標記告訴你接縫在哪。任何 yfinance 全史下載若直接寫入 production CSV，會把
興櫃期間的價格誤植為「上市後歷史」，汙染 12 個月 SMA 這類需要乾淨上市後歷史的計算。

本模組只回答一個問題：這一筆股價資料，是不是官方上市日之後的資料。不判斷 IPO /
listing age / signal eligibility——那是呼叫端（tw_universe_builder／tw_pit_roster）的事。

v2（2026-10-17，WL-006 reliability repair）——為什麼改：
  v1 每次執行都即時打 TWSE t187ap03_L，任何例外一律吞掉、把「已抓過」設成 True、對所有
  標的回傳 None，而 None 被解讀成「不砍」。一次 TWSE 失敗就讓整批興櫃期間資料原樣寫進
  production CSV（2026-09-13／09-16／09-24 三次實際發生並被 commit），也讓 tw_pit_roster
  把每檔都當成「無上市日、保守合格」。
  v2 的規則：
    1. 日常查詢只讀「已驗證的上市日 cache」（listing_dates_verified.json），不連網。
       相同 cache → 相同結果。TWSE 暫時掛掉不影響日常更新。
    2. 官方刷新（refresh_cache，只由 update.py 每次開頭呼叫）抓 TWSE t187ap03_L、
       TWSE t187ap47_L（基金）、TPEx ETF 清單，驗證結構、日期、涵蓋率後才合併進 cache；
       抓不到或回應不完整 → 保留舊 cache、不覆寫、照常用舊 cache。
    3. 價格入庫（ingestion_cutoff／truncate_pre_listing）：只有 VERIFIED_LISTED_EQUITY
       （砍上市日以前）與 VERIFIED_NOT_APPLICABLE（ETF、非台股，原樣）可以放行；
       其餘一律 raise ListingGuardRefusal，絕不回傳「不砍」。
    4. get_listing_date() 保留 v1 介面：已驗證上市股回日期；ETF／非台股回 None；
       「不在已驗證的現行上市名冊裡」（例如已下市、已合併的歷史代號）回 None——這跟 v1
       在 TWSE 抓取成功時的語意完全相同，tw_pit_roster 的凍結契約依賴它；但沒有可信
       cache、上市日衝突、身分不明確時一律 raise，不再靜默回 None。

結果（Status）：
  VERIFIED_LISTED_EQUITY      官方名冊有、日期合法
  VERIFIED_NOT_APPLICABLE     ETF／基金（官方基金清單）或非台股標的
  UNRESOLVED_IDENTITY_OR_DATE 不在已驗證來源、上市日衝突、身分不明確、沒有可信 cache
  SOURCE_FETCH_FAILURE        官方刷新：網路／HTTP 失敗（只出現在 refresh 報告）
  INVALID_OFFICIAL_RESPONSE   官方刷新：JSON 壞掉、結構不對、日期不合法、筆數不足（同上）
  PENDING_TRACKED_EXIT        官方刷新（v2.1）：回應通過完整度檢查，但追蹤中的上市股不在名冊——
                              待 Owner 確認；該檔拒絕入庫、update.py 非零結束、Action 不 commit

新上市股票怎麼進 cache：update.py 每次開頭跑 refresh_cache()，官方回應通過驗證時，
名冊裡所有公司（含新上市）一併寫入，first_verified_at＝當次抓取時間。新股不在 cache、
當天官方又抓不到 → 該檔入庫被拒、update.py 非零結束、Action 不 commit；隔天官方恢復
即自動放行。不手填上市日。

上市日衝突：官方新回應的日期與 cache 不同 → 保留 cache 日期、記入 conflicts、該檔入庫被拒，
直到 Owner 以 `python listing_guard.py accept-official <代號>` 明示採用官方新日期。
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

GUARD_VERSION = "2.1"
SCHEMA = "listing_guard_cache.v1"
CACHE_FILENAME = "listing_dates_verified.json"

VERIFIED_LISTED_EQUITY = "VERIFIED_LISTED_EQUITY"
VERIFIED_NOT_APPLICABLE = "VERIFIED_NOT_APPLICABLE"
UNRESOLVED_IDENTITY_OR_DATE = "UNRESOLVED_IDENTITY_OR_DATE"
SOURCE_FETCH_FAILURE = "SOURCE_FETCH_FAILURE"
INVALID_OFFICIAL_RESPONSE = "INVALID_OFFICIAL_RESPONSE"
# v2.1：官方回應通過完整度檢查，但追蹤中的上市股從名冊消失（可能下市，也可能官方漏列）。
# 這不是網路問題，舊 cache 不能替它擔保——本次 refresh 結果 blocks_publication=True，
# update.py 拒絕該檔並非零結束（Action 不 commit），直到官方名冊恢復或 Owner 把它移出 tickers_tw。
PENDING_TRACKED_EXIT = "PENDING_TRACKED_EXIT"

# 官方來源（角色與 v1 相同：t187ap03_L 仍是上市日的官方輸入）
SOURCES = {
    "twse_t187ap03_L": "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",   # 上市公司基本資料
    "twse_t187ap47_L": "https://openapi.twse.com.tw/v1/opendata/t187ap47_L",   # 上市基金（含 ETF）基本資料
    "tpex_etf_filter": "https://info.tpex.org.tw/api/etfFilter",               # 上櫃 ETF 清單
}
# 完整度門檻（拒收「看起來成功但其實殘缺」的回應）
MIN_LISTED_RECORDS = 800
MIN_ROSTER_RETENTION = 0.90          # 相對前一份已驗證名冊
MAX_MALFORMED_FRACTION = 0.01
MIN_TWSE_FUNDS = 50
MIN_TPEX_ETFS = 20
# 識別：yfinance 符號別名（只放有官方證據的）
ALIASES = {"00679B.TWO": "00679B", "00679B.TW": "00679B"}


class ListingGuardError(Exception):
    def __init__(self, status: str, ticker: str, reason: str):
        self.status, self.ticker, self.reason = status, ticker, reason
        super().__init__(f"{ticker}: {status} ({reason})")


class ListingGuardRefusal(ListingGuardError):
    """價格入庫被拒：呼叫端不得寫出這檔的 CSV。"""


@dataclass(frozen=True)
class Resolution:
    ticker: str
    code: "str | None"
    status: str
    listing_date: "str | None"
    instrument_class: str
    reason: str
    cache_checksum: "str | None"


# ───────────────────────── cache 位置與載入 ─────────────────────────

_CONFIGURED_PATH: "Path | None" = None
_LOADED: "dict | None" = None
_LOADED_FROM: "Path | None" = None
_LOAD_ERROR: "str | None" = None


def configure(repo_root=None, cache_path=None) -> Path:
    """指定 cache 位置（MAS 端的消費者以 data repo 根目錄呼叫）。重設已載入的 cache。"""
    global _CONFIGURED_PATH, _LOADED, _LOADED_FROM, _LOAD_ERROR
    if cache_path is not None:
        _CONFIGURED_PATH = Path(cache_path)
    elif repo_root is not None:
        _CONFIGURED_PATH = Path(repo_root) / CACHE_FILENAME
    else:
        _CONFIGURED_PATH = None
    _LOADED = _LOADED_FROM = _LOAD_ERROR = None
    return cache_path_in_use()


def cache_path_in_use() -> Path:
    if _CONFIGURED_PATH is not None:
        return _CONFIGURED_PATH
    env = os.environ.get("LISTING_GUARD_CACHE")
    if env:
        return Path(env)
    return Path(__file__).resolve().parent / CACHE_FILENAME


def _canonical(payload: dict) -> bytes:
    body = {k: v for k, v in payload.items() if k != "checksum"}
    return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def compute_checksum(payload: dict) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


def validate_cache_payload(payload) -> None:
    """結構＋checksum 驗證。不合法 → ValueError。"""
    if not isinstance(payload, dict) or payload.get("schema") != SCHEMA:
        raise ValueError("schema 不符")
    for k in ("listed_equities", "not_applicable", "conflicts", "retired", "sources"):
        if not isinstance(payload.get(k), dict):
            raise ValueError(f"缺欄位 {k}")
    if payload.get("checksum") != compute_checksum(payload):
        raise ValueError("checksum 不符（檔案被手動改過或損毀）")
    if len(payload["listed_equities"]) < MIN_LISTED_RECORDS:
        raise ValueError("上市名冊筆數不足")
    for code, e in payload["listed_equities"].items():
        _parse_date(e.get("d"))
        if e.get("src") not in payload["sources"]:
            raise ValueError(f"{code} 來源未登記")


def load_cache(path=None) -> dict:
    p = Path(path) if path is not None else cache_path_in_use()
    payload = json.loads(p.read_text(encoding="utf-8"))
    validate_cache_payload(payload)
    return payload


def _cache() -> "dict | None":
    global _LOADED, _LOADED_FROM, _LOAD_ERROR
    p = cache_path_in_use()
    if _LOADED is not None and _LOADED_FROM == p:
        return _LOADED
    try:
        _LOADED, _LOADED_FROM, _LOAD_ERROR = load_cache(p), p, None
    except FileNotFoundError:
        _LOADED, _LOADED_FROM, _LOAD_ERROR = None, p, f"找不到已驗證 cache：{p}"
    except Exception as e:  # noqa: BLE001
        _LOADED, _LOADED_FROM, _LOAD_ERROR = None, p, f"cache 不可信：{p}：{e}"
    return _LOADED


# ───────────────────────── 查詢 ─────────────────────────

def _code_of(repo_name: str) -> "str | None":
    if repo_name in ALIASES:
        return ALIASES[repo_name]
    for suf in (".TWO", ".TW"):
        if repo_name.endswith(suf):
            return repo_name[: -len(suf)]
    return None


def resolve(repo_name: str) -> Resolution:
    """純讀 cache，不連網、不 raise（狀態寫在 status）。"""
    code = _code_of(repo_name)
    if code is None:
        return Resolution(repo_name, None, VERIFIED_NOT_APPLICABLE, None, "NON_TW_MARKET",
                          "非台股代號（美股／UCITS／FX），不屬本防呆範圍", None)
    c = _cache()
    if c is None:
        return Resolution(repo_name, code, UNRESOLVED_IDENTITY_OR_DATE, None, "UNKNOWN",
                          f"NO_TRUSTED_CACHE: {_LOAD_ERROR}", None)
    ck = c["checksum"]
    if code in c["conflicts"]:
        cf = c["conflicts"][code]
        return Resolution(repo_name, code, UNRESOLVED_IDENTITY_OR_DATE, None, "LISTED_EQUITY",
                          f"LISTING_DATE_CONFLICT: cache {cf.get('cached')} vs 官方 {cf.get('official')}", ck)
    in_eq, in_na = code in c["listed_equities"], code in c["not_applicable"]
    if in_eq and in_na:
        return Resolution(repo_name, code, UNRESOLVED_IDENTITY_OR_DATE, None, "UNKNOWN",
                          "AMBIGUOUS_IDENTITY: 同時出現在上市公司名冊與基金清單", ck)
    if in_eq:
        return Resolution(repo_name, code, VERIFIED_LISTED_EQUITY, c["listed_equities"][code]["d"],
                          "TWSE_LISTED_EQUITY", "官方上市日", ck)
    if in_na:
        return Resolution(repo_name, code, VERIFIED_NOT_APPLICABLE, None,
                          c["not_applicable"][code]["class"], "官方基金／ETF 清單", ck)
    return Resolution(repo_name, code, UNRESOLVED_IDENTITY_OR_DATE, None, "UNKNOWN",
                      "NOT_IN_VERIFIED_SOURCES: 不在已驗證的現行上市名冊或基金清單", ck)


def get_listing_date(repo_name: str) -> "str | None":
    """v1 相容介面（tw_pit_roster／price_patch 舊呼叫點）。
    已驗證上市股 → 'YYYY-MM-DD'；ETF／非台股 → None；不在已驗證現行名冊 → None
    （與 v1 在 TWSE 抓取成功時同義：已下市／合併的歷史代號）。
    沒有可信 cache／日期衝突／身分不明確 → raise ListingGuardError（v1 會靜默回 None）。"""
    r = resolve(repo_name)
    if r.status == VERIFIED_LISTED_EQUITY:
        return r.listing_date
    if r.status == VERIFIED_NOT_APPLICABLE:
        return None
    if r.reason.startswith("NOT_IN_VERIFIED_SOURCES"):
        return None
    raise ListingGuardError(r.status, repo_name, r.reason)


def ingestion_cutoff(repo_name: str) -> "str | None":
    """價格入庫用。回傳要砍到的日期（不含當日以前），或 None＝已驗證不適用、原樣。
    其他一律 raise ListingGuardRefusal。"""
    r = resolve(repo_name)
    if r.status == VERIFIED_LISTED_EQUITY:
        return r.listing_date
    if r.status == VERIFIED_NOT_APPLICABLE:
        return None
    raise ListingGuardRefusal(r.status, repo_name, r.reason)


def truncate_pre_listing(df, repo_name: str):
    """把 df（DatetimeIndex）裡早於官方上市日的列去掉。
    回傳 (處理後的 df, listing_date 或 None)。None＝已驗證不適用（ETF／非台股），df 原樣。
    無法確認 → raise ListingGuardRefusal（v1 會原樣放行）。"""
    cutoff = ingestion_cutoff(repo_name)
    if cutoff is None:
        return df, None
    import pandas as pd
    return df[df.index >= pd.Timestamp(cutoff)], cutoff


# ───────────────────────── 官方刷新（只由 update.py 呼叫）─────────────────────────

@dataclass
class RefreshReport:
    status: str                 # OK_UNCHANGED / OK_UPDATED / SOURCE_FETCH_FAILURE / INVALID_OFFICIAL_RESPONSE / PENDING_TRACKED_EXIT
    detail: str
    cache_written: bool
    cache_usable: bool          # 刷新後（或刷新失敗後）是否仍有可信 cache 可用
    admitted: tuple = ()
    retired: tuple = ()
    new_conflicts: tuple = ()
    pending_tracked_exits: tuple = ()   # v2.1：追蹤中、卻從通過驗證的官方名冊消失的代號（待 Owner 確認）

    @property
    def blocks_publication(self) -> bool:
        """True → 呼叫端（update.py）本次必須非零結束、不得 commit。
        只有「追蹤股票退出官方名冊」會設 True；網路失敗／殘缺回應時沿用可信 cache，不阻擋。"""
        return bool(self.pending_tracked_exits) or not self.cache_usable

    def summary(self) -> str:
        s = f"[listing_guard {GUARD_VERSION}] refresh: {self.status} — {self.detail}"
        if self.admitted:
            s += f"\n  新進名冊: {', '.join(self.admitted[:30])}{' …' if len(self.admitted) > 30 else ''}"
        if self.retired:
            s += f"\n  退出名冊: {', '.join(self.retired[:30])}"
        if self.new_conflicts:
            s += f"\n  ⛔ 上市日衝突（該檔入庫將被拒，需 Owner 決定）: {', '.join(self.new_conflicts)}"
        if self.pending_tracked_exits:
            s += (f"\n  ⛔ 追蹤股票退出官方名冊，待 Owner 確認（本次不 commit）: {', '.join(self.pending_tracked_exits)}"
                  "\n     官方漏列 → 下次刷新自動恢復；確定下市 → 從 update.py 的 tickers_tw 移除")
        s += f"\n  cache 可用: {self.cache_usable}；本次寫檔: {self.cache_written}"
        return s


class _FetchFailure(Exception):
    pass


class _InvalidResponse(Exception):
    pass


class _PendingTrackedExit(Exception):
    def __init__(self, codes):
        self.codes = tuple(codes)
        super().__init__(f"追蹤中的上市股從官方名冊消失: {list(self.codes)}")


def _default_fetcher(url: str) -> bytes:
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MAS-data/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                if getattr(r, "status", 200) != 200:
                    raise RuntimeError(f"HTTP {r.status}")
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 + attempt)
    raise _FetchFailure(f"{url}: {last}")


def _parse_date(s) -> _dt.date:
    if not isinstance(s, str) or len(s) != 10:
        raise ValueError(f"日期格式錯誤: {s!r}")
    return _dt.date.fromisoformat(s)


def _yyyymmdd(raw, asof: _dt.date) -> str:
    """'19940905' → '1994-09-05'；必須是真實日期、1950 以後、不晚於抓取日。"""
    if not isinstance(raw, str):
        raise ValueError("非字串")
    raw = raw.strip()
    if len(raw) != 8 or not raw.isdigit():
        raise ValueError(f"非 8 位數字: {raw!r}")
    d = _dt.date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    if d.year < 1950 or d > asof:
        raise ValueError(f"日期超出合理範圍: {d}")
    return d.isoformat()


def _json_list(blob: bytes, what: str):
    try:
        data = json.loads(blob.decode("utf-8"))
    except Exception as e:  # noqa: BLE001
        raise _InvalidResponse(f"{what}: JSON 解析失敗（{e}）")
    return data


def parse_official(payloads: dict, asof: _dt.date, tracked_codes=()) -> dict:
    """驗證三份官方回應，回傳 {'listed': {code: date}, 'na': {code: class}, 'meta': {...}}。
    任何結構性問題 → _InvalidResponse。"""
    tracked_codes = set(tracked_codes)
    # 1) TWSE 上市公司
    d = _json_list(payloads["twse_t187ap03_L"], "twse_t187ap03_L")
    if not isinstance(d, list) or not d:
        raise _InvalidResponse("twse_t187ap03_L: 回應不是非空清單")
    listed, bad, table_dates = {}, [], set()
    for rec in d:
        if not isinstance(rec, dict):
            bad.append(("?", "非物件"))
            continue
        code = rec.get("公司代號")
        if not isinstance(code, str) or not code.strip():
            bad.append(("?", "缺公司代號"))
            continue
        code = code.strip()
        try:
            dd = _yyyymmdd(rec.get("上市日期"), asof)
        except ValueError as e:
            bad.append((code, str(e)))
            continue
        if code in listed and listed[code] != dd:
            raise _InvalidResponse(f"twse_t187ap03_L: {code} 同一回應內出現兩個上市日")
        listed[code] = dd
        if rec.get("出表日期"):
            table_dates.add(str(rec.get("出表日期")))
    bad_tracked = [b for b in bad if b[0] in tracked_codes]
    if bad_tracked:
        raise _InvalidResponse(f"twse_t187ap03_L: 追蹤中的上市股紀錄不合法: {bad_tracked[:5]}")
    if len(bad) > MAX_MALFORMED_FRACTION * len(d):
        raise _InvalidResponse(f"twse_t187ap03_L: 不合法紀錄過多（{len(bad)}/{len(d)}）")
    if len(listed) < MIN_LISTED_RECORDS:
        raise _InvalidResponse(f"twse_t187ap03_L: 上市公司僅 {len(listed)} 筆，低於 {MIN_LISTED_RECORDS}")
    # 2) TWSE 基金（含 ETF）
    f = _json_list(payloads["twse_t187ap47_L"], "twse_t187ap47_L")
    if not isinstance(f, list):
        raise _InvalidResponse("twse_t187ap47_L: 回應不是清單")
    na = {}
    for rec in f:
        code = rec.get("基金代號") if isinstance(rec, dict) else None
        if isinstance(code, str) and code.strip():
            na[code.strip()] = "TWSE_LISTED_FUND"
    if sum(1 for v in na.values() if v == "TWSE_LISTED_FUND") < MIN_TWSE_FUNDS:
        raise _InvalidResponse("twse_t187ap47_L: 基金筆數不足")
    # 3) TPEx ETF
    t = _json_list(payloads["tpex_etf_filter"], "tpex_etf_filter")
    rows = t.get("data") if isinstance(t, dict) else None
    if not isinstance(rows, list):
        raise _InvalidResponse("tpex_etf_filter: 缺 data 清單")
    n_tpex = 0
    for rec in rows:
        code = rec.get("stockNo") if isinstance(rec, dict) else None
        if isinstance(code, str) and code.strip():
            na.setdefault(code.strip(), "TPEX_ETF")
            n_tpex += 1
    if n_tpex < MIN_TPEX_ETFS:
        raise _InvalidResponse("tpex_etf_filter: ETF 筆數不足")
    return {"listed": listed, "na": na,
            "meta": {"malformed_records": len(bad), "records": len(d),
                     "table_dates": sorted(table_dates)}}


def _iso_now(now) -> str:
    n = now or _dt.datetime.now(_dt.timezone.utc)
    return n.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def merge(prior: "dict | None", parsed: dict, payloads: dict, retrieved_at: str,
          tracked_codes=()) -> "tuple[dict, dict]":
    """把已驗證的官方回應合併進 cache。回傳 (new_payload, changes)。
    完整度不足（相對前一份名冊）→ _InvalidResponse，不產生新 cache。"""
    tracked_codes = set(tracked_codes)
    listed_new = parsed["listed"]
    if prior is not None:
        prev = prior["listed_equities"]
        if len(listed_new) < MIN_ROSTER_RETENTION * len(prev):
            raise _InvalidResponse(f"上市名冊 {len(listed_new)} 筆，少於前次 {len(prev)} 的 "
                                   f"{MIN_ROSTER_RETENTION:.0%}，視為殘缺回應")
        missing_tracked = sorted(c for c in tracked_codes if c in prev and c not in listed_new)
        if missing_tracked:   # 回應本身通過完整度檢查 → 不是殘缺回應，是待確認的退出（v2.1）
            raise _PendingTrackedExit(missing_tracked)
    src_meta = {}
    for sid, blob in payloads.items():
        src_meta[sid] = {"url": SOURCES[sid], "retrieved_at": retrieved_at,
                         "sha256": hashlib.sha256(blob).hexdigest()}
    src_meta["twse_t187ap03_L"]["records"] = parsed["meta"]["records"]
    src_meta["twse_t187ap03_L"]["malformed_records"] = parsed["meta"]["malformed_records"]
    src_meta["twse_t187ap03_L"]["table_dates"] = parsed["meta"]["table_dates"]

    p_eq = dict(prior["listed_equities"]) if prior else {}
    p_conf = dict(prior["conflicts"]) if prior else {}
    p_ret = dict(prior["retired"]) if prior else {}
    p_na = dict(prior["not_applicable"]) if prior else {}

    eq, admitted, conflicts_new = {}, [], []
    for code, d in sorted(listed_new.items()):
        old = p_eq.get(code)
        if old is None:
            r = p_ret.get(code)
            if r is not None and r.get("d") != d:
                p_conf[code] = {"cached": r.get("d"), "official": d, "observed_at": retrieved_at,
                                "source": "twse_t187ap03_L"}
                conflicts_new.append(code)
                eq[code] = {"d": r.get("d"), "src": r.get("src", "twse_t187ap03_L"), "at": r.get("at", retrieved_at)}
                continue
            eq[code] = {"d": d, "src": "twse_t187ap03_L", "at": retrieved_at}
            p_ret.pop(code, None)
            admitted.append(code)
        elif old["d"] == d:
            eq[code] = old
        else:  # 衝突：保留 cache 日期、記錄、該檔入庫被拒
            eq[code] = old
            if code not in p_conf:
                conflicts_new.append(code)
            p_conf[code] = {"cached": old["d"], "official": d, "observed_at": retrieved_at,
                            "source": "twse_t187ap03_L"}
    retired = sorted(c for c in p_eq if c not in listed_new)
    for c in retired:
        p_ret[c] = dict(p_eq[c], retired_at=retrieved_at)

    na = {}
    for code, cls in sorted(parsed["na"].items()):
        if code in listed_new:
            continue  # 身分衝突：resolve() 會判成 AMBIGUOUS；這裡不放進 not_applicable 以免遮蔽
        old = p_na.get(code)
        na[code] = old if (old and old.get("class") == cls) else {"class": cls, "src": (
            "twse_t187ap47_L" if cls == "TWSE_LISTED_FUND" else "tpex_etf_filter"), "at": retrieved_at}
    # 前次 not_applicable 但本次清單沒有：保留（基金可能下市；保留不會讓價格被誤放行）
    for code, v in p_na.items():
        if code not in na and code not in listed_new:
            na[code] = v

    def strip_volatile(p):
        return {k: v for k, v in p.items() if k not in ("generated_at", "sources", "checksum")}

    payload = {
        "schema": SCHEMA,
        "guard_version": GUARD_VERSION,
        "generated_at": retrieved_at,
        "sources": src_meta,
        "listed_equities": eq,
        "not_applicable": na,
        "retired": p_ret,
        "conflicts": p_conf,
    }
    if prior is not None and strip_volatile(payload) == strip_volatile(prior):
        changes = {"changed": False}
        return prior, changes
    payload["checksum"] = compute_checksum(payload)
    return payload, {"changed": True, "admitted": admitted, "retired": retired,
                     "new_conflicts": conflicts_new}


def write_cache(payload: dict, path=None) -> Path:
    validate_cache_payload(payload)
    p = Path(path) if path is not None else cache_path_in_use()
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=1) + "\n", encoding="utf-8")
    os.replace(tmp, p)
    configure(cache_path=p)
    return p


def refresh_cache(path=None, tracked=(), fetcher=None, now=None, write=True) -> RefreshReport:
    """官方刷新。永不 raise（失敗寫在報告裡）；失敗時舊 cache 原封不動、照常可用。"""
    p = Path(path) if path is not None else cache_path_in_use()
    tracked_codes = [c for c in (_code_of(t) for t in tracked) if c]
    prior, prior_err = None, None
    try:
        prior = load_cache(p)
    except FileNotFoundError:
        prior_err = "無既有 cache"
    except Exception as e:  # noqa: BLE001
        prior_err = f"既有 cache 不可信（{e}）"
    fetch = fetcher or _default_fetcher
    retrieved_at = _iso_now(now)
    asof = (now or _dt.datetime.now(_dt.timezone.utc)).astimezone(_dt.timezone.utc).date() + _dt.timedelta(days=1)
    try:
        payloads = {sid: fetch(url) for sid, url in SOURCES.items()}
    except Exception as e:  # noqa: BLE001
        return RefreshReport(SOURCE_FETCH_FAILURE, f"{e}；沿用既有 cache" if prior else f"{e}；{prior_err}",
                             False, prior is not None)
    try:
        parsed = parse_official(payloads, asof, tracked_codes)
        new, ch = merge(prior, parsed, payloads, retrieved_at, tracked_codes)
    except _InvalidResponse as e:
        return RefreshReport(INVALID_OFFICIAL_RESPONSE, f"{e}；不覆寫既有 cache" if prior else f"{e}；{prior_err}",
                             False, prior is not None)
    except _PendingTrackedExit as e:
        return RefreshReport(PENDING_TRACKED_EXIT, f"{e}；不覆寫既有 cache，這些代號本次拒絕入庫",
                             False, prior is not None, pending_tracked_exits=e.codes)
    if not ch["changed"]:
        configure(cache_path=p)
        return RefreshReport("OK_UNCHANGED", f"官方回應通過驗證，名冊與 cache 一致（{len(new['listed_equities'])} 檔）",
                             False, True)
    if write:
        write_cache(new, p)
    return RefreshReport("OK_UPDATED", f"官方回應通過驗證，cache 更新（{len(new['listed_equities'])} 檔"
                         f"{'' if prior else '；首次建立'}）", bool(write), True,
                         tuple(ch["admitted"]), tuple(ch["retired"]), tuple(ch["new_conflicts"]))


def accept_official(code: str, path=None) -> dict:
    """Owner 明示採用官方新日期，解除某檔的上市日衝突。"""
    p = Path(path) if path is not None else cache_path_in_use()
    payload = load_cache(p)
    cf = payload["conflicts"].pop(code, None)
    if cf is None:
        raise SystemExit(f"{code} 沒有登記中的衝突")
    payload["listed_equities"][code] = {"d": cf["official"], "src": cf.get("source", "twse_t187ap03_L"),
                                        "at": cf["observed_at"], "owner_accepted_conflict": cf}
    payload["checksum"] = compute_checksum(payload)
    write_cache(payload, p)
    return cf


def _main(argv) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="listing_guard cache 工具")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("refresh"); r.add_argument("--cache")
    v = sub.add_parser("verify"); v.add_argument("--cache")
    q = sub.add_parser("resolve"); q.add_argument("tickers", nargs="+"); q.add_argument("--cache")
    a = sub.add_parser("accept-official"); a.add_argument("code"); a.add_argument("--cache")
    args = ap.parse_args(argv)
    if args.cache:
        configure(cache_path=args.cache)
    if args.cmd == "refresh":
        rep = refresh_cache()
        print(rep.summary())
        return 0 if rep.cache_usable else 2
    if args.cmd == "verify":
        c = load_cache()
        print(json.dumps({"path": str(cache_path_in_use()), "checksum": c["checksum"],
                          "listed_equities": len(c["listed_equities"]), "not_applicable": len(c["not_applicable"]),
                          "conflicts": sorted(c["conflicts"]), "generated_at": c["generated_at"]},
                         ensure_ascii=False, indent=1))
        return 0
    if args.cmd == "resolve":
        for t in args.tickers:
            print(resolve(t))
        return 0
    if args.cmd == "accept-official":
        print(accept_official(args.code))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
