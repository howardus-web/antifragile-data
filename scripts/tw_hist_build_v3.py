#!/usr/bin/env python3
"""
tw_hist_build_v3.py — TW 電子產業市值 historical point-in-time ranking builder（v3-compatible）

STATUS: RESEARCH-ONLY. 不是 production 引擎。不寫回 antifragile-data repo、不產生
commit package、不碰 universe_snapshots/tw/ 下任何現行 production 檔案。這支腳本的
唯一輸出是獨立的 research JSON（--out 指定路徑），供之後可能的 Top80/Top100
reconstruction 使用；是否要用它跑完整 38 個歷史輪換點的正式重建，是下一輪的決定，
不在這支腳本自己的職責內。

用途
----
對任一歷史 as-of-date，重建 point-in-time 電子產業市值排名，輸出 ranked list（不是
只截 Top20）。--top-n 決定輸出到第幾名（20/40/60/80/100 皆可），同一次執行內部一律
先算出完整候選排名，再依 --top-n 截斷輸出——所以同一個 as-of-date 換 --top-n 重跑，
前面共同的名次順序不會變。

跟現行 canonical v3（tw_ranked_universe_history_2007H2-2026H1_v3.json）的關係
--------------------------------------------------------------------------
這支腳本的目標是「用同一套建置規則、把深度延伸到 v3 目前沒有的 61-100 名」，不是另
一套新方法論。方法論比照 v3 自身記載的建置規則（見該檔案 meta.rule 欄位）：

  1. 產業範圍：TWSE 電子相關產業代碼 24-31（半導體/電腦週邊/光電/通信網路/
     電子零組件/電子通路/資訊服務/其他電子）——industry 名稱字串比照 v3 canonical
     實際使用的寫法（例如「電腦週邊」，不是 tw_universe_builder.py 用的「電腦及週邊」，
     兩者是不同腳本、各自獨立長出來的字串，這裡刻意對齊 v3 canonical 而非 production
     live builder，避免同一份研究資料出現兩種產業名稱寫法）。
  2. 排除電信三雄（2412 中華電、3045 台灣大、4904 遠傳）：在排名之前排除，被排除的
     標的完全不佔用任何名次（跟 v3 canonical 的 schema_note 描述一致：telecom
     exclusions applied pre-ranking，never computed a rank for them）。
  3. 市值 = 該歷史交易日收盤價 × 該歷史交易日當下已發行股數。股數來源刻意不用
     t187ap03_L（該端點只回傳「查詢當下」股數，不適合歷史重建，tw_universe_builder.py
     用它是因為那支腳本只服務「當下」這個唯一情境）；改用 MI_QFIIS 帶歷史日期參數
     查詢，這是 v1 歷史建置腳本（scripts/tw_hist_build.py）已經驗證過、也是 v1/v2/v3
     canonical 檔案實際使用的股數來源，本腳本延續同一個資料源，不是新方法。
  4. 排名依市值降冪排序，純排名，**不套用 listing eligibility（12 個月 SMA 形成）篩選，
     不套用 owner override**。這兩者是 tw_universe_builder.py 在建置「即將真的拿去交易
     的 production rotation snapshot」時才套用的下游規則，不屬於這份 ranking archive
     本身——v3 canonical 自己的 meta.rule 描述裡完全沒有 eligibility 這一層，是純市值
     排名檔案；下游要不要套用 mechanical_ineligible／owner_override，是從這份 ranked
     list 取用時才決定的事，不在這支 builder 的職責內。若在這裡先套用 eligibility，
     反而會讓輸出跟 v3 canonical 對不齊（v3 canonical 裡確實可能存在還沒滿 12 個月的
     新股，這是預期行為，不是缺陷）。

跟 scripts/tw_hist_build.py（v1，現行仍在 repo，未被本檔取代）的差異
--------------------------------------------------------------------
v1 那支腳本硬寫死只算 Top20（`top = sorted(cap, key=cap.get, reverse=True)[:20]`），
拿掉 20 名之後的候選直接丟棄，且沒有任何 --top-n 概念。本檔複用 v1 完全相同的資料
抓取邏輯（MI_INDEX 逐產業代碼 + MI_QFIIS 歷史股數），只把「截斷在第幾名」跟「排名
本身要不要保留到輸出」兩件事參數化、獨立出來。v1 本身不變、不修改、不被本檔覆蓋。

用法
----
python tw_hist_build_v3.py --as-of-date 20260226 --top-n 100 --out research_20260226_top100.json

以上 as-of-date 用 TWSE 的 YYYYMMDD 格式（沿用 v3 canonical 的 shares_date 慣例），
若當天非交易日，會自動往前找最近的交易日（跟 v1 find_trading_day 邏輯一致），實際
使用的交易日會記在輸出 metadata 的 shares_date 欄位，不一定等於你傳入的 --as-of-date。
"""
import argparse
import calendar
import datetime
import json
import time
from pathlib import Path

import requests

H = {'User-Agent': 'Mozilla/5.0'}
EXCL = {'2412', '3045', '4904'}  # 電信三雄，排名前排除
# industry 名稱字串刻意對齊 v3 canonical 實際使用的寫法（見上方 docstring 說明）
IND = {'24': '半導體', '25': '電腦週邊', '26': '光電', '27': '通信網路',
       '28': '電子零組件', '29': '電子通路', '30': '資訊服務', '31': '其他電子'}

BUILD_METHODOLOGY = (
    "TWSE MI_INDEX industry codes 24-31 (electronics) point-in-time close; "
    "MI_QFIIS shares outstanding at as-of-date (historical, not current); "
    "mktcap=close*shares; exclude 2412/3045/4904 pre-ranking; rank desc. "
    "No listing-eligibility filter, no owner override — pure ranking archive, "
    "same rule family as tw_ranked_universe_history_2007H2-2026H1_v3.json."
)


class BuildError(Exception):
    pass


def get(url, params, max_attempts=6):
    """比照 v1 script 的 retry/backoff 邏輯，未修改行為。"""
    for attempt in range(max_attempts):
        try:
            r = requests.get(url, params=dict(response='json', **params), headers=H, timeout=30)
            j = r.json()
            time.sleep(3.2)
            return j
        except Exception:
            wait = 30 * (attempt + 1)
            print(f"  [backoff {wait}s]", flush=True)
            time.sleep(wait)
    raise BuildError(f'{url} 連續 {max_attempts} 次重試失敗')


def find_trading_day(as_of_date: str) -> str:
    """as_of_date 為 YYYYMMDD 或 YYYY-MM-DD；非交易日往前找，跟 v1 邏輯一致。"""
    s = as_of_date.replace('-', '')
    d = datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    for _ in range(8):
        if d.weekday() < 5:
            ds = d.strftime('%Y%m%d')
            j = get('https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX', {'date': ds, 'type': '24'})
            if j.get('stat') == 'OK' and any(len(t.get('data', [])) > 0 for t in j.get('tables', [])):
                return ds
        d -= datetime.timedelta(days=1)
    raise BuildError(f'{as_of_date} 往前找 8 天都沒找到有效交易日')


def fetch_shares_historical(ds: str):
    """歷史股數，MI_QFIIS 帶指定日期查詢——這是跟 tw_universe_builder.py（只能查當下）
    最關鍵的差異，point-in-time 正確性靠這個函式。找不到當天資料時往前走最多 5 個
    交易日（跟 v1 fetch_shares 邏輯一致，未修改行為）。"""
    d = datetime.datetime.strptime(ds, '%Y%m%d').date()
    for _ in range(5):
        dd = d.strftime('%Y%m%d')
        j = get('https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS', {'date': dd, 'selectType': 'ALLBUT0999'})
        tb = j.get('tables', [j])[0] if 'tables' in j else j
        data = tb.get('data') or j.get('data')
        if data:
            sh, nm = {}, {}
            for row in data:
                c = row[0].strip()
                try:
                    sh[c] = int(row[3].replace(',', ''))
                    nm[c] = row[1].strip()
                except (ValueError, IndexError):
                    pass
            return sh, nm, dd
        d -= datetime.timedelta(days=1)
        while d.weekday() >= 5:
            d -= datetime.timedelta(days=1)
    raise BuildError(f'{ds} 往前找 5 個交易日都抓不到 MI_QFIIS 股數資料')


def build_ranked_list(as_of_date: str, top_n: int, exclusions=None):
    """回傳 (ranked_list, shares_date)。ranked_list 是完整排名（截到 top_n），
    每筆帶 rank/code/name/industry/close/shares/mktcap，跟 v3 canonical 的
    snapshots[date]['top'] 欄位結構一致，方便直接比對。"""
    excl = set(exclusions) if exclusions is not None else set(EXCL)
    ds = find_trading_day(as_of_date)

    closes, ind_of = {}, {}
    for code in IND:
        j = get('https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX', {'date': ds, 'type': code})
        for t in j.get('tables', []):
            if '每日收盤行情' in t.get('title', '') or '收盤行情' in t.get('title', ''):
                for row in t['data']:
                    c = row[0].strip()
                    try:
                        closes[c] = float(row[8].replace(',', ''))
                        ind_of[c] = code
                    except (ValueError, IndexError):
                        pass
    if not closes:
        raise BuildError(f'{ds}：MI_INDEX 產業表全空')

    shares, names, sdate = fetch_shares_historical(ds)
    if not shares:
        raise BuildError(f'{ds}：股數缺漏')

    cap = {c: closes[c] * shares[c] for c in closes
           if c in shares and c not in excl and shares[c] > 0}
    ranked_codes = sorted(cap, key=cap.get, reverse=True)

    if len(ranked_codes) < top_n:
        print(f"  ⚠ {ds}：合格候選只有 {len(ranked_codes)} 檔，不足 top_n={top_n}，"
              f"輸出會少於要求的深度（不視為錯誤，如實反映當天候選數量）。")

    selected = ranked_codes[:top_n]
    ranked_list = [
        dict(rank=i + 1, code=c, name=names.get(c, ''), industry=IND[ind_of[c]],
             close=closes[c], shares=shares[c], mktcap=round(cap[c]))
        for i, c in enumerate(selected)
    ]
    return ranked_list, sdate


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--as-of-date', required=True, help='YYYYMMDD 或 YYYY-MM-DD，比照 v3 shares_date 慣例')
    ap.add_argument('--top-n', type=int, required=True, help='輸出到第幾名，例如 20/40/60/80/100')
    ap.add_argument('--exclusions', default=','.join(sorted(EXCL)),
                     help='逗號分隔的排除代碼，預設電信三雄')
    ap.add_argument('--out', required=True, help='輸出 research JSON 路徑')
    args = ap.parse_args()

    exclusions = [x.strip() for x in args.exclusions.split(',') if x.strip()]

    print(f"[1] 建置 {args.as_of_date} 電子產業市值排名，top_n={args.top_n}（排除 {exclusions}）...")
    ranked_list, shares_date = build_ranked_list(args.as_of_date, args.top_n, exclusions)
    print(f"    實際交易日：{shares_date}，取得 {len(ranked_list)} 檔")

    payload = {
        "status": "RESEARCH-ONLY — not a production artifact, not committed to antifragile-data repo",
        "build_script": "tw_hist_build_v3.py",
        "build_methodology": BUILD_METHODOLOGY,
        "requested_as_of_date": args.as_of_date,
        "shares_date": shares_date,
        "top_n": args.top_n,
        "exclusions": [f"{c}.TW" for c in exclusions],
        "n_returned": len(ranked_list),
        "ranked_list": ranked_list,
    }
    Path(args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"[2] 寫入 {args.out}")
    print("完成。")


if __name__ == '__main__':
    try:
        main()
    except BuildError as e:
        print(f"\n✗ BuildError: {e}")
        raise SystemExit(1)
