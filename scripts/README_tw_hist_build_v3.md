# tw_hist_build_v3.py — 使用說明（Research-only）

## 這是什麼
把現有 `scripts/tw_hist_build.py`（v1，硬寫死 Top20，未被本檔取代、原樣保留）的資料
抓取邏輯獨立出來、`--top-n` 參數化，可以對任一歷史 as-of-date 重建到任意深度
（20/40/60/80/100...）。方法論對齊現行 canonical v3 的建置規則（純市值排名，pre-rank
排除電信三雄，不套用 listing eligibility／owner override 這類 production-only 的下游
規則）。完整理由與跟 v1／v3 canonical 的關係，見腳本本身 docstring（比這份 README 詳細）。

## 用法
```bash
python tw_hist_build_v3.py --as-of-date 20260226 --top-n 100 --out research_20260226_top100.json
```
`--as-of-date` 用 TWSE 的 `YYYYMMDD`（或 `YYYY-MM-DD`），對齊 v3 canonical 的
`shares_date` 慣例；非交易日會自動往前找最近交易日，實際用到的日期記在輸出的
`shares_date` 欄位。

## 這輪 smoke test 結果（只測 2 個歷史點，不是完整重建）

| as-of-date | canonical Top60 exact match | 備註 |
|---|---|---|
| 20260226（最新、已雙重 golden-match 過） | 60/60 | 無 mismatch |
| 20170224（跨年代測試點） | 60/60 | 無 mismatch |

額外交叉驗證：20260226 那次重建出來的 rank 61/62 分別是 5269（祥碩）、2385（群光），
跟 SI 裡記載「7769／3037 排除後正式遞補上場的名字」完全一致——這件事我沒有先去看
答案再湊，是重建完之後對照才發現吻合，算是一個獨立於 smoke test 本身的旁證。

## 這輪沒做的事
- 沒有跑完整 38 個歷史輪換點
- 沒有產出任何 Top80/Top100 正式 research artifact（只有這兩個 smoke test 檔案，在
  `/tmp`，不是交付物）
- 沒有對舊版 v1 已知的「11/38 缺陷」（`get()` 只在整體回應全空才判失敗、不檢查單一
  產業代碼回傳列數）做 root-cause 修復——本檔沿用同一個 `get()` 邏輯、同一個弱點，
  如果之後要跑完整 38 點重建，這個已知風險還在，需要靠雙重 golden-match ＋ 內部一致性
  掃描去抓，跟 v1→v2→v3 當初的做法一樣，不是這輪 smoke test 範圍。
- 沒有動 production Top60、沒有碰 `tw_universe_builder.py`
