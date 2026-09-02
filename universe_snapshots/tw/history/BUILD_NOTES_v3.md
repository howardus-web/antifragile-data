# TW 電子 ranked 歷史母池 v3（depth 60）建置記錄
建置日 2026-09-02。Top60 production migration 用途。不動 v1、v2。

## 一、與 v2 的關係
v2（depth 40）維持不變、繼續有效，不因 v3 存在而過期——任何只需要 depth<=40 的研究可以繼續用 v2。
v3 是獨立的新檔案，depth 60，方法論與 v2 完全相同（見 v2 的 BUILD_NOTES_v2.md §二），唯一差異是
排名深度 40→60。v3 的 rank 1-40 部分與 v2 逐筆相同（同一批原始抓取資料，只是这次沒有裁到 40 就停）。

## 二、驗證
- Golden match ×2：20260226 對 production universe_2026-08-31_v2_rotation（20 檔）20/20，且各自在
  v3 的排名位置正確反映 7769（listing_guard，掛牌未滿12個月）與 3037（owner override）兩個排除——
  production 遞補上場的 2449／2301 在 v3 裡確實是 rank 21/22。20250829 對 production
  universe_2026-02-28_v1_rotation 20/20。
- 內部一致性掃描（depth-60 範圍，前後輪換點皆須在 rank50 內）：3 筆候選，其中 2 筆（3406 於
  20180227、2313 於 20190227）查證後為「該期排名 61/70，本來就在 depth-60 之外，被截斷誤判為缺席」，
  不是資料缺陷；只有 1 筆是真異常，且是已知殘留——2408 南亞科 20140829（減資重整停牌，v2 已記錄，
  非本次延伸新增）。
- Root cause 狀態：與 v2 相同，NOT hardened。`tw_hist_build_topN.py` 的 `get()` 仍只在整體回應全空時
  才判失敗，若某產業代碼回傳不完整表格仍會被靜默接受——這是 v1 當初 11/38 缺陷的根因，這次 41-60
  層延伸沿用同一支腳本、同一個未修的弱點，靠事後一致性掃描與雙 golden match 補驗證，不是腳本層面
  修好了。根因硬化本次 scope boundary 明確排除，另案處理。

## 三、41-60 層新增的 54 檔價格
- Top60 全期（38 輪換點）不重複成員 148 檔，94 檔已在 9/2 bundle 交付（rank 1-40 全期成員），
  本次新增 54 檔（只在 rank 41-60 出現過的成員）。
- 47 檔現存：yfinance auto_adjust 還原價，全期。
- 7 檔已下市（2350 大宇資訊、2403 友旺、2411 沛亨…等，詳見churn v3）：
  tw/{code}.TW.csv 為還原價 = FinMind 原始收盤 × 自最近事件往回累乘的 factor（等價 v2 的 9 檔方法），
  稽核 sidecar：raw_frozen_v3/{code}_raw.csv（原始收盤）、raw_frozen_v3/tw_adjustment_factors_v3.csv
  （62 筆事件，獨立於 v2 的 tw_adjustment_factors_v2.csv，不合併、不覆寫）。
  抽查：2456 於 2008-08-14 除權息，原始單日 -5.86% → 還原後 +3.49%，還原方向與量級合理。
- 資料品質敏感度：這 7 檔占 Top60 權重更低（1/60 而非 1/40），且與 v2 的 9 檔同一方法論、同量級
  雜訊來源；沿用 v2 已驗證的「data-quality sensitivity immaterial」結論，本次未重跑 raw vs adjusted
  的 CAGR/Sharpe 對照（v2 那次已建立 ≤0.04pp 的量級基準，時間成本考量下不重複驗證）。
- 未納入：研究階段（boundary study／Low-Vol／Persistence）用過的 rank 61-100 相關價格與成交量資料，
  依 scope boundary 不建立該層 infrastructure，不在本次交付範圍。

## 四、生命週期
v3 資料檔、54 檔新增價格、7 檔還原價與 sidecar：凍結，永不更新。update.py 每日清單另外處理
（見 production migration 說明），跟這份歷史檔案本身無關——歷史檔案不因為 production 開始用而改變。
