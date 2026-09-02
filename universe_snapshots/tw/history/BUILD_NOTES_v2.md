# TW 電子 ranked 歷史母池 v2（depth 40）建置與 v1 修復記錄
建置日 2026-08-31（研究階段）；repo 整併 2026-09-02。Lab 產出；commit 屬 owner 決策。
本檔取代 2026-08-31 的 BUILD_NOTES_top40_extension.md（內容已併入）。v1 的 BUILD_NOTES.md 原文保留不動。

## 一、資料層 / 策略層契約（本次最重要的結構決定）
- Data layer：tw_ranked_universe_history_2007H2-2026H1_v2.json。38 個半年輪換點（2007-08-31 ~ 2026-02-26），
  每點保存 point-in-time 市值排名前 40 名（rank/code/name/industry/close/shares/mktcap）。
- Strategy layer：任何 Top-N 都是 snapshots[date]['top'][:N] 的截取，不另存獨立 Top-N 歷史檔。
  Top20（現行 production 定義）與 Top40（System Review Candidate）都從同一份資料切，deterministic。
- 「Top20」不再是歷史資料資產本身的邊界；深度 40 是本次資料層的邊界，>40 的 N 沒有資料。
- 日期語意：key = 排名 as-of 交易日（=shares_date）。依 production lag 慣例，該排名於 6 個月後（下一個半年點）生效。
  回測用法：排名 dates[i] 的生效期間 = [dates[i+1], dates[i+2])。

## 二、方法（與 v1、與 tw_universe_builder.py 凍結規則完全相同，唯一改動是排名深度 20→40）
- 母體：TWSE MI_INDEX 產業別歷史日表 type=24..31（電子八子產業），排除電信三雄 2412/3045/4904
- 股數：MI_QFIIS 歷史日表（當天無資料往前找最近交易日）
- 市值 = close × shares，同集合內排序取前 40
- 38 個交易日直接沿用 v1 已驗證的 shares_date，未重新掃描月底交易日
- 腳本：scripts/tw_hist_build_topN.py（產出原始 38 entry）→ scripts/tw_hist_wrap_v2.py（包 meta，不改資料）

## 三、驗證
- Golden match ×2：20260226 對 production universe_2026-08-31_v2_rotation 20/20；
  20250829 對 production universe_2026-02-28_v1_rotation 20/20（v1 歷史檔在此點是錯的，production 是對的）。
- 內部一致性掃描：全 38 期 × 40 名，找「前後輪換點皆在前 25 名、中間單一日期缺席」訊號，僅 1 筆——
  2408 南亞科 20140829。逐一查證（含全市場 ALLBUT0999 表）該日無成交紀錄，為減資重整停牌，真實事件，
  非抓取缺陷。列為 known residual，不補。
- 本檔案凍結，永不更新。未來輪換點由 production 流程產生。

## 四、v1 缺陷（tw_universe_history_2007H2-2026H1_v1.json，2026-07-08 建置）
38 期中 11 期有缺陷，機器可讀清單見 tw_universe_history_2007H2-2026H1_v1_KNOWN_DEFECTS.json。
- 10 期成員缺口：穩定大型股（鴻海 3 期、華碩、廣達、和碩、光寶科、研華、台達電、大立光、南電、國巨等）
  在該期單獨消失，鄰近兩期皆在列；空位由不該進榜的名字（旺宏、微星、景碩、大聯大、力成、旭隼等）頂上。
  最嚴重一期 20220225 錯 8 檔，20180227 錯 5 檔。
- 1 期價格錯誤（20190227）：20 檔成員相同，但 3481 群創 close 12.45（正確 10.25）、3008 大立光 4305（正確 4360），
  rank 15-18 順序錯。2026-09-02 以 FinMind 原始日價裁決，v2 正確。
- 根因：tw_hist_build.py 的 get() 只在整體回應為空時判失敗；MI_INDEX 某個產業代碼回傳不完整或錯行時被靜默接受。
  v2 建置腳本沿用同一 get()，但以第三節的內部一致性掃描與雙 golden match 補驗證；未來若再建置，建議加
  per-industry 表列數與相鄰期 union 規模的 sanity gate（本次未改腳本，保持腳本與產出的對應）。
- v1 從未進入 production：production 三份 rotation snapshot 皆由官方 builder 獨立產生，皆與 v2 一致。

## 五、價格資料（本次新增 34 檔，全部為 Top21-40 才出現的成員；v1 的 59 檔一律未動）
- 現存 25 檔：yfinance auto_adjust 還原價，全期。與 v1 不同處：v1 對其 25 檔有做 2010 前 TWSE 官方月底 splice，
  本次 25 檔未做。回測起點受 0055.TW（2008-01 上市）+ SMA12 暖機所限實際自 2010-01 起，2010 前段未被使用；
  若未來需要 pre-2010，這 25 檔的 2010 前段應視為未驗證。
- 已下市/私有化 9 檔（2384 勝華、2448 晶電、2475 華映、3682 亞太電、5264 鎧勝-KY、2315 神達、3514 昱晶、
  6286 立錡、8078 華寶）：tw/{code}.TW.csv 為還原價 = FinMind 原始收盤 × 自最近事件往回累乘的 factor
  （factor = after_price / before_price，FinMind TaiwanStockDividendResult，等價 TWT49U）。
  稽核 sidecar：tw/raw_frozen/{code}_raw.csv（原始收盤）、tw/raw_frozen/tw_adjustment_factors_v2.csv（76 筆事件）。
  抽查：晶電 2003-08-06 除權息，原始單日 -15.89% → 還原後 +2.85%。
- 資料品質敏感度（研究階段 Validation 1）：9 檔以原始價 vs 還原價各跑一次 Top25/30/40，
  CAGR 差 ≤0.04pp、Sharpe 差 ≤0.002，immaterial。
- 未納入 repo：研究階段抓取的成交量資料（tw_volume/）僅供流動性診斷，不進 repo。
- update.py 的 tickers_tw 不加任何歷史名字（與 v1 慣例相同；歷史成員為一次性抓取）。
- 日曆對齊（2026-09-02 發現）：TWSE 官方來源的 14 檔下市股序列（v1 的 2311/2325/3474/3009/3697 與本次 9 檔）含
  台股週六補交易日（如 2010-02-06、2012-02-04、2012-12-22），yfinance 來源沒有。任何把兩者放進同一個日頻矩陣的引擎，
  若以聯集當索引，補班日會讓 yfinance 股票變 NaN 而漏掉一天報酬（Top20 約 0.1-0.2pp CAGR）。修法：master 日曆 =
  yfinance 來源序列的交易日聯集，TWSE 來源的補班日報酬併入下一交易日；0050.TW.csv 缺 2026-08-28，不宜當 master。
  lab_kit_2026-08-31_tw_topn 三支腳本已套用。production tw_rs_engine 只載入現役 snapshot 成員，不含下市股，不受影響。
  2026-07-08 的十八年回測若曾持有 2311/2325/3474 期間亦有同一漏報酬，量級同上，不改結論。

## 六、下游 revalidation（v1 → v2，同規則 Top20，2010-01-29 ~ 2026-08-31，已套用 §五 日曆對齊）
- 全期指標：CAGR 11.96% → 11.90%；Vol 13.23% → 13.29%；Sharpe 0.921 → 0.913；MaxDD -27.44% 不變。
  NAV 路徑差最大 +5.0%/-1.3%，期末 +0.9%。逐年差 >1pp：2014 +2.3、2015 -1.6、2016 -1.2、2022 +2.2、2026 -4.5（v1-v2）。
- breadth：mean n_on 9.85 → 9.88；P(n_on≥19) 兩版皆 1/200 月；P(n_on=20) 0 → 1/200。
- 受影響分析：
  1. 2026-07-08 十八年 TW RS 回測（TOP20MTNM vs 0050、regime 保護數字）——CAGR 級距差 0.05pp；
     2022 年數字會下修約 2pp 但「電子主導下跌時濾網保護」方向不變；GFC 段無缺陷日期，數字不變。
     裁定：data corrected, conclusion unchanged。
  2. 2026-07-08 churn 統計（59 檔／平均存續 12.9 期／5 檔消失）——修正為 53 檔／14.3 期／5 檔消失。
     多出的 6 檔皆為誤頂進榜；鴻海修正為 38 期全勤。裁定：data corrected, conclusion unchanged（且更強）；
     往後引用改用 53 / 14.3。新檔 tw_universe_churn_v2.json 同時提供 top20 與 top40 兩層。
  3. 2026-08-30 融資結構 lab report（現制 90/95 vs 候選 85/90，213 個月）——兩制差異只在 n_on≥19 的月份，
     兩版皆 1/200；裁定：data corrected, conclusion unchanged。
  4. 2026-08-31 Top-N Size Ontology 研究（本 repo lab/lab_kit_2026-08-31_tw_topn）——自 Layer 1 起即用 v2，不受影響。
- 無任何分析需列 System Review Candidate。

## 七、生命週期
- v2 資料檔、9 檔下市股還原價與 sidecar、34 檔價格：凍結，永不更新。無新增定期維護項目。
- v1 資料檔與其 BUILD_NOTES.md、tw_universe_churn.json：原樣保留供 2026-07 ~ 2026-08 研究重現，狀態見 V1_STATUS.md。
- 新研究一律讀 v2。

## 八、解讀提醒（承 v1）
2007–2014 段來自不同市場結構（漲跌幅 7%、面板/DRAM 權值、高散戶比重），供方向性存活理解，不與 2015 後 metric
直接平均（P-30）。
