# 入 repo 前驗證附註(2026-07-08,Claude lab session)

kit 收入 repo 位置:lab/lab_kit_2026-07-07/(凍結、帶日期、不進 production 命名空間)。
setup 的設計不變:session 時把 data/tickers 複製進「本地工作副本」的 us/,不 commit 進 repo 的 us/。

## 驗證結果(對 2026-07-08 的 repo main)
- [PASS] universe_26y_monthly:Sharpe 1.109123、MaxDD -0.147698(2008-10-31)——bit-level 一致。
- [PASS] universe_real + CDE_XLV:Sharpe 1.2499、active 45/74——bit-level 一致。
- [FAIL] universe_13y baseline:Sharpe 0.954267 vs expected 0.986760(MaxDD 一致)。

## 13y 錨點失敗的定位(未結案,屬 kit 原 session 維護線)
對 07-06 22:17 UTC 的 repo vintage(SHA 4f56550c)重跑,得到**完全相同**的 0.954267——
排除 repo 資料漂移;差異在執行環境或 expected 值記錄時的窗口狀態。expected 的窗口字串
(→ 2026-07-01)與 26y(→ 2026-05-31)、real(→ 2026-06-30)的 clamp 形態不一致,建議
原 session 環境複查 13y expected 是否記錄自 clamp 修正前的一跑。MaxDD 完全一致代表
NAV 路徑主體相同,分歧集中於窗口邊緣或均值層。

## 順帶發現:clamp 防不住歷史改寫
real+CDE 對 07-06 vintage 反而 FAIL(1.2478),對 07-08 main PASS——expected 值釘在
07-07 的 repo 狀態,而 yfinance 在 07-06→07-07 之間改寫過六月段的歷史值。另實測 TLT
在 07-06→07-08 之間被改寫 185 天(2002~2019,量級 ±2e-6,月配息再調整的浮點雜訊)。
結論:窗口 clamp 只防「新增資料」,不防「歷史值改寫」;錨點驗證的失敗要先區分這兩種來源。
GLD/BTAL 同期未變。

## 26 年宇宙的既有限制(kit README 已載,此處僅索引)
proxy 對映(TLT→VUSTX、GLD→CEF、BTAL→BAB 合成、γ→TSMOM 月報酬)、月頻 ensemble
[3,6,12]、BTAL 合成銜接落差 0.7%。使用時依 P-30/P-40 語言解讀:方向性存活理解,
不與 production 日頻 metric 直接平均。
