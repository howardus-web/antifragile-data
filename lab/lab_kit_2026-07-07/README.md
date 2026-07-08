# lab_kit — 實驗宇宙重建包

建立日期：2026-07-06/07（CDE 研究線 session）
用途：把該次 session 建立的三個實驗宇宙打包成可重複使用的環境。以後任何 lab 對話，
下載 production repo + 這個 kit，跑一次 setup + verify，就回到同一個實驗環境，
不用重新下載 AQR 資料、重新合成 BTAL 延伸序列、重新踩一次所有坑。

## 快速上手（新 session 三步）

```bash
# 1. 下載 production repo（跟平常一樣）
curl -sL "https://github.com/howardus-web/antifragile-data/archive/refs/heads/main.zip" -o repo.zip && unzip -q repo.zip

# 2. setup：把 kit 的 session-added tickers 灌進 repo/us/
python3 lab_kit/build_universes.py --setup --repo ./antifragile-data-main

# 3. verify：重算三個錨點，確認環境跟 2026-07-06 session 一致
python3 lab_kit/build_universes.py --verify --repo ./antifragile-data-main
```

之後在腳本裡 `from build_universes import *`，三個宇宙函式直接用。

## 三個宇宙

| | universe_real | universe_13y | universe_26y_monthly |
|---|---|---|---|
| 六味 | 全真實（含 DBMF） | AQMIX 代 DBMF | QQQ/XLE 真實；TLT→VUSTX、GLD→CEF、BTAL→BAB 延伸、γ→TSMOM |
| 頻率 | 日頻 | 日頻 | 月頻 |
| IV ensemble | [60,120,252] 交易日 | [60,120,252] 交易日 | [3,6,12] 個月 |
| 窗口 | ~2019-05 起（DBMF 上市日卡） | 2012-10 起（AQMIX+252 天暖機） | 1999-04 起（QQQ 上市日卡） |
| 涵蓋 2008 | 否 | 否 | **是**（P-40 的意義所在） |
| 用途 | 離 production 最近的驗證 | 標準沙盒（多數玩具用這個） | 長歷史穩健性 / 危機樣本 |

## 資料清單與來源

**data/aqr/**（外部下載，2026-07-06 抓取）
- `bab_usa_daily.csv` — AQR Betting Against Beta 美股日頻因子，1930-12-01 → 2026-04-30。
  來源：aqr.com Datasets「Betting Against Beta: Equity Factors, Daily」xlsx，USA 欄（第 24 欄）。
- `bab_usa_monthly.csv` — 同上月頻版，1930-12 → 2026-04。
- `tsmom_monthly.csv` — AQR Time Series Momentum 全資產聚合月報酬，1985-01 → 2026-05。
  來源：aqr.com Datasets「Time Series Momentum: Factors, Monthly」。**月報酬序列，不是價格；
  只能用在月頻宇宙，不能轉日頻**（月轉日會抹平月內波動，直接汙染 IV 的 vol 計算）。

**data/derived/**
- `btal_extended.csv` — 合成 BTAL：以真實 BTAL 起點（2011-09-13，收盤 21.0933）為錨，
  用 BAB 日報酬往回推算到 1930-12-01，再接上真實 BTAL 至今。銜接點落差 0.7%
  （合成端 21.2495 vs 真實 21.0933），對 80 年因子代理屬正常量級。
- `ftse100_gbp_extended_daily.csv` — FTSE 100 日頻 GBP 延伸鏈，1984-01-03 →
  現在（42.5 年）。拼接點 2010-09-16：之前是 `^FTSE` 純價格指數，之後接 `CUKX.L`
  （iShares FTSE 100 UCITS，累積型，真含息）。前段沒有股息，隱含年化低估約
  3.5%（FTSE 100 歷史殖利率量級）——訊號、相對強弱、regime 切分這類不依賴絕對
  報酬水位的用法不受影響；CAGR/Sharpe 這種吃絕對水位的指標，跨越拼接點比較
  會失真，要嘛只用 2010-09 後的乾淨段，要嘛在前段補一個殖利率估計值。
- `uk_equity_monthly_price_1958.csv` — 英國股市月頻純價格延伸鏈，1957-12 →
  現在（68.6 年）。拼接點 1984-01：之前是 FRED 的 OECD 英國全股價指數
  （`SPASTT01GBM661N`），之後接 `^FTSE` 月頻。涵蓋 1973-74 英股腰斬、1987、
  1992 ERM 危機，是這個 kit 目前最長的歐洲股市危機樣本。**前段（1984 前）是
  月均價不是月底價**——跟 `^FTSE` 重疊期月報酬相關只有 0.68（其餘三條拼接鏈
  的重疊期驗證都在 0.98 以上），月均取樣把波動壓低約兩成，這段不能用於任何
  對波動或回撤深度敏感的計算，只能看趨勢與大級別 regime。

**data/tickers/**（yfinance 抓取，2026-07-06，auto_adjust=True）
- `*_full.csv`：QQQ、SPY、XLE、XLP、XLV、XLU、XLF、XLI、EEM、ITA、USMV、SPLV、VUSTX
  的完整上市歷史（repo 內同名檔案窗口較短，這批是全史版）。
- `AQMIX.csv`（2010-01-05 起）、`CEF.csv`（金+銀信託，1986 起）、`GC=F.csv`（黃金期貨）、
  `vix_daily.csv`（^VIX，1990 起）。
- `EWU_full.csv` — iShares MSCI UK（1996-03 起，真含息，**USD 計價**）。不是 FTSE 100
  本尊，是 MSCI UK，成分高度重疊但不完全一樣。USD 計價這件事對這個標的特別要緊：
  FTSE 100 成分股約 75% 營收在英國以外，英鎊貶值時這些公司的海外獲利換算成英鎊
  反而增加，指數常常在英鎊崩的時候上漲（2016 脫歐公投是教科書案例）——所以 EWU
  這條 USD 曝險，跟用 `ftse100_gbp_extended_daily.csv` 那條 GBP 曝險，是兩種不同性
  格的資產，不是同一個標的換個計價單位而已，用哪條要先想清楚是要測「英國股市」
  還是「美元投資人眼中的英國曝險」。
- `fred_gbpusd.csv` — 英鎊兌美元日頻匯率，1971-01 起（FRED, `DEXUSUK`）。GBP/USD
  兩種曝險視角互換用。

## 陷阱清單（每一條都是這次 session 真的踩過的）

1. **假月底**：資料尾端不完整月份（例如只有 7/1 一個交易日）的最後交易日，會被
   month_end_mask 誤判成該月月底。曾造成「最新一期配置」抓錯訊號日，RS-Long 的
   QQQ/XLE 排名整個對調。重要計算前先 `clamp_to_complete_month`。
2. **兩種月頻訊號不等價**：「月底取樣後算 MA3/MA6」跟「日頻算 MA60/120 再讀月底值」
   不是同一條算法，active frequency 差 6.76pp（67.6% vs 60.8%）。**production CDE
   規則是後者**（`cde_signal_daily`），前者已作廢（Graveyard #59）。
3. **CEF ≠ 純金**：金+銀信託，白銀 beta 會混進 π_R proxy。
4. **VUSTX ≠ TLT**：共同基金，duration 與費用結構跟 TLT 有差。
5. **TSMOM 頻率鎖死**：見上，月頻 only。
6. **AQR 資料尾端**：BAB 到 2026-04、TSMOM 到 2026-05，比價格資料短兩三個月，
   月頻宇宙的共同窗尾端由它們決定。要更新需重新下載 AQR xlsx（連結可能變動）。
7. **repo 每日 CI 更新**：錨點驗證已把窗口 clamp 死（13.7y→2026-07-01、
   26y→2026-05-31、real→2026-06-30），repo 新增資料不影響 verify 結果。
8. **LSE 分配型 ETF 常是假 TR**：`ISF.L`（iShares FTSE 100，分配型）配息紀錄
   只有 54 筆、稀稀落落，直接拿它的價格序列當 total return 用會低估報酬——
   歐洲 LSE 上市的 ETF 分「distributing」（配息，價格序列不含息）跟
   「accumulating」（累積，股息滾進淨值，價格序列才是真 TR）兩種股別，名稱
   常常長得很像（`ISF.L` vs `CUKX.L`），抓資料前務必用 `yfinance` 的
   `.dividends` 檢查配息紀錄是否稀疏到不合理，稀疏就是假 TR 的訊號。
   IUHC（CDE_XLV 執行層 vehicle）同樣是 LSE 上市，未來若要驗證它的價格序列
   是不是真 TR，用同一招檢查。
9. **FRED 國際股價指數常是月均價不是月底價**：`SPASTT01GBM661N` 這類 OECD
   月頻股價指數，經比對驗證是月內均價，不是月底收盤——月均取樣會人工壓低
   波動、扭曲任何依賴月底快照的訊號讀值。用 FRED 系列前，先跟同期一條有把
   握的月底序列比對月報酬相關係數；相關掉到 0.9 以下就要懷疑取樣方式不是
   月底值。

## 驗證錨點（expected_metrics.json）

| 宇宙 | Sharpe | MaxDD | 備註 |
|---|---|---|---|
| 13.7y IV baseline | 0.986760 | −9.26% @2016-12-01 | 六味 AQMIX 版 |
| 26y monthly IV baseline | 1.109123 | −14.77% @2008-10-31 | 吃到金融海嘯 |
| real + CDE_XLV | 1.2499 | −6.7152% @2020-10-30 | production reconciliation reference，active 45/74 |

2026-07-07 首次打包時三個錨點全數通過（Sharpe 至小數第六位一致）。

## 存放建議

主推：production repo 開 `lab/` 子資料夾放整個 kit——每次 session 本來就會下載 repo，
kit 跟著一起到手，零額外步驟，且有 git 版本控制。次選：Project files（上傳 zip，
session 內解壓）。不建議放 Drive A 類資料夾——那裡是 production 狀態檔的家，
混入 lab 工具會模糊 A 類的定義。

## 這個 kit 不包含什麼

回測結果、報告 HTML、假設矩陣的 JSON——這些重算成本幾秒，不值得佔空間。
CDE production reference 三件套（signal schedule / walkforward / metrics）已交付
production 工程線，不在 kit 範圍。研究結論查 decision_graveyard 與 insight_archive，
kit 只管環境，不管結論。
