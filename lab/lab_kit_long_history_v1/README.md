# lab_kit_long_history_v1 — IRG_LONG_HISTORY_RESEARCH_KIT_v1

`RESEARCH_ONLY` · `LAB_LONG_HISTORY` · `PROXY_CONTAINING` · `NOT_PRODUCTION_EQUIVALENT`

建立：2026-10-04（post-hard-close infrastructure capture）。這個目錄是 Research Harness `long_history()` 的 **runtime authority**：
`howardus-web/antifragile-data` 的 `lab/lab_kit_long_history_v1/`。這個目錄的 zip 只是可攜的交付／備份鏡像，不是 authority。

## 這是什麼

2026-10-04 那次 26 年長歷史研究（DBMF 26Y crisis extension）實際用過的凍結輸入序列，原樣收成一包，讓以後的長週期研究不用再重搭
BTAL／長債／黃金／managed futures 的 proxy 管線。它只保存「重建同一個最長歷史宇宙所需的最小凍結輸入」加上 provenance、identity manifest 與驗證工具。

## 這不是什麼

- 不是 production 資料。production price repo（`us/`、`tw/`、`matrices/`）、production state、執行路徑都不讀這個目錄；把它整個刪掉，production 照跑。
- 不是 production 等價回測。六個角色裡四個是 proxy。
- 不是 canonical 的日頻 RS 訊號。這個宇宙是月頻，分數是 6M／12M 的月頻翻譯（`MONTHLY_6M_12M_TRANSLATION`），不是 126／252 交易日那條實作。
- 不是重開研究的理由。2026-10 SIMPLIFIED architecture research 是 HARD CLOSED；「現在有更長的歷史可用」本身不是 reopen trigger。

## 角色與來源

| 經濟角色 | LONG 序列 | 檔案 | 身分 |
|---|---|---|---|
| US equity | QQQ | `data/tickers/QQQ_full.csv` | 真實 QQQ |
| long Treasury | VUSTX | `data/tickers/VUSTX_full.csv` | 長債角色的 proxy，**不是 TLT** |
| energy equity | XLE | `data/tickers/XLE_full.csv` | 真實 XLE |
| gold | CEF | `data/tickers/CEF.csv` | 黃金角色的 proxy，**不是 GLD**，含白銀曝險 |
| anti-beta | BTAL long proxy F1 或 S3 → 2011-10 起真實 BTAL | `data/derived/btal_extended_{F1,S3}_monthly_returns.csv` | 2011-10 之前是 proxy，之後是真實 BTAL（鏈接） |
| managed futures（Structural MF sleeve 的 LONG vehicle） | AQR TSMOM（全資產聚合，月報酬） | `data/aqr/tsmom_monthly.csv` | managed-futures **類別 proxy**，不是 DBMF 重建；PROD vehicle 是 DBMF、MEDIUM 替代品是 AQMIX |

`data/derived/btal_proxy_monthly_returns.csv` 帶著 2011-10 起的真實 BTAL 月報酬（`BTAL` 欄），只用來驗 splice；loader 不讀其他欄。

價格檔是日頻調整後收盤；月價＝每月最後一筆觀測，月報酬＝其 `pct_change`。TSMOM 與 BTAL 檔本身就是月報酬。不內插、不回填；共同支撐區間內缺月會直接報錯。支撐區間由資料推導：以這份凍結資料是 1999-04..2026-05（326 個報酬月），第一個完整 12M 訊號月 2000-03，持有月 2000-04..2026-05，共 314 個月。

## Structural MF：sleeve 與 vehicle 分開講

「Structural MF」是 US sleeve 裡的一個經濟 sleeve／結構性資本席位，權重 15% 是 **sleeve 層級**的架構常數，不是「DBMF 的權重」。
填這個席位的東西依歷史模式不同：PROD 是 DBMF（production vehicle）、MEDIUM 是 AQMIX（研究替代品）、LONG 是這個 kit 的 AQR TSMOM（類別 proxy）。
證據跟著 vehicle 的等級走——TSMOM 或 AQMIX 的結果永遠不能當 DBMF 的 production 等價證據引用。換 vehicle 本身不等於改 sleeve 架構；但 production 的 vehicle 更換仍要走正式 production governance。
Harness 的每個 LONG `US_ENGINE` receipt 都帶 `structural_mf_sleeve` 區塊把這些明示出來。

## F1 與 S3

兩個都是 primary。沒有預設值、不平均、不選贏家，呼叫時必須明講 `btal_proxy_variant="F1"` 或 `"S3"`。

- **F1**（fit-informed）：`rf + a + 0.7176·BAB − 0.6165·(Mkt−RF)`，係數在 2011-10..2026-04 的真實 BTAL 上擬合一次後凍結。
- **S3**（construction-informed）：`rf + mean_{ME3,ME4,ME5}(LoBeta − HiBeta)`，French size × beta 等權格子，沒有擬合參數。

> F1 與 S3 只差在 2011-10 之前的 BTAL 那一腿。它們是兩種 proxy 建構，不是兩個獨立的歷史宇宙；兩者一致不算獨立複製。

Hybrid（MIX2）與 BAB 延伸序列不是 canonical variant，檔案不在這個 kit 裡。

## Runtime 規則

Harness 的例行 LONG 路徑**原樣消費**這些凍結序列。不重新擬合 F1、不重建 S3、不下載、不修補。
`provenance/proxy_construction_AUDIT_ONLY/build_proxy.py` 是當初建 proxy 的原始碼，只留作 provenance／audit；Harness 不 import、不執行它，它依賴的 French／BAB 原始檔也不在這個 kit 裡。

## 檔案

| 路徑 | 類別 | 用途 |
|---|---|---|
| `KIT_MANIFEST.json` | — | identity manifest：每檔 SHA-256、payload identity、角色對照、provenance |
| `verify_kit.py` | tool | 獨立驗證（只用 stdlib）：逐檔 hash、payload identity、splice、推導支撐區間 |
| `data/**` | data | 八個凍結輸入檔，與 26Y 研究的 `input_identity.json` 逐檔 hash 相同 |
| `provenance/input_identity_26Y.json` | provenance | 26Y 研究當時的來源身分記錄（原檔） |
| `provenance/proxy_spec.json` | provenance | F1／S3 的定義與 live-fit 診斷（原檔） |
| `provenance/primary_freeze_sha256.txt`、`slot_weights_frozen.json` | provenance | 26Y 研究的凍結雜湊與 slot weights 記錄 |
| `provenance/proxy_construction_AUDIT_ONLY/build_proxy.py` | provenance | proxy 建構原始碼，audit only |
| `reference/lab26.py` | reference | 26Y 研究當時的獨立 adapter（Harness 當時不支援這個宇宙）；現在只作 parity 對照 |
| `reference/full26_{f1,s3}_monthly.csv`、`primary_results_frozen.json` | reference | 26Y 研究凍結的逐月結果，用來驗 Harness LONG `RS_LONG` 與它逐點相同 |

## 用法

```bash
python3 lab/lab_kit_long_history_v1/verify_kit.py          # 驗 kit
```

```python
import research_harness as rh                                # MAS corpus，Harness v2.2 起
r = rh.long_history("RS_LONG",   repo_root="<antifragile-data clone>", btal_proxy_variant="F1")
u = rh.long_history("US_ENGINE", repo_root="<antifragile-data clone>", btal_proxy_variant="S3")
r.receipt["evidence_class"]          # "LAB_LONG_HISTORY"
r.receipt["production_equivalent"]   # False
```

`RS_LONG`＝六角色動態排名，套 canonical `SLOT_WEIGHTS`（26Y 研究 BENCH 那條路徑，與 `reference/` 逐點相同）。
`US_ENGINE`＝Fixed-15 的長歷史類比：MF proxy 固定 15%、不排名，其餘五個角色依同一個分數排名、拿 0.85·G。`FULL`、`TW_RS` 不支援（沒有長歷史的 TW／家庭機器，不合成）。

## 來源與身分

- 價格與 TSMOM：本 repo `lab/lab_kit_2026-07-07/data/`（yfinance 2026-07-06 全史版、AQR Datasets），逐檔位元組複製。
- BTAL F1／S3：`BTAL_LONG_PROXY_LAB_2026-10-04.zip`（SHA-256 `6f6c1f79a6f3baaa2ff51ba8c8115b1b17b1d4fcca14bd21d63a78c15fda746b`），逐檔位元組複製。
- 26Y 研究 artifact：`DBMF_26Y_CRISIS_EXTENSION_ARTIFACTS_2026-10-04.zip`（SHA 見 manifest）。
- payload identity（八個 data 檔的檔名＋hash 清單的 SHA-256）登記在 MAS corpus 的 `long_history_universe.REGISTERED_KITS["v1"]`；
  loader 遇到未登記的 identity 會拒絕。要延長或更換資料＝出新版 kit＋在程式裡登記，不是覆寫這個目錄。

## 已知限制（沿用 26Y 研究）

AQR TSMOM 不是 DBMF（重疊期月相關約 0.61），而且是不扣費用的假設性序列；F1／S3 在 2008 的 BTAL 名次不同，會改變 GFC 段的結果方向；
VUSTX、CEF 與它們代表的角色不是同一個標的。任何用這個 kit 得到的結果都是 `LAB_LONG_HISTORY`，不能當成 PROD／MEDIUM 證據引用。
