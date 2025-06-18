[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/hsp1234-web/taifexd-date/blob/main/data_pipeline_v15/run_v15.ipynb)

# 台灣期貨交易所每日交易行情資料整合專案 (v15)

這是一個專業級的數據整合平台，旨在自動化下載、解析、並儲存台灣期貨交易所 (TAIFEX) 的每日交易行情資料。本專案從 v13 的單一腳本演進至 v15 的模組化應用程式，具備高穩定性、可維護性與易用性。

## 核心功能

* **容錯式綱要適應**：智慧處理不同時期的欄位差異，自動補齊缺失欄位，確保資料一致性。
* **冪等與可續行設計**：透過 Manifest 檔案追蹤已處理的檔案，即使中斷也能從上次的進度繼續，避免重複處理。
* **本地優先工作流程**：所有檔案下載至本地端（或 Colab 環境）後再進行處理，大幅提升效率。
* **模組化架構**：將資料提取 (Extract)、轉換 (Transform)、載入 (Load) 的各個階段拆分為獨立模組，易於維護與擴展。
* **多重執行入口**：同時提供專業開發者使用的命令列介面 (CLI) 和適合普通使用者的 Colab 一鍵執行腳本。

## 專案結構

```
MyTaifexDataProject/
├── input/         # 放置從期交所下載的原始 .zip 檔案
├── output/        # 存放最終產出的 taifex_data.duckdb 資料庫檔案
├── archive/       # 成功處理完的 .zip 檔案會被移至此處封存
├── manifest/      # 存放 manifest.json 狀態紀錄檔
└── logs/          # 存放執行的日誌檔案
```

## 如何執行 (Google Colab)

#### 方法一：使用專案筆記本

1.  點擊本文件最上方的 "Open in Colab" 徽章。
2.  在開啟的 Colab 筆記本中，依照儲存格的指示執行即可。

#### (推薦) 方法二：一鍵複製執行
如果您希望在一個全新的 Colab 環境中快速執行，請直接複製以下所有程式碼，並貼到 Colab 的儲存格中執行。它會自動完成所有環境設定與數據處理。

```python
# ====================================================================
# 數據整合平台 v15 - Colab 快速啟動腳本
# ====================================================================
#
# 使用說明：
# 1. 開啟一個新的 Google Colab 筆記本。
# 2. 將此儲存格中的所有程式碼複製並貼上。
# 3. 點擊「執行」按鈕。
# 4. 依照提示完成 Google Drive 的授權。
#
# 注意：
# - 腳本將會在您的 Google Drive 根目錄下建立一個名為 'MyTaifexDataProject' 的資料夾。
# - 請將所有要處理的 .zip 檔案上傳到 'MyTaifexDataProject/input/' 資料夾中。

from google.colab import drive
import os

# --- 步驟 1: 掛載 Google Drive ---
# 這裡會彈出一個視窗，要求您授權 Colab 存取您的 Google Drive
print("正在掛載 Google Drive...")
drive.mount('/content/drive')
print("✅ Google Drive 掛載成功！")

# --- 步驟 2: 下載最新的專案程式碼 ---
# 我們會從 GitHub 上複製整個專案，確保所有程式碼都是最新版本
print("
正在從 GitHub 下載最新版本的程式碼...")
os.chdir('/content/') # 切換到工作目錄
if os.path.exists('taifexd-date'):
    # 如果目錄已存在，先刪除舊的，確保是全新的下載
    print("偵測到舊有專案目錄，正在移除...")
    !rm -rf taifexd-date
!git clone https://github.com/hsp1234-web/taifexd-date.git
print("✅ 專案程式碼下載完成！")

# --- 步驟 3: 安裝必要的相依套件 ---
# 執行資料管道所需的核心 Python 套件
print("
正在安裝相依套件...")
!pip install pandas psutil pyarrow duckdb -q
print("✅ 相依套件安裝完成！")

# --- 步驟 4: 執行數據整合管道 ---
# 這是整個流程的核心，將會開始處理您放在 input 資料夾中的檔案
print("
🚀 即將啟動數據整合管道...")
print("="*50)
# 切換到 v15 腳本所在的目錄
os.chdir('/content/taifexd-date/data_pipeline_v15')
# 執行主程式，並指定 Google Drive 中的專案資料夾名稱
!python main.py --project_name MyTaifexDataProject

print("
🎉 全部流程執行完畢！請至 Google Drive 的 'MyTaifexDataProject/output' 資料夾查看結果。")
```
