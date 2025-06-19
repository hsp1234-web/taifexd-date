# 數據整合平台 v15 - 智慧更新版

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/TaifexDataService/taifexd-data/blob/main/data_pipeline_v15/run_stable.ipynb)

**注意：** 我們建議使用 `data_pipeline_v15/run_stable.ipynb` 做為新的穩定版本執行入口。

本專案是一個為處理台灣期貨交易所 (TAIFEX) 每日交易數據而設計的自動化數據整合平台。此版本內建智慧更新與參數化執行功能。

## 使用教學

本專案的核心是提供一個可重複使用的 Google Colab 筆記本。

### 首次設定

1. 點擊上方的 [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/TaifexDataService/taifexd-data/blob/main/data_pipeline_v15/run_stable.ipynb) 徽章，開啟公版「樣板筆記本」。
2. 在 Colab 選單中，點擊 **「檔案」(File)** -> **「在雲端硬碟中儲存複本」(Save a copy in Drive)**。
3. 這會在您的 Google Drive 中建立一個專屬於您的「個人版筆記本」。**請關閉公版，並在您個人的版本上進行所有後續操作。**

### 日常執行

1. 從您的 Google Drive 開啟您的「個人版筆記本」。
2. 執行第一個儲存格，它會自動從 GitHub 下載最新程式碼、安裝精確的套件版本，並讓您設定本次執行的參數。
3. 依照筆記本中的引導完成操作即可。

## 預期 Google Drive 資料夾結構

本專案會在您的 Google Drive 根目錄下，尋找您所設定的專案資料夾（預設為 `MyTaifexDataProject`）。請確保您的資料夾結構如下：

MyTaifexDataProject/
│
├── Input/
│   ├── zip/
│   │   ├── TAIFEX_ABC.zip
│   │   └── TAIFEX_XYZ.zip
│   └── (unzip)/
│
└── Output/
    ├── database/
    │   └── processed_data.duckdb (此檔名可由參數控制)
    └── log/
        └── pipeline.log (此檔名可由參數控制)