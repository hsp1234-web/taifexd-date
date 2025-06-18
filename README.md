# 數據整合平台 v15

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/hsp1234-web/taifexd-date/blob/main/data_pipeline_v15/run_v15.ipynb)

本專案是一個為處理台灣期貨交易所 (TAIFEX) 每日交易數據而設計的自動化數據整合平台。其核心價值在於提供一個具備高度容錯能力與冪等性 (Idempotent) 的 ETL (Extract, Transform, Load) 流程，確保數據處理的穩定與一致性。

## 使用教學

為了讓所有使用者都能輕鬆執行，我們提供兩種最簡單的方式：

### (推薦) 方法一：點擊徽章一鍵開啟

1.  點擊本文件最上方的 [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/hsp1234-web/taifexd-date/blob/main/data_pipeline_v15/run_v15.ipynb) 徽章。
2.  您的瀏覽器將會自動開啟一個預設好的 Google Colab 筆記本。
3.  在筆記本中，點擊執行按鈕 ▶️，即可開始。

### 方法二：複製程式碼自行貼上

如果您習慣使用自己的 Colab 環境，請依照以下步驟操作：

1.  開啟一個新的 Google Colab 筆記本。
2.  完整複製下方的所有程式碼。
3.  將程式碼貼到筆記本的儲存格中，並點擊執行按鈕 ▶️。

```python
#@markdown ## 參數設定
#@markdown 請在下方輸入您的專案資料夾名稱，然後點擊 ▶️ 執行此儲存格。

project_folder = "MyTaifexDataProject" #@param {type:"string"}

#@markdown ---
#@markdown ## 執行主要程式
#@markdown 確認參數後，下方的程式碼會自動完成所有工作。

# --- 環境設定 ---
import warnings
import os
warnings.filterwarnings('ignore')
print(f"✅ 參數設定完成！專案資料夾將被設定為：{project_folder}")

# --- 掛載 Google Drive ---
try:
    from google.colab import drive
    print("\n⏳ 正在請求掛載 Google Drive...")
    drive.mount('/content/drive')
    print("✅ Google Drive 掛載成功！")
except ImportError:
    print("非 Colab 環境，跳過掛載 Drive。")

# --- 下載最新專案 ---
print("\n⏳ 正在從 GitHub 下載最新專案...")
!git clone -q https://github.com/hsp1234-web/taifexd-date.git
project_path = "/content/taifexd-date/data_pipeline_v15"
os.chdir(project_path)
print("✅ 專案下載完成！")

# --- 安裝相依套件 ---
print("\n⏳ 正在安裝必要的 Python 套件...")
!pip install -q pandas duckdb
print("✅ 套件安裝完成！")

# --- 執行數據管道主程式 ---
print("\n🚀 即將啟動數據整合平台...")
print("-" * 20)
!python main.py --project-folder-name={project_folder}
print("-" * 20)
print("🎉 執行完畢！")
```

## 預期 Google Drive 資料夾結構

本專案會在您的 Google Drive 根目錄下，尋找您所設定的專案資料夾（預設為 `MyTaifexDataProject`）。請確保您的資料夾結構如下：

```
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
    │   └── processed_data.duckdb
    └── log/
        └── pipeline.log
```
