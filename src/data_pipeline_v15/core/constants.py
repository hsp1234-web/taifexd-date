# -*- coding: utf-8 -*-

"""
核心常數模組

集中管理整個數據管線中使用的所有固定設定值，
例如資料夾名稱、特定檔案名稱等，以提高可維護性。
"""

# --- 核心資料夾結構 ---
# 這些變數定義了管線在專案資料夾中建立的標準目錄結構。

INPUT_DIR = "00_input"
"""存放待處理原始數據 (例如 ZIP 檔案) 的資料夾。"""

PROCESSED_DIR = "01_processed"
"""存放已成功處理並載入資料庫的原始檔案的備份資料夾。"""

ARCHIVE_DIR = "02_archive"
"""存放 manifest.json 檔案的地方，用於追蹤已處理的檔案紀錄。"""

QUARANTINE_DIR = "03_quarantine"
"""存放處理失敗、格式錯誤或無法解析的檔案的隔離區。"""

DB_DIR = "98_database"
"""存放最終輸出的 DuckDB 資料庫檔案的資料夾。"""

LOG_DIR = "99_logs"
"""存放所有執行日誌檔案的資料夾。"""


# --- 檔案處理相關 ---

MANIFEST_FILE = "manifest.json"
"""用來記錄每個檔案處理狀態的 JSON 檔案名稱。"""

# Keys for result dictionaries
KEY_STATUS = "status"
KEY_MESSAGE = "message" # Already present from previous subtask, kept for completeness
KEY_FILE = "file"
KEY_REASON = "reason"
KEY_TABLE = "table"
KEY_COUNT = "count"
KEY_PATH = "path"
KEY_RESULTS = "results"

# Status values
STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"
STATUS_GROUP_RESULT = "group_result"
