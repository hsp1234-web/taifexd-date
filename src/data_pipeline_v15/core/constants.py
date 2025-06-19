# -*- coding: utf-8 -*-
"""專案共享常數模組。

此模組定義了在整個 data_pipeline_v15 專案中可能重複使用的字串常數，
例如路徑名稱、字典鍵名、狀態字串等，以避免魔法字串並提高可維護性。
"""

# --- 路徑相關常數 ---
DIR_NAME_INPUT = "01_input_files"
DIR_NAME_STAGING = "02_staging_parquet"
DIR_NAME_DATABASE = "03_database_output"
DIR_NAME_MANIFESTS = "04_manifests"
DIR_NAME_FAILED = "05_failed_files"
DIR_NAME_LOGS = "99_logs"

# --- file_parser.py 返回字典鍵名 ---
KEY_STATUS = "status"
KEY_FILE = "file"
KEY_REASON = "reason"
KEY_TABLE = "table"
KEY_COUNT = "count"
KEY_PATH = "path"
KEY_RESULTS = "results"  # 用於 group_result

# --- file_parser.py 狀態值 ---
STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"
STATUS_GROUP_RESULT = "group_result"

# --- 執行模式與衝突策略 ---
RUN_MODE_NORMAL = "NORMAL"
RUN_MODE_BACKFILL = "BACKFILL"
CONFLICT_STRATEGY_REPLACE = "REPLACE"
CONFLICT_STRATEGY_IGNORE = "IGNORE"

# --- 其他通用字串 ---
# 可根據掃描結果在此處添加更多，例如 ".csv", ".zip", "utf-8" 等，如果它們在多處被硬式編碼。
# 目前僅包含計劃中明確列出的。
