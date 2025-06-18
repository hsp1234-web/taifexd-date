# -*- coding: utf-8 -*-
import os
import shutil
import traceback
from typing import TYPE_CHECKING

from .utils.logger import Logger

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

# Note: os, shutil, traceback are already imported above and should not be duplicated.
# The original script in A029 was intended to organize this, but resulted in duplicates.
# This cleanup removes the duplicates and the old commented-out type hints.


def load_staging_to_database(
    db_conn,  # type: DuckDBPyConnection (actual type hint if using TYPE_CHECKING)
    schemas_config: dict,
    local_staging_path: str,
    logger,  # type: Logger (actual type hint if using TYPE_CHECKING)
    conflict_strategy: str,
) -> None:
    """將指定暫存區 (staging) 的 Parquet 檔案載入到 DuckDB 資料庫中。

    此函式會掃描 `local_staging_path` 下的子目錄，每個子目錄被視為一個獨立的綱要 (schema)。
    它會讀取每個綱要子目錄中的所有 `.parquet` 檔案，並根據 `schemas_config` 中對應的綱要定義，
    將數據插入（或更新）到由 `db_table_name` 指定的資料庫表格中。
    載入過程中會自動處理主鍵 (`id`) 的序列生成。所有操作完成後，暫存區將被清空。

    :param db_conn: 已建立的 DuckDB 資料庫連線物件。
    :type db_conn: duckdb.DuckDBPyConnection
    :param schemas_config: 包含綱要定義的字典。
                           鍵為綱要名稱 (應與 `local_staging_path` 下的子目錄名對應)，
                           值為該綱要的詳細設定，至少需包含 `db_table_name` (資料庫表格名)
                           和 `columns_map` (欄位對映)。若 `conflict_strategy` 為 'REPLACE'，
                           則還需包含 `unique_key` (唯一鍵列表)。
    :type schemas_config: dict
    :param local_staging_path: 暫存區的根路徑，其中包含以綱要命名的子目錄，
                               子目錄內存放待載入的 Parquet 檔案。
    :type local_staging_path: str
    :param logger: 用於記錄日誌訊息的 Logger 物件執行個體。
    :type logger: Logger
    :param conflict_strategy: 數據衝突時的處理策略。可選值：
                              - 'REPLACE': 如果數據已存在（根據 `unique_key` 判斷），則更新現有記錄。
                                         若無 `unique_key`，則行為類似 'IGNORE'。
                              - 'IGNORE': 如果數據已存在，則忽略新數據（不執行任何操作）。
    :type conflict_strategy: str
    :raises Exception: 若在資料庫操作或檔案系統操作（如清理暫存區）時發生未預期的錯誤。
                      具體的錯誤會被記錄到 logger。
    """
    logger.log("階段二 (載入): 開始將暫存數據載入資料庫...", level="info")

    if not os.path.exists(local_staging_path) or not os.path.isdir(local_staging_path):
        logger.log(
            f"暫存區路徑 '{local_staging_path}' 不存在或不是一個目錄，跳過載入。",
            level="warning",
        )
        return

    processed_schemas = 0
    for schema_name in os.listdir(local_staging_path):
        schema_staging_path = os.path.join(local_staging_path, schema_name)
        if not os.path.isdir(schema_staging_path):
            continue

        schema_def = schemas_config.get(schema_name)
        if not schema_def:
            logger.log(
                f"在 schemas_config 中找不到名為 '{schema_name}' 的綱要定義，跳過此目錄。",
                level="warning",
            )
            continue

        table_name = schema_def.get("db_table_name")
        if not table_name:
            logger.log(
                f"綱要 '{schema_name}' 中未定義 'db_table_name'，跳過。",
                level="warning",
            )
            continue

        # Ensure table_name is quoted to handle potential special characters or case sensitivity in SQL
        quoted_table_name = f'"{table_name}"'

        parquet_files = [
            f for f in os.listdir(schema_staging_path) if f.lower().endswith(".parquet")
        ]
        if not parquet_files:
            logger.log(
                f"綱要 '{schema_name}' 的暫存目錄 '{schema_staging_path}' 中沒有 Parquet 檔案，跳過載入。",
                level="info",
            )
            continue

        logger.log(
            f"正在載入資料至 {quoted_table_name} 表格 (綱要: {schema_name}, 策略: {conflict_strategy})...",
            level="substep",
        )

        parquet_glob_path = os.path.join(schema_staging_path, "*.parquet").replace(
            "\\", "/"
        )  # Ensure forward slashes for DuckDB path

        try:
            # Ensure column names from schema_def["columns_map"] are quoted
            db_cols_list = [f'"{c}"' for c in schema_def["columns_map"].keys()]

            # Columns for the SELECT part from read_parquet (must match Parquet file columns)
            # These are already defined by schema_def["columns_map"].keys()
            select_cols_from_parquet_str = ", ".join(db_cols_list)

            # Columns for the INSERT INTO part (includes "id" + columns from Parquet)
            insert_cols_str = f'"id", {select_cols_from_parquet_str}'

            # Sequence name, also quoted
            quoted_seq_name = f'"seq_{table_name}"'
            db_conn.execute(f"CREATE SEQUENCE IF NOT EXISTS {quoted_seq_name};")

            conflict_clause = "ON CONFLICT DO NOTHING"  # Default for "IGNORE"
            if conflict_strategy.upper() == "REPLACE" and schema_def.get("unique_key"):
                unique_key_list = [f'"{k}"' for k in schema_def["unique_key"]]
                unique_key_str = ", ".join(unique_key_list)

                update_set_list = [
                    f"{col_name}=excluded.{col_name}" for col_name in db_cols_list
                ]
                update_set_str = ", ".join(update_set_list)
                conflict_clause = (
                    f"ON CONFLICT ({unique_key_str}) DO UPDATE SET {update_set_str}"
                )

            # The Parquet files are expected to have columns matching schema_def["columns_map"].keys()
            # due to the reindex operation in the file_parser step.
            # The SELECT clause for read_parquet should list these columns.
            sql_query = (
                f"INSERT INTO {quoted_table_name} ({insert_cols_str}) "
                f"SELECT nextval({quoted_seq_name}), {select_cols_from_parquet_str} "
                f"FROM read_parquet('{parquet_glob_path}', filename=false, hive_partitioning=false) "  # Added options for robustness
                f"{conflict_clause};"
            )

            db_conn.execute(sql_query)
            logger.log(
                f"{quoted_table_name} 表格 (綱要: {schema_name}) 載入完成。",
                level="success",
            )
            processed_schemas += 1
        except Exception as e:
            logger.log(
                f"載入 {quoted_table_name} 表格 (綱要: {schema_name}) 時發生嚴重錯誤: {e}",
                level="error",
            )
            logger.log(traceback.format_exc(), level="error")

    if processed_schemas > 0:
        logger.log(f"所有可處理的綱要資料均已載入資料庫。", level="info")
    else:
        logger.log(
            f"在暫存區 '{local_staging_path}' 中沒有找到可載入的資料。", level="info"
        )

    try:
        if os.path.exists(local_staging_path):
            shutil.rmtree(local_staging_path)
            # No need to os.makedirs(local_staging_path) if the expectation is just to clear it.
            # If it needs to exist for subsequent runs in the same overall script execution, then recreate.
            # Based on typical ETL, clearing is often enough. Let's assume it should be recreated for safety.
            os.makedirs(local_staging_path, exist_ok=True)
            logger.log(f"暫存區 '{local_staging_path}' 已清理並重建。", level="success")
    except Exception as e:
        logger.log(f"清理暫存區 '{local_staging_path}' 時發生錯誤: {e}", level="error")
