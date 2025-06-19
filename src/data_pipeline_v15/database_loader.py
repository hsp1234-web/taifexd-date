# -*- coding: utf-8 -*-

"""
資料庫載入模組

此模組負責將處理完成的 DataFrame 數據，高效地載入至 DuckDB 資料庫中。
"""

import logging
import json # Added for loading schemas
import os # Added for os.path.exists
from typing import TYPE_CHECKING

import duckdb
# pandas is no longer needed in this file directly for load_data
# import pandas as pd

if TYPE_CHECKING:
    from logging import Logger


class DatabaseLoader:
    """
    負責與 DuckDB 互動，並載入處理後的數據。
    """

    def __init__(self, database_file: str, logger: "Logger"):
        """
        初始化 DatabaseLoader。

        Args:
            database_file (str): DuckDB 資料庫檔案的完整路徑。
            logger (Logger): 用於記錄日誌的 Logger 物件。
        """
        self.database_file = database_file
        self.logger = logger
        self.connection = None
        self.allowed_table_names = set()
        self._load_allowed_table_names()
        self._connect()

    def _load_allowed_table_names(self):
        """
        Loads allowed table names from the schemas.json configuration file.
        """
        # Path to schemas.json, assuming it's relative to the project root or a known config path
        # Adjust the path as necessary based on project structure.
        # For this example, assuming it's in a 'config' directory sibling to 'src'
        # and the script runs from a context where this relative path is valid.
        # A more robust solution might involve passing the config path or using absolute paths.
        schemas_file_path = "config/schemas.json"
        try:
            self.logger.info(f"Loading allowed table names from '{schemas_file_path}'...")
            with open(schemas_file_path, "r", encoding="utf-8") as f:
                schemas_data = json.load(f)

            for schema_key, schema_config in schemas_data.items():
                if isinstance(schema_config, dict):
                    table_name = schema_config.get("db_table_name")
                    if table_name:
                        self.allowed_table_names.add(table_name)
                    else:
                        self.logger.warning(
                            f"Schema '{schema_key}' in '{schemas_file_path}' is missing 'db_table_name'."
                        )
                else:
                    self.logger.warning(
                        f"Schema entry for '{schema_key}' in '{schemas_file_path}' is not a valid dictionary."
                    )

            if self.allowed_table_names:
                self.logger.info(
                    f"Successfully loaded {len(self.allowed_table_names)} allowed table names: {self.allowed_table_names}"
                )
            else:
                self.logger.warning(
                    f"No allowed table names were loaded from '{schemas_file_path}'. "
                    "Ensure the file exists, is valid JSON, and contains 'db_table_name' entries."
                )

        except FileNotFoundError:
            self.logger.error(
                f"Schemas file '{schemas_file_path}' not found. No table names will be allowed."
            )
            self.allowed_table_names = set()
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Error decoding JSON from '{schemas_file_path}': {e}. No table names will be allowed."
            )
            self.allowed_table_names = set()
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred while loading allowed table names from '{schemas_file_path}': {e}",
                exc_info=True
            )
            self.allowed_table_names = set()


    def _connect(self):
        """
        建立與 DuckDB 資料庫的連線。
        """
        try:
            self.logger.info(f"正在連線至 DuckDB 資料庫：{self.database_file}")
            self.connection = duckdb.connect(database=self.database_file, read_only=False)
            self.logger.info("✅ DuckDB 資料庫連線成功。")
        except Exception as e:
            self.logger.critical(f"❌ 無法連線至 DuckDB 資料庫！錯誤：{e}", exc_info=True)
            raise

    def load_parquet(self, table_name: str, parquet_file_path: str):
        """
        將 Parquet 檔案的數據載入到 DuckDB 的指定資料表中。

        Args:
            table_name (str): 目標資料表的名稱。
            parquet_file_path (str): Parquet 檔案的路徑。
        """
        if self.connection is None:
            self.logger.error("資料庫連線不存在，無法載入 Parquet 資料。")
            return

        if table_name not in self.allowed_table_names:
            self.logger.error(
                f"Table name '{table_name}' is not in the allowed list of table names. "
                f"Allowed names: {self.allowed_table_names}. Aborting load operation for '{parquet_file_path}'."
            )
            return

        if not os.path.exists(parquet_file_path):
            self.logger.error(
                f"Parquet file '{parquet_file_path}' does not exist. Aborting load operation for table '{table_name}'."
            )
            return

        self.logger.info(f"準備將 Parquet 檔案 '{parquet_file_path}' 載入至資料表 '{table_name}'...")

        try:
            # Create table if it doesn't exist based on Parquet schema
            # Using f-string directly with table_name and parquet_file_path is safe here
            # as these are validated/system-generated, not direct user SQL input.
            # DuckDB handles quoting for read_parquet path internally.
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" AS
            SELECT * FROM read_parquet('{parquet_file_path}') LIMIT 0;
            """
            self.connection.execute(create_table_sql)
            self.logger.info(f"資料表 '{table_name}' 結構已確認/建立。")

            # Insert data from Parquet file
            insert_sql = f"""
            INSERT INTO "{table_name}"
            SELECT * FROM read_parquet('{parquet_file_path}');
            """
            self.connection.execute(insert_sql)

            # Get the count of rows loaded
            # This approach re-reads the parquet file for count, alternative is to see if INSERT returns row count.
            # For simplicity and accuracy of what's in the file:
            count_query = f"SELECT COUNT(*) FROM read_parquet('{parquet_file_path}')"
            loaded_count = self.connection.execute(count_query).fetchone()[0]

            self.logger.info(f"✅ 成功將 {loaded_count} 筆資料從 '{parquet_file_path}' 載入至資料表 '{table_name}'。")

        except Exception as e:
            self.logger.error(
                f"❌ 載入 Parquet 檔案 '{parquet_file_path}' 至資料表 '{table_name}' 時發生錯誤: {e}",
                exc_info=True
            )

    def close_connection(self):
        """
        安全地關閉與資料庫的連線。
        """
        if self.connection:
            self.logger.info("正在關閉 DuckDB 資料庫連線...")
            self.connection.close()
            self.connection = None
            self.logger.info("資料庫連線已成功關閉。")
