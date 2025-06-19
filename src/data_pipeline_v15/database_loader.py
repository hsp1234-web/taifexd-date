# -*- coding: utf-8 -*-

"""
資料庫載入模組

此模組負責將處理完成的 DataFrame 數據，高效地載入至 DuckDB 資料庫中。
"""

import logging
from typing import TYPE_CHECKING

import duckdb
import pandas as pd

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
        self._connect()

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

    def load_data(self, df: pd.DataFrame):
        """
        將 DataFrame 的數據載入到 DuckDB 的指定資料表中。

        此方法會根據 DataFrame 中 'schema_type' 欄位的唯一值，
        動態決定要將數據存入哪一個資料表。

        Args:
            df (pd.DataFrame): 包含已處理數據的 DataFrame。
                               必須包含 'schema_type' 欄位。
        """
        if self.connection is None:
            self.logger.error("資料庫連線不存在，無法載入資料。")
            return

        if "schema_type" not in df.columns or df["schema_type"].nunique() != 1:
            self.logger.error(
                "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
            )
            return

        table_name = df["schema_type"].iloc[0]
        self.logger.info(f"準備將 {len(df)} 筆資料載入至資料表 '{table_name}'...")

        try:
            # 將 DataFrame 註冊為一個臨時視圖，然後使用 INSERT INTO ... SELECT ... 的方式載入
            # 這種方式比直接 `to_sql` 更能確保資料表被正確建立與附加
            self.connection.register("temp_df_view", df)

            # 使用 CREATE TABLE IF NOT EXISTS 確保資料表存在
            self.connection.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df_view LIMIT 0;
            """)

            # 插入數據
            self.connection.execute(f"""
                INSERT INTO {table_name} SELECT * FROM temp_df_view;
            """)

            self.logger.info(f"✅ 成功將 {len(df)} 筆資料附加至資料表 '{table_name}'。")

        except Exception as e:
            self.logger.error(f"❌ 載入資料至資料表 '{table_name}' 時發生錯誤: {e}", exc_info=True)
        finally:
            # 移除臨時視圖
            self.connection.unregister("temp_df_view")

    def close_connection(self):
        """
        安全地關閉與資料庫的連線。
        """
        if self.connection:
            self.logger.info("正在關閉 DuckDB 資料庫連線...")
            self.connection.close()
            self.connection = None
            self.logger.info("資料庫連線已成功關閉。")
