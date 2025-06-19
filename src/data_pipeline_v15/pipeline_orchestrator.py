import os
import shutil
import time
from datetime import datetime

import duckdb

from .core.constants import (
    ARCHIVE_DIR,
    DB_DIR,
    INPUT_DIR,
    LOG_DIR,
    PROCESSED_DIR,
    QUARANTINE_DIR,
)
from .database_loader import DatabaseLoader
from .file_parser import FileParser
from .manifest_manager import ManifestManager
from .utils.logger import setup_logger
from .utils.monitor import get_hardware_usage


class PipelineOrchestrator:
    """
    協調整個數據管線的執行流程。
    """

    def __init__(
        self,
        base_path: str,
        project_folder_name: str,
        database_name: str,
        log_name: str,
        target_zip_files: str,
        debug_mode: bool = False,
    ):
        """
        初始化協調器。

        Args:
            base_path (str): 專案的基礎路徑 (例如 /content 或 /content/drive/MyDrive)。
            project_folder_name (str): 專案資料夾的名稱。
            database_name (str): DuckDB 資料庫檔案名稱。
            log_name (str): 日誌檔案名稱。
            target_zip_files (str): 指定要處理的 ZIP 檔案列表（以逗號分隔），或為空。
            debug_mode (bool, optional): 是否啟用除錯模式。預設為 False。
        """
        # --- 路徑設定 ---
        self.project_path = os.path.join(base_path, project_folder_name)
        self.input_path = os.path.join(self.project_path, INPUT_DIR)
        self.archive_path = os.path.join(self.project_path, ARCHIVE_DIR)
        self.processed_path = os.path.join(self.project_path, PROCESSED_DIR)
        self.quarantine_path = os.path.join(self.project_path, QUARANTINE_DIR)
        self.db_path = os.path.join(self.project_path, DB_DIR)
        self.log_path = os.path.join(self.project_path, LOG_DIR)

        self.database_file = os.path.join(self.db_path, database_name)

        # --- 參數設定 ---
        self.debug_mode = debug_mode
        self.logger = setup_logger(self.log_path, log_name, debug_mode)

        # --- 模組初始化 ---
        self.manifest_manager = ManifestManager(self.archive_path)
        self.file_parser = FileParser(self.manifest_manager, self.logger)
        self.db_loader = DatabaseLoader(self.database_file, self.logger)

        # --- 目標檔案處理 ---
        self.target_files = (
            [f.strip() for f in target_zip_files.split(",") if f.strip()]
            if target_zip_files
            else []
        )
        if self.target_files:
            self.logger.info(f"已指定處理特定檔案：{self.target_files}")
        else:
            self.logger.info("未指定特定檔案，將處理所有輸入檔案。")

    def _setup_directories(self):
        """
        建立所有必要的資料夾結構。
        """
        self.logger.info("正在設定專案資料夾結構...")
        directories = [
            self.project_path,
            self.input_path,
            self.archive_path,
            self.processed_path,
            self.quarantine_path,
            self.db_path,
            self.log_path,
        ]
        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
                self.logger.debug(f"成功建立或確認目錄存在：{directory}")
            except OSError as e:
                self.logger.error(f"建立目錄失敗：{directory}，錯誤：{e}")
                raise
        self.logger.info("✅ 專案資料夾結構設定完成。")

    def run(self):
        """
        執行完整數據管線。
        """
        start_time = time.time()
        self.logger.info(f"====== 數據管線開始執行 @ {datetime.now():%Y-%m-%d %H:%M:%S} ======")
        if self.debug_mode:
            self.logger.debug(get_hardware_usage("管線啟動前"))

        try:
            # 1. 設定資料夾
            self._setup_directories()

            # 2. 載入或建立 Manifest
            self.logger.info("正在載入處理紀錄 (manifest)...")
            self.manifest_manager.load_or_create_manifest()

            # 3. 掃描輸入資料夾
            self.logger.info(f"正在掃描輸入資料夾：{self.input_path}")
            all_files_in_input = os.listdir(self.input_path)

            # 過濾出要處理的檔案
            if self.target_files:
                files_to_process = [f for f in all_files_in_input if f in self.target_files]
                self.logger.info(f"根據指定，將處理 {len(files_to_process)} 個檔案。")
            else:
                files_to_process = all_files_in_input
                self.logger.info(f"將處理全部 {len(files_to_process)} 個檔案。")

            if not files_to_process:
                self.logger.warning("輸入資料夾中沒有找到任何要處理的檔案。")
                return

            # 4. 處理每個檔案
            for filename in files_to_process:
                file_path = os.path.join(self.input_path, filename)
                if not os.path.isfile(file_path):
                    continue

                self.logger.info(f"--- 開始處理檔案: {filename} ---")

                # 檢查是否已處理過
                if self.manifest_manager.is_file_processed(filename):
                    self.logger.warning(f"檔案 '{filename}' 已被處理過，將跳過。")
                    continue

                # 解析檔案
                result_df, status, message = self.file_parser.parse_file(file_path)

                if status == "success" and not result_df.empty:
                    self.logger.info(f"檔案 '{filename}' 解析成功，共 {len(result_df)} 筆資料。")
                    # 載入資料庫
                    self.db_loader.load_data(result_df)
                    # 移動到已處理資料夾
                    shutil.move(file_path, os.path.join(self.processed_path, filename))
                    self.logger.info(f"檔案已移動至 -> {self.processed_path}")
                else:
                    self.logger.error(f"檔案 '{filename}' 處理失敗或為空: {message}")
                    # 移動到隔離區
                    shutil.move(file_path, os.path.join(self.quarantine_path, filename))
                    self.logger.warning(f"檔案已移動至隔離區 -> {self.quarantine_path}")

                # 更新 Manifest
                self.manifest_manager.update_manifest(filename, status, message)
                self.logger.info(f"--- 檔案處理完畢: {filename} ---")


        except Exception as e:
            self.logger.critical(f"管線執行過程中發生無法恢復的錯誤: {e}", exc_info=True)
        finally:
            end_time = time.time()
            self.logger.info(f"====== 數據管線執行完畢 @ {datetime.now():%Y-%m-%d %H:%M:%S} ======")
            self.logger.info(f"總耗時: {end_time - start_time:.2f} 秒")
            if self.debug_mode:
                self.logger.debug(get_hardware_usage("管線結束後"))

            # 關閉DuckDB連接
            self.db_loader.close_connection()
            self.logger.info("資料庫連接已關閉。")
