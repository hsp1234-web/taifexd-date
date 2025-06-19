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
    KEY_STATUS,
    KEY_PATH,
    KEY_TABLE,
    KEY_REASON,
    KEY_RESULTS,
    KEY_FILE,
    KEY_COUNT,
    STATUS_SUCCESS,
    STATUS_ERROR,
    STATUS_GROUP_RESULT,
    STATUS_SKIPPED,
)
from .core import constants  # Added to allow access to constants like constants.KEY_STATUS
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

        # Ensure the database directory exists before DatabaseLoader tries to connect
        os.makedirs(self.db_path, exist_ok=True) # db_path needs to be defined first

        # --- 參數設定 ---
        self.debug_mode = debug_mode
        # Logger setup needs log_path, so it comes after path setup.
        self.logger = setup_logger(self.log_path, log_name, debug_mode)

        # Now that logger and paths are set, setup directories.
        self._setup_directories()

        self.database_file = os.path.join(self.db_path, database_name)
        self.schemas_config = {} # Added as per requirement

        # --- 模組初始化 ---
        manifest_file_path = os.path.join(self.project_path, "manifest.json") # Define manifest file path
        self.manifest_manager = ManifestManager(manifest_path=manifest_file_path, logger=self.logger)
        self.file_parser = FileParser(self.manifest_manager, self.logger) # FileParser now takes manifest_manager and logger
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
            # 1. 載入或建立 Manifest
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
                    self.logger.debug(f"Skipping non-file item: {filename}")
                    continue

                self.logger.info(f"--- 開始處理檔案: {filename} ---")

                # Get file hash to check against the manifest
                file_hash = ManifestManager.get_file_hash(file_path)
                if not file_hash:
                    self.logger.error(f"無法計算檔案雜湊值: {file_path}。可能檔案不存在或無法讀取。將跳過此檔案。")
                    # Optionally, move to quarantine and update manifest with error for filename
                    # For now, just logging and skipping.
                    self.manifest_manager.update_manifest(filename, STATUS_ERROR, f"無法計算檔案雜湊值 (路徑: {file_path})")
                    try:
                        shutil.move(file_path, os.path.join(self.quarantine_path, filename))
                        self.logger.warning(f"檔案 '{filename}' 因無法取得雜湊值已移動至隔離區。")
                    except Exception as move_e:
                        self.logger.error(f"移動檔案 '{filename}' 至隔離區失敗 (因無法取得雜湊值): {move_e}")
                    continue

                if self.manifest_manager.has_been_processed(file_hash):
                    self.logger.warning(f"檔案 '{filename}' (雜湊: {file_hash}) 已被處理過且記錄在案，將跳過。")
                    continue

                # Call the refactored file_parser's parse_file method
                parse_result = self.file_parser.parse_file(file_path, self.processed_path, self.schemas_config)

                overall_status_for_manifest = parse_result.get(constants.KEY_STATUS, constants.STATUS_ERROR)
                overall_message_for_manifest = parse_result.get(constants.KEY_REASON, f"檔案 '{filename}' 處理時遇到未知狀況。")

                # Default assumption: move to quarantine unless explicitly successful
                move_to_processed = False

                if overall_status_for_manifest == constants.STATUS_GROUP_RESULT:
                    self.logger.info(f"檔案 '{filename}' (ZIP) 包含多個項目，正在逐一處理子項目...")
                    all_sub_results = parse_result.get(constants.KEY_RESULTS, [])

                    if not all_sub_results:
                        # This case implies the ZIP was perhaps empty or had non-CSV contents,
                        # but was still successfully opened and assessed as a 'group'.
                        # The file_parser should ideally set a more specific KEY_REASON.
                        overall_message_for_manifest = parse_result.get(constants.KEY_REASON, f"ZIP 檔案 '{filename}' 未包含可處理的 CSV 內容。")
                        self.logger.warning(overall_message_for_manifest)
                        # Keep move_to_processed = False (goes to quarantine) unless a sub-file succeeds.

                    successful_sub_files = 0
                    failed_sub_files = 0
                    at_least_one_sub_file_db_loaded = False

                    for item_result in all_sub_results:
                        item_status = item_result.get(constants.KEY_STATUS)
                        item_msg = item_result.get(constants.KEY_REASON, "子項目處理訊息遺失")
                        item_path = item_result.get(constants.KEY_PATH)
                        item_table = item_result.get(constants.KEY_TABLE)
                        item_display_name = item_result.get(constants.KEY_FILE, "未知子項目")
                        item_data_count = item_result.get(constants.KEY_COUNT, 0)

                        if item_status == constants.STATUS_SUCCESS and item_path and item_table:
                            self.logger.info(f"子項目 '{item_display_name}' (來自 {filename}) 解析成功，共 {item_data_count} 筆資料。表格: {item_table}, 路徑: {item_path}。")
                            try:
                                # Assuming DatabaseLoader.load_data(table_name, parquet_file_path)
                                self.db_loader.load_data(item_table, item_path)
                                self.logger.info(f"子項目 '{item_display_name}' 的資料已成功載入資料庫。")
                                successful_sub_files += 1
                                at_least_one_sub_file_db_loaded = True # Key for moving ZIP to processed
                            except Exception as db_e:
                                self.logger.error(f"子項目 '{item_display_name}' 資料載入資料庫失敗: {db_e}")
                                failed_sub_files += 1
                        elif item_status == constants.STATUS_SKIPPED:
                             self.logger.warning(f"子項目 '{item_display_name}' (來自 {filename}) 被跳過: {item_msg}")
                             # Not necessarily a failure for the whole ZIP
                        else: # Error or other non-success status for the sub-item
                            self.logger.error(f"子項目 '{item_display_name}' (來自 {filename}) 解析失敗: {item_msg}")
                            failed_sub_files += 1

                    if at_least_one_sub_file_db_loaded: # If any sub-file was successfully loaded to DB
                        move_to_processed = True
                        overall_status_for_manifest = constants.STATUS_SUCCESS # Mark overall ZIP as success if at least one part succeeded
                        overall_message_for_manifest = f"ZIP '{filename}' 處理完成。成功載入資料庫: {successful_sub_files} 個子項目, 處理失敗/跳過: {failed_sub_files} 個子項目。"
                    else:
                        # No sub-file made it to the DB
                        move_to_processed = False
                        overall_status_for_manifest = constants.STATUS_ERROR # Mark overall ZIP as error
                        if failed_sub_files > 0 :
                             overall_message_for_manifest = f"ZIP '{filename}' 所有可處理的子項目均處理失敗或資料庫載入失敗 ({failed_sub_files} 個失敗)。"
                        elif all_sub_results and successful_sub_files == 0 and failed_sub_files == 0 : # All skipped or no actual data
                             overall_message_for_manifest = f"ZIP '{filename}' 未包含成功載入資料庫的資料 (可能所有子項目被跳過或無資料)。"
                        # If all_sub_results was empty from the start, overall_message_for_manifest retains its earlier value.


                elif overall_status_for_manifest == constants.STATUS_SUCCESS:
                    # This branch is for single CSV files (or other direct success statuses from file_parser)
                    parquet_path = parse_result.get(constants.KEY_PATH)
                    table_name = parse_result.get(constants.KEY_TABLE)
                    file_data_count = parse_result.get(constants.KEY_COUNT, 0)

                    if parquet_path and table_name:
                        self.logger.info(f"檔案 '{filename}' 解析成功，共 {file_data_count} 筆資料。表格: {table_name}, 路徑: {parquet_path}")
                        try:
                            # Assuming DatabaseLoader.load_data(table_name, parquet_file_path)
                            self.db_loader.load_data(table_name, parquet_path)
                            self.logger.info(f"檔案 '{filename}' 的資料已成功載入資料庫。")
                            move_to_processed = True
                            overall_message_for_manifest = f"檔案 '{filename}' 成功處理並載入 {file_data_count} 筆記錄到表格 '{table_name}'。"
                        except Exception as db_e:
                            self.logger.error(f"檔案 '{filename}' 解析成功但資料庫載入失敗: {db_e}")
                            overall_status_for_manifest = constants.STATUS_ERROR # Update status due to DB error
                            overall_message_for_manifest = f"檔案 '{filename}' 解析成功但資料庫載入失敗: {db_e}"
                            move_to_processed = False
                    else:
                        # STATUS_SUCCESS but no path or table - should ideally not happen.
                        self.logger.error(f"檔案 '{filename}' 狀態為成功但缺少 Parquet 路徑或表格名稱。")
                        overall_status_for_manifest = constants.STATUS_ERROR # Correct status to error
                        overall_message_for_manifest = parse_result.get(constants.KEY_REASON, f"檔案 '{filename}' 狀態為成功但缺少必要資訊 (路徑/表格)。")
                        move_to_processed = False

                # For all other statuses (STATUS_ERROR, STATUS_SKIPPED at the top level for the file itself)
                # move_to_processed remains False, overall_status_for_manifest and overall_message_for_manifest
                # should already be set from parse_result.get(...)
                # Example: if file was skipped by file_parser, its status would be STATUS_SKIPPED.
                # If file_parser had an error (e.g. bad zip file), status would be STATUS_ERROR.

                if move_to_processed:
                    try:
                        shutil.move(file_path, os.path.join(self.processed_path, filename))
                        self.logger.info(f"檔案 '{filename}' 已處理並移動至 -> {self.processed_path}")
                    except Exception as move_e:
                        self.logger.error(f"成功處理後，移動檔案 '{filename}' 至 {self.processed_path} 失敗: {move_e}")
                        overall_status_for_manifest = constants.STATUS_ERROR # Update status due to move error
                        overall_message_for_manifest += f" 但移至已處理資料夾失敗: {move_e}"
                else:
                    try:
                        shutil.move(file_path, os.path.join(self.quarantine_path, filename))
                        self.logger.warning(f"檔案 '{filename}' 已移動至隔離區 -> {self.quarantine_path} (原因: {overall_message_for_manifest})")
                    except Exception as move_e:
                        self.logger.error(f"處理失敗後，移動檔案 '{filename}' 至 {self.quarantine_path} 失敗: {move_e}")
                        # The manifest will still record the original processing error.

                # For update_manifest, it should ideally use the file_hash if successful,
                # or filename if hashing failed but we still want to record the attempt.
                # The current ManifestManager.update_manifest tries to re-hash filename.
                # This needs to be reconciled. For now, we pass filename.
                # If overall_status_for_manifest is success, update_manifest will attempt to get hash.
                # If it was an error before hashing (like in the block above), filename is fine.
                self.manifest_manager.update_manifest(filename, overall_status_for_manifest, overall_message_for_manifest)
                self.logger.info(f"--- 檔案處理完畢: {filename} (最終狀態記錄: {overall_status_for_manifest}) ---")

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
