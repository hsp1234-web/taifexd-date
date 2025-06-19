# -*- coding: utf-8 -*-

import json
# import logging
import os
import shutil
import sys # Added for stderr in version loading fallback
import importlib.metadata # Added for version loading
# import threading
# import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

import duckdb

from .database_loader import load_staging_to_database
from .file_parser import worker_process_file
from .manifest_manager import FileManifest
from .utils.logger import Logger
from .utils.monitor import HardwareMonitor
from .core import constants # 修改：導入常數

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


class PipelineOrchestrator:
    """數據整合管道協調器。
    (docstring 已省略)
    """

    def __init__(self, project_folder_name: str, database_name: str, log_name: str, zip_files: str,
                 run_mode: str = "NORMAL", conflict_strategy: str = "REPLACE",
                 schemas_config_path: str = "config/schemas.json",  # Relative to package root
                 duckdb_memory_limit_gb: int = 4, duckdb_threads: int = -1, # -1 for auto os.cpu_count()
                 micro_batch_size: int = 20, hardware_monitor_interval: int = 2,
                 recreate_workspace_on_run: bool = True, cleanup_workspace_on_finish: bool = False,
                 debug_mode: bool = False):
        """初始化 PipelineOrchestrator。
        (docstring 已省略)
        """
        self.project_folder_name = project_folder_name
        self.database_name = database_name
        self.log_name = log_name
        self.zip_files = zip_files
        self.run_mode = run_mode.upper()
        self.conflict_strategy = conflict_strategy.upper()
        self.schemas_config_path = schemas_config_path
        self.duckdb_memory_limit_gb = duckdb_memory_limit_gb
        self.duckdb_threads = duckdb_threads if duckdb_threads != -1 else (os.cpu_count() or 2)
        self.micro_batch_size = micro_batch_size
        self.hardware_monitor_interval = hardware_monitor_interval
        self.recreate_workspace_on_run = recreate_workspace_on_run
        self.cleanup_workspace_on_finish = cleanup_workspace_on_finish
        self.is_debug_mode = debug_mode # Store debug_mode

        # Resolve paths first as logger path depends on it
        self.paths = self._resolve_paths()

        # Setup main logger
        os.makedirs(self.paths["local_logs_dir"], exist_ok=True)
        self.log_file_path = os.path.join(self.paths["local_logs_dir"], self.log_name) # Main log file

        # Dynamically set log level based on debug_mode
        log_level = 'DEBUG' if self.is_debug_mode else 'INFO'
        self.logger = Logger(log_file_path=self.log_file_path, level=log_level)
        self.logger.log(
            f"PipelineOrchestrator (數據整合平台 v15) initialized. Logging to: {self.log_file_path} with level: {log_level}",
            level="info",
        )
        # Log parameters used for this run
        params_for_log = {k: v for k, v in self.__dict__.items() if not k.startswith('_') and k not in ['logger', 'hw_monitor', 'file_manifest', 'paths', 'schemas']}
        self.logger.log(
            f"Run parameters: {json.dumps(params_for_log, indent=2, ensure_ascii=False)}",
            level="info",
        )
        self.logger.log(
            f"Resolved workspace paths: {json.dumps(self.paths, indent=2, ensure_ascii=False)}",
            level="info",
        )

        # Load schemas
        self.schemas = self._load_schemas()

        self.hw_monitor = None # Initialize to None
        if self.is_debug_mode:
            self.hw_monitor = HardwareMonitor(
                logger=self.logger, interval=self.hardware_monitor_interval
            )
        self.file_manifest = None

        try:
            # 使用先前確認的套件名稱 "data-pipeline-v15"
            self.version = importlib.metadata.version("data-pipeline-v15")
        except importlib.metadata.PackageNotFoundError:
            # 如果套件未安裝或找不到版本，則設定為 "unknown"
            self.version = "unknown"
            if hasattr(self, 'logger') and self.logger: # 檢查 logger 是否已初始化
                self.logger.warning("無法讀取 'data-pipeline-v15' 的版本號，可能未透過 poetry install 安裝。")
            else: # 如果 logger 尚未初始化，則打印到 stderr
                # import sys # Already imported at the top
                print("警告: 無法讀取 'data-pipeline-v15' 的版本號，可能未透過 poetry install 安裝。", file=sys.stderr)


    def _load_schemas(self) -> Dict[str, Any]:
        """從指定的路徑載入 schemas.json 檔案。"""
        # Assuming schemas_config_path is relative to the package root (where main.py is)
        # If this script (pipeline_orchestrator.py) is in src/data_pipeline_v15/, need to go up.
        package_root = Path(__file__).parent.parent.parent # Adjust based on actual structure
        abs_schemas_path = package_root / self.schemas_config_path
        try:
            with open(abs_schemas_path, 'r', encoding='utf-8') as f:
                loaded_schemas = json.load(f)
            self.logger.log(f"Successfully loaded schemas from: {abs_schemas_path}", level="info")
            return loaded_schemas
        except FileNotFoundError:
            self.logger.log(f"ERROR: Schemas file not found at {abs_schemas_path}", level="error")
            raise
        except json.JSONDecodeError as e:
            self.logger.log(f"ERROR: Failed to parse schemas file {abs_schemas_path}: {e}", level="error")
            raise


    def _resolve_paths(self) -> Dict[str, str]:
        """根據初始參數建構並回傳一個包含所有絕對路徑的字典。"""
        # project_folder_name is the root for all operations for this instance.
        local_workspace_root = os.path.abspath(self.project_folder_name)

        resolved = {
            "local_workspace": local_workspace_root,
            "local_input": os.path.join(
                local_workspace_root,
                constants.DIR_NAME_INPUT
            ),
            "local_staging": os.path.join(
                local_workspace_root,
                constants.DIR_NAME_STAGING
            ),
            "local_database_dir": os.path.join(
                local_workspace_root,
                constants.DIR_NAME_DATABASE
            ),
            "local_db_file_path": os.path.join(
                local_workspace_root, # Database file stored in the database directory
                constants.DIR_NAME_DATABASE,
                self.database_name # Use the provided database name
            ),
            "local_manifests_dir": os.path.join(
                local_workspace_root,
                constants.DIR_NAME_MANIFESTS
            ),
            "file_manifest_path": os.path.join(
                local_workspace_root,
                constants.DIR_NAME_MANIFESTS,
                "file_manifest.json", # Standard manifest file name
            ),
            "local_failed_dir": os.path.join(
                local_workspace_root,
                constants.DIR_NAME_FAILED
            ),
            "local_logs_dir": os.path.join(
                local_workspace_root, constants.DIR_NAME_LOGS
            ),
        }
        return resolved

    def _setup_workspace(self) -> None:
        """準備本地工作區。
        (docstring 已省略)
        """
        self.logger.log("步驟 A: 環境準備與本地工作區設定", level="step")
        # recreate = self.config.get("recreate_workspace_on_run", True) # OLD
        if self.recreate_workspace_on_run and os.path.exists(self.paths["local_workspace"]):
            self.logger.log(
                f"偵測到設定 recreate_workspace_on_run 為 True，正在刪除現有工作區: {self.paths['local_workspace']}",
                level="info",
            )
            try:
                shutil.rmtree(self.paths["local_workspace"])
                self.logger.log(
                    f"舊工作區 '{self.paths['local_workspace']}' 已成功刪除。",
                    level="success",
                )
            except Exception as e:
                self.logger.log(
                    f"刪除舊工作區 '{self.paths['local_workspace']}' 時發生錯誤: {e}",
                    level="error",
                )
        # self.paths already contains all necessary directory paths based on constants
        dirs_to_create = [
            self.paths["local_workspace"],       # Root project folder
            self.paths["local_input"],           # Input for zips
            self.paths["local_staging"],         # Staging for Parquet
            self.paths["local_database_dir"],    # Database storage
            self.paths["local_manifests_dir"],   # Manifests
            self.paths["local_failed_dir"],      # Failed files
            self.paths["local_logs_dir"],        # Logs
        ]
        self.logger.log("開始建立工作區目錄結構...", level="info")
        for path_value in sorted(list(set(dirs_to_create))): # Ensure parent dirs are created first if not explicitly listed
            try:
                os.makedirs(path_value, exist_ok=True)
                self.logger.log(f"確保目錄存在: {path_value}", level="substep")
            except Exception as e:
                self.logger.log(
                    f"建立目錄 '{path_value}' 時發生錯誤: {e}", level="error"
                )
                raise
        self.logger.log(
            f"本地工作區 '{self.paths['local_workspace']}' 及其子目錄已準備就緒。",
            level="success",
        )
        try:
            self.file_manifest = FileManifest(
                manifest_path=self.paths["file_manifest_path"],
                logger=self.logger
            )
            self.logger.log(
                f"FileManifest 已在 '{self.paths['file_manifest_path']}' 初始化。",
                level="success",
            )
            self.logger.log(
                f"已從 manifest 載入 {len(self.file_manifest.processed_hashes)} 個已處理檔案的雜湊值。",
                level="info",
            )
        except Exception as e:
            self.logger.log(f"初始化 FileManifest 時發生錯誤: {e}", level="error")
            raise

    def _get_files_to_process(self) -> List[Dict[str, str]]:
        """掃描輸入目錄，根據執行模式和 Manifest 篩選待處理檔案。"""
        self.logger.log("掃描本地輸入目錄並建立工作清單...", level="substep")

        input_zip_dir = os.path.join(self.paths["local_input"], "zip") # Assuming zips are in Input/zip/
        os.makedirs(input_zip_dir, exist_ok=True) # Ensure zip directory exists

        all_files_in_input_zip_dir = []
        if self.zip_files: # If specific files are listed
            potential_files = [f.strip() for f in self.zip_files.split(',') if f.strip()]
            self.logger.log(f"指定處理檔案列表: {potential_files}", level="info")
            for file_name in potential_files:
                full_path = os.path.join(input_zip_dir, file_name)
                if os.path.isfile(full_path) and file_name.lower().endswith(".zip"):
                    all_files_in_input_zip_dir.append(full_path)
                else:
                    self.logger.log(f"指定的檔案 '{file_name}' 在 '{input_zip_dir}' 中未找到或非ZIP檔，已跳過。", level="warning")
        else: # Process all zip files in the directory
            self.logger.log(f"未指定特定ZIP檔案，將掃描 '{input_zip_dir}' 目錄下的所有 .zip 檔案。", level="info")
            for root, _, files in os.walk(input_zip_dir):
                for name in files:
                    if not name.startswith(".") and name.lower().endswith(".zip"):
                        all_files_in_input_zip_dir.append(os.path.join(root, name))

        self.logger.log(f"在 '{input_zip_dir}' 中找到 {len(all_files_in_input_zip_dir)} 個ZIP檔案待檢查。", level="info")

        files_to_process_info = []
        # if self.config.get("run_mode", constants.RUN_MODE_NORMAL).upper() == constants.RUN_MODE_BACKFILL: # OLD
        if self.run_mode == constants.RUN_MODE_BACKFILL:
            self.logger.log(
                "警告：您正處於【歷史回填模式】(BACKFILL)，將重新處理所有符合條件的來源檔案。",
                level="warning",
            )
            for f_path in all_files_in_input_zip_dir:
                file_hash = self.file_manifest.get_file_hash(f_path)
                if file_hash:
                    files_to_process_info.append(
                        {"path": f_path, "hash": file_hash}
                    )
                else:
                    self.logger.log(
                        f"無法獲取檔案 {f_path} 的雜湊值，將跳過。", level="warning"
                    )
        else: # Normal mode
            for f_path in all_files_in_input_zip_dir:
                file_hash = self.file_manifest.get_file_hash(f_path)
                if file_hash and not self.file_manifest.has_been_processed(
                    file_hash
                ):
                    files_to_process_info.append(
                        {"path": f_path, "hash": file_hash}
                    )
                elif file_hash and self.file_manifest.has_been_processed(file_hash):
                     self.logger.log(f"檔案 {f_path} (雜湊: {file_hash[:7]}...) 已處理過，正常模式下跳過。", level="info")
                elif not file_hash: # Could be due to file not found or read error
                    self.logger.log(
                        f"無法獲取檔案 {f_path} 的雜湊值或檔案不存在，將跳過。",
                        level="warning",
                    )
        return files_to_process_info

    def _initialize_database(self) -> Optional["DuckDBPyConnection"]:
        """初始化 DuckDB 資料庫連線並確保所有必要的表格結構存在。"""
        db_conn = None
        # max_workers = self.duckdb_threads # Already calculated in __init__
        try:
            db_conn = duckdb.connect(
                database=self.paths["local_db_file_path"],
                read_only=False,
            )
            # db_settings = self.config.get( # OLD
            #     "duckdb_settings",
            #     {"memory_limit_gb": 1, "threads": max_workers},
            # )
            db_conn.execute(
                f"SET memory_limit='{self.duckdb_memory_limit_gb}GB';"
            )
            db_conn.execute(
                f"SET threads={self.duckdb_threads};"
            )
            self.logger.log(
                f"DuckDB 資料庫連線已建立: {self.paths['local_db_file_path']}",
                level="success",
            )
            # for schema_key, schema_val in self.config["schemas"].items(): # OLD
            for schema_key, schema_val in self.schemas.items():
                actual_table_name = schema_val["db_table_name"]
                sql_quoted_table_name = (
                    '"' + actual_table_name.replace('"', '""') + '"'
                )
                cols_map = schema_val.get("columns_map", {})
                if not cols_map:
                    self.logger.log(f"Schema '{schema_key}' has no columns_map defined, skipping table creation for it.", level="warning")
                    continue
                cols_list_py = ['"id" BIGINT'] + [ # Assuming 'id' is always BIGINT and primary, or managed otherwise
                    f'"'
                    + col_name.replace('"', '""')
                    + f'''" {col_info["db_type"]}'''
                    for col_name, col_info in cols_map.items()
                ]
                cols_list_sql_str = ", ".join(cols_list_py)
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {sql_quoted_table_name} ({cols_list_sql_str});"
                db_conn.execute(create_table_sql)
                self.logger.log(f"Ensured table exists: {actual_table_name} via SQL: {create_table_sql}", level="debug")
            self.logger.log(
                "DuckDB 資料庫表格結構已確認/建立。", level="success"
            )
            return db_conn
        except Exception as db_init_err:
            self.logger.log(
                f"建立 DuckDB 連線或初始化表格失敗: {db_init_err}",
                level="error",
            )
            self.logger.log(traceback.format_exc(), level="error")
            if db_conn:
                try:
                    db_conn.close()
                except Exception as db_close_err:
                    self.logger.log(f"嘗試關閉失敗的 DuckDB 連線時發生錯誤: {db_close_err}", level="warning")
            return None

    def _process_file_batch(self, batch_files_info: List[Dict[str, str]], num_workers: int) -> Set[str]:
        """並行處理一個批次的檔案，解析並回傳成功處理的檔案雜湊。"""
        parsed_results_in_batch = []
        # Using self.duckdb_threads for ProcessPoolExecutor as well, or define a new param for parsing workers
        # For now, let's assume parsing can also use similar number of workers as db threads.
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(
                    worker_process_file,
                    item["path"], # Full path to the zip file
                    self.paths["local_staging"], # Staging directory for Parquet files
                    self.schemas, # Loaded schemas
                ): item
                for item in batch_files_info
            }
            for future in as_completed(futures):
                item_info = futures[future]
                try:
                    result = future.result()
                    if result.get(constants.KEY_STATUS) == constants.STATUS_GROUP_RESULT:
                        for sub_result in result.get(constants.KEY_RESULTS, []):
                            parsed_results_in_batch.append(
                                {
                                    "item_info": item_info, # item_info contains original path and hash
                                    "parse_output": sub_result, # sub_result is for one extracted file from zip
                                }
                            )
                    else: # Single file result (e.g. if zip contains one relevant file or processing non-zip)
                        parsed_results_in_batch.append(
                            {"item_info": item_info, "parse_output": result}
                        )
                except Exception as e:
                    self.logger.log(
                        f"處理檔案 {item_info['path']} 時發生嚴重執行緒錯誤: {e}",
                        level="error",
                    )
                    self.logger.log(traceback.format_exc(), level="error")
                    # Record this as a failed parse for the original file
                    parsed_results_in_batch.append(
                        {
                            "item_info": item_info,
                            "parse_output": {
                                constants.KEY_STATUS: constants.STATUS_ERROR,
                                constants.KEY_FILE: os.path.basename(item_info["path"]), # Original ZIP filename
                                constants.KEY_REASON: f"Worker error: {str(e)}",
                            },
                        }
                    )

        current_batch_successful_hashes = set() # Hashes of original ZIP files
        processed_a_sub_file_successfully_for_zip = {} # Tracks if any sub-file from a zip was successful

        for res_item in parsed_results_in_batch:
            original_item_info = res_item["item_info"] # Info of the source ZIP
            parse_output = res_item["parse_output"] # Info of one file inside the ZIP

            status = parse_output.get(constants.KEY_STATUS)
            # file_display_name is the name of the file *inside* the zip, or the zip itself if error at that level
            file_display_name = parse_output.get(constants.KEY_FILE, os.path.basename(original_item_info["path"]))
            reason = parse_output.get(constants.KEY_REASON, "未知錯誤")

            if status == constants.STATUS_SUCCESS:
                self.logger.log(
                    f"↳ 成功 ({parse_output.get(constants.KEY_TABLE, 'N/A')}): {file_display_name} (來源ZIP: {os.path.basename(original_item_info['path'])}) -> 暫存 {parse_output.get(constants.KEY_COUNT, 0)} 筆。",
                    level="success",
                )
                # If any sub-file from the ZIP is processed successfully, the ZIP is considered successful for manifest
                processed_a_sub_file_successfully_for_zip[original_item_info["hash"]] = True
            elif status == constants.STATUS_SKIPPED:
                self.logger.log(
                    f"↳ 跳過: {file_display_name} (來源ZIP: {os.path.basename(original_item_info['path'])}) -> 原因: {reason}",
                    level="warning",
                )
            else: # STATUS_ERROR
                self.logger.log(
                    f"↳ 失敗: {file_display_name} (來源ZIP: {os.path.basename(original_item_info['path'])}) -> 原因: {reason}",
                    level="error",
                )
                # If a sub-file fails, we don't necessarily mark the whole ZIP as failed for manifest,
                # unless no other sub-file from it succeeds.
                # However, we still copy the original ZIP to failed_dir if any sub_result is an error.
                # This part might need refinement based on desired granularity of "failure"

                source_zip_to_copy = original_item_info["path"]
                failed_zip_basename = os.path.basename(source_zip_to_copy)
                failed_dest_path = os.path.join(self.paths["local_failed_dir"], failed_zip_basename)

                if not os.path.exists(failed_dest_path): # Copy only once
                    try:
                        if os.path.exists(source_zip_to_copy):
                             shutil.copy(source_zip_to_copy, failed_dest_path)
                             self.logger.log(
                                 f"  因内部檔案 '{file_display_name}' 處理失敗，來源ZIP檔案 {failed_zip_basename} 已複製到: {failed_dest_path}",
                                 level="info",
                             )
                    except Exception as copy_err:
                        self.logger.log(
                            f"  複製失敗的來源ZIP檔案 {failed_zip_basename} 時發生錯誤: {copy_err}",
                            level="warning",
                        )

        # Update successful hashes for the batch based on `processed_a_sub_file_successfully_for_zip`
        for zip_hash, was_successful in processed_a_sub_file_successfully_for_zip.items():
            if was_successful:
                current_batch_successful_hashes.add(zip_hash)

        return current_batch_successful_hashes

    def _load_batch_to_db_and_update_manifest(self, db_conn: "DuckDBPyConnection", successful_hashes_in_batch: Set[str]) -> None:
        """將一個成功處理批次的數據載入資料庫並更新檔案處理清單。"""
        if not successful_hashes_in_batch: # These are hashes of ZIP files
            self.logger.log("沒有成功處理的檔案雜湊可供載入資料庫或更新 Manifest。", level="info")
            return

        load_staging_to_database(
            db_conn=db_conn,
            schemas_config=self.schemas, # Use loaded schemas
            local_staging_path=self.paths["local_staging"],
            logger=self.logger,
            conflict_strategy=self.conflict_strategy, # Use instance variable
        )
        self.file_manifest.add_processed_hashes(
            successful_hashes_in_batch # Hashes of the ZIP files
        )
        self.logger.log(
            f"Manifest 已更新，添加了 {len(successful_hashes_in_batch)} 個已處理檔案的雜湊值。",
            level="success",
        )

    def _teardown(self, db_conn: Optional["DuckDBPyConnection"]) -> None:
        """執行管道的收尾工作，如關閉連線、清理工作區和停止監控。"""
        if db_conn:
            try:
                db_conn.close()
                self.logger.log("DuckDB 資料庫連線已關閉。", level="success")
            except Exception as db_close_err:
                self.logger.log(
                    f"關閉 DuckDB 連線時發生錯誤: {db_close_err}", level="warning"
                )

        # if self.config.get("cleanup_workspace_on_finish", False) and os.path.exists( # OLD
        if self.cleanup_workspace_on_finish and os.path.exists(
            self.paths["local_workspace"]
        ):
            try:
                shutil.rmtree(self.paths["local_workspace"])
                self.logger.log(
                    f"本地工作區 '{self.paths['local_workspace']}' 已成功清理。",
                    level="success",
                )
            except Exception as ws_clean_err:
                self.logger.log(
                    f"清理本地工作區 '{self.paths['local_workspace']}' 時發生錯誤: {ws_clean_err}",
                    level="warning",
                )

        if self.is_debug_mode and self.hw_monitor:
            self.hw_monitor.stop()
        # final_log_path = getattr(self, "main_log_file_path", self.log_file_path) # OLD, now self.log_file_path is the main one
        self.logger.log(
            f"{self.project_folder_name} (數據整合平台 v15) 已執行完畢。日誌檔案位於: {self.log_file_path}",
            level="step",
        )

    def run(self) -> None:
        """執行完整的數據整合管道。"""
        # 準備版本與模式字串
        version_info = ""
        if self.is_debug_mode:
            # 假設 self.version 已在 __init__ 中設定
            version_string = f"v{self.version}" if self.version != "unknown" else "(版本未知)"
            version_info = f" {version_string} (除錯模式)"

        # 打印包含版本資訊的啟動橫幅
        # 假設 self.project_folder_name 存在
        project_name = getattr(self, 'project_folder_name', '數據整合平台') # 提供預設值以防萬一
        self.logger.log(f"""\
{os.linesep}
================================================================================
🚚 {project_name} (數據整合平台{version_info}) 執行開始...
================================================================================"""
            , level="step" # Keep level as step, or change to info if preferred for banner
        )
        if self.is_debug_mode and self.hw_monitor:
            self.hw_monitor.start()
        db_conn: Optional["DuckDBPyConnection"] = None

        try:
            self._setup_workspace() # Uses self.recreate_workspace_on_run

            files_to_process_info = self._get_files_to_process() # Uses self.zip_files, self.run_mode

            if not files_to_process_info:
                self.logger.log("沒有新的檔案需要處理。", level="success")
            else:
                self.logger.log(
                    f"共發現 {len(files_to_process_info)} 個新檔案待處理。", level="info"
                )

                total_files = len(files_to_process_info)
                # batch_size = self.config.get("micro_batch_size", 20) # OLD
                batch_size = self.micro_batch_size
                num_batches = (total_files + batch_size - 1) // batch_size
                successful_overall_hashes: Set[str] = set()

                # cpu_count = os.cpu_count() # OLD, self.duckdb_threads is set in init
                # max_workers = self.config.get("duckdb_settings", {}).get( # OLD
                #     "threads", cpu_count if cpu_count else 2
                # )
                # Using self.duckdb_threads for number of workers in ProcessPoolExecutor
                # This could be a separate parameter if parsing and DB loading need different parallelism
                num_processing_workers = self.duckdb_threads


                for i in range(num_batches):
                    batch_start_index = i * batch_size
                    batch_end_index = (i + 1) * batch_size
                    batch_files_info = files_to_process_info[batch_start_index:batch_end_index]

                    self.logger.log(
                        f"微批次 {i + 1}/{num_batches}: 開始處理 {len(batch_files_info)} 個檔案...",
                        level="substep",
                    )

                    current_batch_successful_hashes = self._process_file_batch(batch_files_info, num_processing_workers)

                    if current_batch_successful_hashes:
                        if db_conn is None: # Initialize DB only if there's something to load
                            db_conn = self._initialize_database() # Uses self.paths, self.schemas, self.duckdb_...

                        if db_conn:
                            # Uses self.schemas, self.paths, self.conflict_strategy
                            self._load_batch_to_db_and_update_manifest(db_conn, current_batch_successful_hashes)
                            successful_overall_hashes.update(current_batch_successful_hashes)
                        else:
                            self.logger.log(f"微批次 {i + 1}/{num_batches}: 資料庫未成功初始化，跳過此批次的資料載入。", level="error")
                    else:
                        self.logger.log(
                            f"微批次 {i + 1}/{num_batches} 中沒有成功解析的檔案可供載入資料庫。",
                            level="info",
                        )
                    self.logger.log(f"微批次 {i + 1}/{num_batches} 處理完畢。", level="info")

                self.logger.log(
                    f"所有 {num_batches} 個微批次均已處理完成。共 {len(successful_overall_hashes)} 個來源ZIP檔案成功處理並記錄到 Manifest。",
                    level="info",
                )

        except Exception as e:
            self.logger.log(f"管道執行過程中發生未預期的嚴重錯誤: {e}", level="error")
            self.logger.log(traceback.format_exc(), level="error")
        finally:
            self._teardown(db_conn) # Uses self.cleanup_workspace_on_finish, self.paths, self.project_folder_name


if __name__ == "__main__":
    # This main block needs to be updated to reflect new __init__ signature
    print(
        f"PipelineOrchestrator 模組可被導入。"
    )

    # Create a dummy schemas.json for testing if it doesn't exist
    current_script_dir = Path(__file__).parent
    dummy_config_dir = current_script_dir.parent.parent / "config"
    dummy_schemas_file = dummy_config_dir / "schemas.json"

    if not dummy_schemas_file.exists():
        print(f"創建臨時的 {dummy_schemas_file} 以便測試...")
        os.makedirs(dummy_config_dir, exist_ok=True)
        temp_schemas_content = {
            "test_schema": {
                "db_table_name": "test_table",
                "columns_map": {"col1": {"db_type": "VARCHAR"}, "col2": {"db_type": "INTEGER"}}
            }
        }
        with open(dummy_schemas_file, 'w', encoding='utf-8') as f_temp:
            json.dump(temp_schemas_content, f_temp, indent=2)

    print(f"\n--- 測試 PipelineOrchestrator 初始化 (使用新參數) ---")
    try:
        # Ensure the project folder for testing exists or can be created cleanly
        test_project_folder = "./temp_orchestrator_test_workspace"
        if os.path.exists(test_project_folder):
            shutil.rmtree(test_project_folder) # Clean up before test

        orchestrator = PipelineOrchestrator(
            project_folder_name=test_project_folder,
            database_name="test_db.duckdb",
            log_name="test_pipeline.log",
            zip_files="test1.zip,test2.zip", # Example
            run_mode="NORMAL",
            conflict_strategy="REPLACE",
            schemas_config_path="config/schemas.json", # Relative to package root
            debug_mode=True # Test with debug mode on
        )
        print(f"Orchestrator 初始化成功 (Debug Mode: {orchestrator.is_debug_mode}).")
        print(f" - 主日誌檔案位於: {orchestrator.log_file_path}")
        print(f" - 工作區路徑:")
        for key, path_val in orchestrator.paths.items():
            print(f"    - {key}: {path_val}")
        print(f" - Schemas: {list(orchestrator.schemas.keys()) if orchestrator.schemas else 'Not loaded'}")

        # Basic workspace setup test
        orchestrator._setup_workspace()
        print(f" - Workspace setup 執行完成。檢查 '{test_project_folder}' 目錄。")
        # orchestrator.run() # Could run a full test if dummy input files are provided

    except Exception as e:
        print(f"Orchestrator 測試時發生錯誤: {e}")
        print(traceback.format_exc())
    finally:
        # Clean up dummy schema and workspace if created
        # if dummy_schemas_file.exists() and "temp_schemas_content" in locals():
        #     print(f"刪除臨時的 {dummy_schemas_file}...")
        #     os.remove(dummy_schemas_file)
        # if os.path.exists(test_project_folder) and "test_project_folder" in locals():
        #     print(f"測試後建議手動清理測試工作區: {test_project_folder}")
        pass
