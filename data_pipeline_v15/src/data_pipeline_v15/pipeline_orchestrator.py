# -*- coding: utf-8 -*-

import json
import logging # Not strictly needed here if only self.logger is used, but good for general context
import os
import shutil
import sys # Not strictly needed here, but good for general context
import threading # Not strictly needed here
import time # Not strictly needed here
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

import duckdb

from .database_loader import load_staging_to_database
from .file_parser import worker_process_file
from .manifest_manager import FileManifest
from .utils.logger import Logger
from .utils.monitor import HardwareMonitor

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


class PipelineOrchestrator:
    """數據整合管道協調器。

    負責管理和執行整個數據處理流程，包括初始化、設定工作區、
    檔案發現與篩選、並行解析、資料庫載入、處理狀態追蹤 (Manifest)
    以及最終的清理等步驟。

    本類別透過依賴注入的方式接收一個組態字典 (config)，並使用該組態
    來初始化其依賴的各個模組 (如 Logger, HardwareMonitor, FileManifest 等)，
    並將具體的操作委派給這些模組執行。

    主要公開方法為 `run()`，它啟動並完整執行整個管道流程。

    :ivar config: 包含管道所有必要組態的字典。
    :vartype config: dict
    :ivar paths: 一個由 `_resolve_paths` 方法根據 `config` 解析生成的路徑字典。
    :vartype paths: dict
    :ivar logger: 用於日誌記錄的 Logger 物件執行個體。
    :vartype logger: Logger (來自 .utils.logger.Logger)
    :ivar hw_monitor: 硬體資源監控器物件執行個體。
    :vartype hw_monitor: HardwareMonitor (來自 .utils.monitor.HardwareMonitor)
    :ivar file_manifest: 檔案處理清單管理器物件執行個體，在 `_setup_workspace` 中初始化。
    :vartype file_manifest: Optional[FileManifest] (來自 .manifest_manager.FileManifest)
    :ivar log_file_path: 目前執行產生的日誌檔案路徑。 # This is the initial CWD log
    :vartype log_file_path: str
    :ivar main_log_file_path: 主要執行日誌檔案的路徑，通常在工作區內。
    :vartype main_log_file_path: str
    """

    def __init__(self, config: Dict[str, Any]):
        """初始化 PipelineOrchestrator。

        :param config: 包含管道所有必要設定的字典。
                       預期包含路徑設定 (`paths`)、執行策略 (`run_mode`, `conflict_strategy`)、
                       資料庫設定 (`duckdb_settings`, `db_filename`)、綱要設定 (`schemas`)、
                       以及其他可選設定 (如 `hardware_monitor_interval`, `micro_batch_size`,
                       `recreate_workspace_on_run`, `cleanup_workspace_on_finish`)。
        :type config: dict
        """
        self.config = config
        _initial_log_file_name = (
            f"pipeline_init_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        self.log_file_path = os.path.abspath( # Initial log in CWD
            _initial_log_file_name
        )

        # Initialize logger to write to the initial CWD log first.
        # The final log path (main_log_file_path) is determined after paths are resolved.
        # For simplicity in this version, self.logger instance is not changed after init.
        # Ideally, logging should switch to main_log_file_path.
        self.logger = Logger(log_file_path=self.log_file_path)
        self.logger.log(
            f"PipelineOrchestrator (數據整合平台 v15) PRE-INIT an Logger at {self.log_file_path}",
            level="info",
        )

        self.paths = self._resolve_paths()

        _run_log_file_name = (
            f"執行日誌_v15_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        _log_dir = self.paths.get(
            "local_logs_dir", self.paths.get("local_workspace", ".") # Default to workspace or CWD
        )
        os.makedirs(_log_dir, exist_ok=True)
        self.main_log_file_path = os.path.join(_log_dir, _run_log_file_name)

        # Log the intent to switch, though the logger object itself doesn't change its handler in this version.
        if self.main_log_file_path != self.log_file_path:
            self.logger.log(
                f"Intended main log file: {self.main_log_file_path}. Initial logs are in: {self.log_file_path}",
                level="info",
            )

        self.logger.log(
            f"PipelineOrchestrator (數據整合平台 v15) initialized.", level="info"
        )
        self.logger.log(
            f"Run configuration: {json.dumps(config, indent=2, ensure_ascii=False)}",
            level="info",
        )
        self.logger.log(
            f"Resolved workspace paths: {json.dumps(self.paths, indent=2, ensure_ascii=False)}",
            level="info",
        )

        self.hw_monitor = HardwareMonitor(
            logger=self.logger, interval=self.config.get("hardware_monitor_interval", 2)
        )
        self.file_manifest = None # type: Optional[FileManifest]

    def _resolve_paths(self) -> Dict[str, str]:
        """根據主設定檔中的路徑配置，建構並回傳一個包含所有絕對路徑的字典。
        :meta private:
        :return: 一個包含所有已解析的絕對路徑的字典。
        :rtype: dict
        """
        paths_config = self.config.get("paths", {})
        project_name_for_paths = self.config.get(
            "project_name", "MyDataPipelineProject_v15"
        )
        base_workspace_parent = os.path.abspath(
            paths_config.get("local_workspace_base_parent", "pipeline_workspaces")
        )
        local_workspace_root = os.path.join(
            base_workspace_parent, project_name_for_paths
        )
        db_filename = self.config.get(
            "db_filename", f"{project_name_for_paths}_output.duckdb"
        )
        resolved = {
            "local_workspace_base_parent": base_workspace_parent,
            "local_workspace": local_workspace_root,
            "local_input": os.path.join(
                local_workspace_root,
                paths_config.get("input_dir_name", "01_input_files"),
            ),
            "local_staging": os.path.join(
                local_workspace_root,
                paths_config.get("staging_dir_name", "02_staging_parquet"),
            ),
            "local_database_dir": os.path.join(
                local_workspace_root,
                paths_config.get("database_dir_name", "03_database_output"),
            ),
            "local_db_file_path": os.path.join(
                local_workspace_root,
                paths_config.get("database_dir_name", "03_database_output"),
                db_filename,
            ),
            "local_manifests_dir": os.path.join(
                local_workspace_root,
                paths_config.get("manifests_dir_name", "04_manifests"),
            ),
            "file_manifest_path": os.path.join(
                local_workspace_root,
                paths_config.get("manifests_dir_name", "04_manifests"),
                "file_manifest.json",
            ),
            "local_failed_dir": os.path.join(
                local_workspace_root,
                paths_config.get("failed_files_dir_name", "05_failed_files"),
            ),
            "local_logs_dir": os.path.join(
                local_workspace_root, paths_config.get("logs_dir_name", "99_logs")
            ),
        }
        return resolved

    def _setup_workspace(self) -> None:
        """準備本地工作區。

        根據設定 (`config['recreate_workspace_on_run']`)，可選擇性地刪除並重建工作區。
        然後建立所有必要的子目錄 (input, staging, database, manifests, failed_files)，
        並初始化 `self.file_manifest` 以追蹤已處理檔案。

        :meta private:
        :raises Exception: 如果在建立目錄或初始化 FileManifest 時發生嚴重錯誤。
        """
        self.logger.log("步驟 A: 環境準備與本地工作區設定", level="step")
        recreate = self.config.get("recreate_workspace_on_run", True)
        if recreate and os.path.exists(self.paths["local_workspace"]):
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
        dirs_to_create = [
            self.paths["local_workspace"],
            self.paths["local_input"],
            self.paths["local_staging"],
            self.paths["local_database_dir"],
            self.paths["local_manifests_dir"],
            self.paths["local_failed_dir"],
            self.paths["local_logs_dir"],
        ]
        self.logger.log("開始建立工作區目錄結構...", level="info")
        for path_value in dirs_to_create:
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
                manifest_path=self.paths["file_manifest_path"]
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
        all_files_in_input = []
        for root, _, files in os.walk(self.paths["local_input"]):
            for name in files:
                if not name.startswith("."):
                    all_files_in_input.append(os.path.join(root, name))

        files_to_process_info = []
        if self.config.get("run_mode", "NORMAL").upper() == "BACKFILL":
            self.logger.log(
                "警告：您正處於【歷史回填模式】，將重新處理所有來源檔案。",
                level="warning",
            )
            for f_path in all_files_in_input:
                file_hash = self.file_manifest.get_file_hash(f_path)
                if file_hash:
                    files_to_process_info.append(
                        {"path": f_path, "hash": file_hash}
                    )
                else:
                    self.logger.log(
                        f"無法獲取檔案 {f_path} 的雜湊值，將跳過。", level="warning"
                    )
        else:  # NORMAL 模式
            for f_path in all_files_in_input:
                file_hash = self.file_manifest.get_file_hash(f_path)
                if file_hash and not self.file_manifest.has_been_processed(
                    file_hash
                ):
                    files_to_process_info.append(
                        {"path": f_path, "hash": file_hash}
                    )
                elif not file_hash:
                    self.logger.log(
                        f"無法獲取檔案 {f_path} 的雜湊值或檔案不存在，將跳過。",
                        level="warning",
                    )
        return files_to_process_info

    def _initialize_database(self) -> Optional["DuckDBPyConnection"]:
        """初始化 DuckDB 資料庫連線並確保所有必要的表格結構存在。"""
        db_conn = None
        cpu_count = os.cpu_count()
        max_workers = self.config.get("duckdb_settings", {}).get(
            "threads", cpu_count if cpu_count else 2
        )
        try:
            db_conn = duckdb.connect(
                database=self.paths["local_db_file_path"],
                read_only=False,
            )
            db_settings = self.config.get(
                "duckdb_settings",
                {"memory_limit_gb": 1, "threads": max_workers},
            )
            db_conn.execute(
                f"SET memory_limit='{db_settings.get('memory_limit_gb',1)}GB';"
            )
            db_conn.execute(
                f"SET threads={db_settings.get('threads',max_workers)};"
            )
            self.logger.log(
                f"DuckDB 資料庫連線已建立: {self.paths['local_db_file_path']}",
                level="success",
            )
            for schema_key, schema_val in self.config["schemas"].items():
                actual_table_name = schema_val["db_table_name"]
                sql_quoted_table_name = (
                    '"' + actual_table_name.replace('"', '""') + '"'
                )
                cols_map = schema_val.get("columns_map", {})
                if not cols_map:
                    continue
                cols_list_py = ['"id" BIGINT'] + [
                    f'"'
                    + col_name.replace('"', '""')
                    + f'''" {col_info["db_type"]}'''
                    for col_name, col_info in cols_map.items()
                ]
                cols_list_sql_str = ", ".join(cols_list_py)
                db_conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {sql_quoted_table_name} ({cols_list_sql_str});"
                )
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

    def _process_file_batch(self, batch_files_info: List[Dict[str, str]], max_workers: int) -> Set[str]:
        """並行處理一個批次的檔案，解析並回傳成功處理的檔案雜湊。"""
        parsed_results_in_batch = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    worker_process_file,
                    item["path"],
                    self.paths["local_staging"],
                    self.config["schemas"],
                ): item
                for item in batch_files_info
            }
            for future in as_completed(futures):
                item_info = futures[future]
                try:
                    result = future.result()
                    if result.get("status") == "group_result":
                        for sub_result in result.get("results", []):
                            parsed_results_in_batch.append(
                                {
                                    "item_info": item_info,
                                    "parse_output": sub_result,
                                }
                            )
                    else:
                        parsed_results_in_batch.append(
                            {"item_info": item_info, "parse_output": result}
                        )
                except Exception as e:
                    self.logger.log(
                        f"處理檔案 {item_info['path']} 時發生嚴重執行緒錯誤: {e}",
                        level="error",
                    )
                    self.logger.log(traceback.format_exc(), level="error")
                    parsed_results_in_batch.append(
                        {
                            "item_info": item_info,
                            "parse_output": {
                                "status": "error",
                                "file": os.path.basename(item_info["path"]),
                                "reason": str(e),
                            },
                        }
                    )
        current_batch_successful_hashes = set()
        for res_item in parsed_results_in_batch:
            original_item_info = res_item["item_info"]
            parse_output = res_item["parse_output"]
            status = parse_output.get("status")
            file_display_name = parse_output.get("file", os.path.basename(original_item_info["path"]))
            reason = parse_output.get("reason", "未知錯誤")

            if status == "success":
                self.logger.log(
                    f"↳ 成功 ({parse_output.get('table', 'N/A')}): {file_display_name} -> 暫存 {parse_output.get('count', 0)} 筆。",
                    level="success",
                )
                current_batch_successful_hashes.add(original_item_info["hash"])
            elif status == "skipped":
                self.logger.log(
                    f"↳ 跳過: {file_display_name} -> 原因: {reason}",
                    level="warning",
                )
            else:  # error
                self.logger.log(
                    f"↳ 失敗: {file_display_name} -> 原因: {reason}",
                    level="error",
                )
                source_file_to_copy = original_item_info["path"]
                failed_file_basename = os.path.basename(source_file_to_copy)
                try:
                    failed_dest_path = os.path.join(
                        self.paths["local_failed_dir"],
                        failed_file_basename,
                    )
                    if os.path.exists(source_file_to_copy):
                        if os.path.isdir(source_file_to_copy):
                            shutil.copytree(
                                source_file_to_copy,
                                failed_dest_path,
                                dirs_exist_ok=True,
                            )
                        else:
                            shutil.copy(source_file_to_copy, failed_dest_path)
                        self.logger.log(
                            f"  失敗的檔案 {failed_file_basename} 已複製到: {failed_dest_path}",
                            level="info",
                        )
                except Exception as copy_err:
                    self.logger.log(
                        f"  複製失敗檔案 {failed_file_basename} 時發生錯誤: {copy_err}",
                        level="warning",
                    )
        return current_batch_successful_hashes

    def _load_batch_to_db_and_update_manifest(self, db_conn: "DuckDBPyConnection", successful_hashes_in_batch: Set[str]) -> None:
        """將一個成功處理批次的數據載入資料庫並更新檔案處理清單。"""
        if not successful_hashes_in_batch:
            self.logger.log("沒有成功處理的檔案雜湊可供載入資料庫或更新 Manifest。", level="info")
            return

        load_staging_to_database(
            db_conn=db_conn,
            schemas_config=self.config["schemas"],
            local_staging_path=self.paths["local_staging"],
            logger=self.logger,
            conflict_strategy=self.config.get(
                "conflict_strategy", "IGNORE"
            ),
        )
        self.file_manifest.add_processed_hashes(
            successful_hashes_in_batch
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

        if self.config.get("cleanup_workspace_on_finish", False) and os.path.exists(
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

        self.hw_monitor.stop()
        final_log_path = getattr(self, "main_log_file_path", self.log_file_path)
        self.logger.log(
            f"{self.config.get('project_name', '數據整合平台 v15')} 已執行完畢。日誌檔案位於: {final_log_path}",
            level="step",
        )

    def run(self) -> None:
        """執行完整的數據整合管道。"""
        self.logger.log(
            f"{self.config.get('project_name', '數據整合平台 v15')} 執行開始...",
            level="step",
        )
        self.hw_monitor.start()
        db_conn: Optional["DuckDBPyConnection"] = None

        try:
            self._setup_workspace()

            files_to_process_info = self._get_files_to_process()

            if not files_to_process_info:
                self.logger.log("沒有新的檔案需要處理。", level="success")
            else:
                self.logger.log(
                    f"共發現 {len(files_to_process_info)} 個新檔案待處理。", level="info"
                )

                total_files = len(files_to_process_info)
                batch_size = self.config.get("micro_batch_size", 20)
                num_batches = (total_files + batch_size - 1) // batch_size
                successful_overall_hashes: Set[str] = set()

                cpu_count = os.cpu_count()
                max_workers = self.config.get("duckdb_settings", {}).get(
                    "threads", cpu_count if cpu_count else 2
                )

                for i in range(num_batches):
                    batch_start_index = i * batch_size
                    batch_end_index = (i + 1) * batch_size
                    batch_files_info = files_to_process_info[batch_start_index:batch_end_index]

                    self.logger.log(
                        f"微批次 {i + 1}/{num_batches}: 開始處理 {len(batch_files_info)} 個檔案...",
                        level="substep",
                    )

                    current_batch_successful_hashes = self._process_file_batch(batch_files_info, max_workers)

                    if current_batch_successful_hashes:
                        if db_conn is None: # Initialize DB only if there's something to load
                            db_conn = self._initialize_database()

                        if db_conn: # Proceed only if DB was successfully initialized
                            self._load_batch_to_db_and_update_manifest(db_conn, current_batch_successful_hashes)
                            successful_overall_hashes.update(current_batch_successful_hashes)
                        else:
                            self.logger.log(f"微批次 {i + 1}/{num_batches}: 資料庫未成功初始化，跳過此批次的資料載入。", level="error")
                    else:
                        self.logger.log(
                            f"微批次 {i + 1}/{num_batches} 中沒有成功解析的檔案可供載入資料庫。",
                            level="info", # Changed from warning, as it's a normal outcome for a batch.
                        )
                    self.logger.log(f"微批次 {i + 1}/{num_batches} 處理完畢。", level="info")

                self.logger.log(
                    f"所有 {num_batches} 個微批次均已處理完成。共 {len(successful_overall_hashes)} 個檔案成功處理並記錄到 Manifest。",
                    level="info",
                )

        except Exception as e:
            self.logger.log(f"管道執行過程中發生未預期的嚴重錯誤: {e}", level="error")
            self.logger.log(traceback.format_exc(), level="error")
        finally:
            self._teardown(db_conn)


if __name__ == "__main__":
    print(
        f"PipelineOrchestrator 模組 ({PipelineOrchestrator.__doc__.strip()[:10]}...) 可被導入。"
    )
    mock_config_for_init = {
        "project_name": "TestPipe_v15",
        "paths": {
            "local_workspace_base_parent": "./temp_workspaces",
        },
        "db_filename": "test_output.duckdb",
         "schemas": {
            "test_schema": {
                "db_table_name": "test_table",
                "columns_map": {"col1": {"db_type": "VARCHAR"}, "col2": {"db_type": "INTEGER"}}
            }
        }
    }
    print(f"\n--- 測試 PipelineOrchestrator 初始化 ---")
    try:
        orchestrator = PipelineOrchestrator(config=mock_config_for_init)
        print(f"Orchestrator 初始化成功.")
        print(
            f" - 主日誌檔案位於: {orchestrator.main_log_file_path}"
        )
        print(f" - 工作區路徑:")
        for key, path_val in orchestrator.paths.items():
            print(f"    - {key}: {path_val}")
    except Exception as e:
        print(f"Orchestrator 測試時發生錯誤: {e}")
