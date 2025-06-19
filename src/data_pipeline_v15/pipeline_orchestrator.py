import os
import shutil
import time
import json
import yaml # Added import
from datetime import datetime
from pathlib import Path

import duckdb

# Removed direct imports of directory constants
from .core.constants import (
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
        config_file_path: str, # New parameter for config file
        base_path: str,
        project_folder_name_override: str = None, # Allow override from main.py args
        database_name_override: str = None,       # Allow override from main.py args
        log_name_override: str = None,             # Allow override from main.py args
        target_zip_files: str,
        debug_mode: bool = False,
        schemas_file_path: str = None, # Optional path for schemas
    ):
        """
        初始化協調器。

        Args:
            config_file_path (str): 設定檔 (config.yaml) 的路徑。
            base_path (str): 專案的基礎路徑 (例如 /content 或 /content/drive/MyDrive)。
            project_folder_name_override (str, optional): 覆寫設定檔中的專案資料夾名稱。
            database_name_override (str, optional): 覆寫設定檔中的資料庫檔案名稱。
            log_name_override (str, optional): 覆寫設定檔中的日誌檔案名稱。
            target_zip_files (str): 指定要處理的 ZIP 檔案列表（以逗號分隔），或為空。
            debug_mode (bool, optional): 是否啟用除錯模式。預設為 False。
            schemas_file_path (str, optional): schemas.json 的可選路徑。如果為 None，則使用預設路徑。
        """
        # --- 載入設定檔 ---
        self.config = self._load_config(config_file_path)

        # --- 參數設定 (優先使用覆蓋值，其次是設定檔，最後是預設值) ---
        self.project_folder_name = project_folder_name_override or self.config.get("project_folder", "MyTaifexDataProject")
        self.database_name = database_name_override or self.config.get("database_name", "processed_data.duckdb")
        self.log_name = log_name_override or self.config.get("log_name", "pipeline.log")
        self.directories_config = self.config.get("directories", {})
        self.local_workspace_root = Path(self.config.get("local_workspace", "/tmp/taifex_data_workspace"))
        # remote_base_path is the equivalent of the old base_path for GDrive, not read from main.py args anymore directly for this var
        self.remote_base_path = Path(self.config.get("remote_base_path", "/content/drive/MyDrive"))


        # --- 本地路徑設定 ---
        # base_path from main.py is now considered the root for the *remote* project folder if not using GDrive,
        # or the parent of MyDrive if using GDrive.
        # For local-first, the primary operations happen in local_workspace_root.
        self.local_project_path = self.local_workspace_root / self.project_folder_name
        self.local_input_path = self.local_project_path / self.directories_config.get("input", "00_input")
        self.local_processed_path = self.local_project_path / self.directories_config.get("processed", "01_processed")
        self.local_archive_path = self.local_project_path / self.directories_config.get("archive", "02_archive")
        self.local_quarantine_path = self.local_project_path / self.directories_config.get("quarantine", "03_quarantine")
        self.local_db_path = self.local_project_path / self.directories_config.get("db", "98_database")
        self.local_log_path = self.local_project_path / self.directories_config.get("log", "99_logs")
        self.local_database_file = self.local_db_path / self.database_name
        self.local_manifest_file = self.local_archive_path / constants.MANIFEST_FILE

        # --- 遠端路徑設定 ---
        # The 'base_path' argument passed to __init__ is used here to construct the root of the remote project.
        # This allows for flexibility (e.g. if --no-gdrive is used, base_path is /content)
        self.remote_project_path = Path(base_path) / self.project_folder_name # base_path is /content/drive/MyDrive or /content
        self.remote_input_path = self.remote_project_path / self.directories_config.get("input", "00_input")
        self.remote_processed_path = self.remote_project_path / self.directories_config.get("processed", "01_processed")
        self.remote_archive_path = self.remote_project_path / self.directories_config.get("archive", "02_archive")
        self.remote_quarantine_path = self.remote_project_path / self.directories_config.get("quarantine", "03_quarantine")
        self.remote_db_path = self.remote_project_path / self.directories_config.get("db", "98_database")
        self.remote_log_path = self.remote_project_path / self.directories_config.get("log", "99_logs")
        self.remote_database_file = self.remote_db_path / self.database_name
        self.remote_manifest_file = self.remote_archive_path / constants.MANIFEST_FILE

        self.debug_mode = debug_mode

        # Logger setup now uses local log path. Logs will be synced back.
        Path(self.local_log_path).mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger(self.local_log_path, self.log_name, debug_mode) # Use self.log_name

        # _setup_directories will now primarily ensure local structure. Remote structure is assumed or handled by sync.
        # os.makedirs(self.local_db_path, exist_ok=True) # Done in _setup_local_directories

        # --- 載入 Schemas 設定 ---
        # Use provided schemas_file_path if available, otherwise default
        effective_schemas_file_path_str = None
        if schemas_file_path:
            effective_schemas_file_path_str = str(schemas_file_path)
            self.logger.info(f"使用提供的 schemas 路徑: '{effective_schemas_file_path_str}'")
        else:
            # Default path logic for schemas.json, relative to project root (which is base_path/project_folder)
            # Assuming config.yaml is at the root of the project_path for consistency.
            # If schemas.json is meant to be outside project_path, this needs adjustment.
            # For now, let's assume it's within the project structure, e.g., in a 'config' subfolder of project_path.
            # However, the original code put it outside the data_pipeline_v15 package,
            # in a 'config' folder at the repository root.
            # Let's keep that logic for now. The project_root is base_path.
            # No, project_root should be where pyproject.toml or .git is.
            # Let's assume config/schemas.json is relative to the project_path for now.
            # If config.yaml is at project_path, then schemas.json could be project_path/config/schemas.json

            # Re-evaluating the original logic: Path(__file__).parent.parent.parent.parent
            # This means if pipeline_orchestrator.py is in src/data_pipeline_v15/pipeline_orchestrator.py
            # then parent is src/data_pipeline_v15
            # parent.parent is src
            # parent.parent.parent is the repo root where config/schemas.json was.
            # This implies base_path might not be the repo root.
            # Let's clarify: if config.yaml is at repo root, then schemas.json is also at repo root in a config folder.
            # The `base_path` is where the `project_folder_name` is created.
            # So, if `base_path` is `/content/drive/MyDrive`, and `project_folder_name` is `MyTaifexDataProject`,
            # then `project_path` is `/content/drive/MyDrive/MyTaifexDataProject`.
            # If `config.yaml` is expected to be at `project_path/config.yaml`,
            # then `schemas.json` could be at `project_path/config/schemas.json`.
            # Or, if `config.yaml` is at the repo root (where `main.py` is), then `schemas.json` is `repo_root/config/schemas.json`.
            # The current `main.py` implies `CONFIG_FILE = "config.yaml"` is at the same level as `main.py`.
            # Let's assume the repo root is where `main.py` and `config.yaml` are.
            # `base_path` is for Colab/Drive specifics. The actual project structure is created *within* `base_path/_project_folder`.

            # Let's assume schemas.json is located relative to where config.yaml is.
            # If config_file_path is "config.yaml", then schemas.json is "config/schemas.json"
            # This seems the most consistent approach.
            config_dir = Path(config_file_path).parent
            default_schemas_path = config_dir / "config" / "schemas.json" # This implies config.yaml is NOT in the config folder
                                                                    # but schemas.json is in a subfolder 'config' relative to config.yaml's location
            # This was the previous structure: project_root / "config" / "schemas.json"
            # If config.yaml is at the root, then schemas.json is at ./config/schemas.json
            # This seems correct.

            current_processing_dir = Path(os.getcwd()) # Where the script is run from
            # If main.py runs from repo root, and config.yaml is there, then config_file_path is "config.yaml"
            # default_schemas_path would be "config/schemas.json"
            # This matches the original structure.

            default_schemas_path = Path("config") / "schemas.json" # Relative to where main.py is run
            effective_schemas_file_path_str = str(default_schemas_path)
            self.logger.info(f"使用預設的 schemas 路徑: '{effective_schemas_file_path_str}'")


        self.schemas_config = {}
        if os.path.exists(effective_schemas_file_path_str):
            try:
                with open(effective_schemas_file_path_str, "r", encoding="utf-8") as f:
                    self.schemas_config = json.load(f)
                self.logger.info(f"成功從 '{effective_schemas_file_path_str}' 載入 schemas 設定。")
            except json.JSONDecodeError as e:
                self.logger.error(f"解析 schemas 設定檔 '{effective_schemas_file_path_str}' 失敗: {e}")
                self.schemas_config = {} # Keep it as an empty dict on error
            except Exception as e:
                self.logger.error(f"讀取 schemas 設定檔 '{effective_schemas_file_path_str}' 時發生其他錯誤: {e}")
                self.schemas_config = {} # Keep it as an empty dict on error
        else:
            self.logger.warning(f"Schemas 設定檔 '{effective_schemas_file_path_str}' 不存在。FileParser 將使用空設定。") # Changed to warning
            self.schemas_config = {}


        # --- 模組初始化 ---
        # ManifestManager and DatabaseLoader now operate on local paths
        self.manifest_manager = ManifestManager(manifest_path=self.local_manifest_file, logger=self.logger)
        self.file_parser = FileParser(self.manifest_manager, self.logger, self.schemas_config) # processed_path here is for internal logic if needed, actual move is local
        self.db_loader = DatabaseLoader(self.local_database_file, self.logger)

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

    def _load_config(self, config_file_path: str) -> dict:
        """載入 YAML 設定檔"""
        try:
            with open(config_file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Logger might not be initialized yet if this is called early.
            # Consider logging this error after logger setup, or print.
            print(f"警告: 設定檔 {config_file_path} 未找到。將使用空的設定字典。")
            return {}
        except yaml.YAMLError as e:
            print(f"錯誤: 設定檔 {config_file_path} 解析失敗: {e}")
            return {}

    def _setup_local_directories(self):
        """建立所有必要的本地資料夾結構"""
        self.logger.info(f"正在設定本地工作區資料夾結構於: {self.local_project_path}")
        directories = [
            self.local_input_path,
            self.local_processed_path,
            self.local_archive_path,
            self.local_quarantine_path,
            self.local_db_path,
            self.local_log_path, # Already created for logger, but good to have in list
        ]
        # Create project root first
        self.local_project_path.mkdir(parents=True, exist_ok=True)
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"成功建立或確認本地目錄存在：{directory}")
            except OSError as e:
                self.logger.error(f"建立本地目錄失敗：{directory}，錯誤：{e}")
                raise # Re-raise to stop execution if essential dirs can't be made
        self.logger.info("✅ 本地工作區資料夾結構設定完成。")

    def _create_remote_directories_if_not_exist(self):
        """如果遠端目錄不存在，則建立它們"""
        self.logger.info(f"檢查/建立遠端專案資料夾結構於: {self.remote_project_path}")
        remote_dirs_to_check = [
            self.remote_project_path,
            self.remote_input_path,
            self.remote_processed_path,
            self.remote_archive_path,
            self.remote_quarantine_path,
            self.remote_db_path,
            self.remote_log_path,
        ]
        for remote_dir in remote_dirs_to_check:
            try:
                remote_dir.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"成功建立或確認遠端目錄存在：{remote_dir}")
            except Exception as e:
                # Log error but don't necessarily stop, as some might be non-critical (e.g. GDrive might auto-create)
                self.logger.error(f"嘗試建立遠端目錄 {remote_dir} 失敗: {e}. 可能需要手動建立。")


    def _sync_file(self, source_file: Path, dest_file: Path, direction: str):
        """同步單個檔案，如果來源存在。 direction 是 'to_local' 或 'to_remote'."""
        if source_file.exists():
            try:
                # Ensure destination directory exists
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_file, dest_file)
                self.logger.info(f"同步 ({direction}): '{source_file}' -> '{dest_file}'")
            except Exception as e:
                self.logger.error(f"同步 ({direction}) 檔案 '{source_file}' 至 '{dest_file}' 失敗: {e}")
        else:
            self.logger.info(f"同步 ({direction}): 來源檔案 '{source_file}' 不存在，跳過。")

    def _sync_directory_content(self, source_dir: Path, dest_dir: Path, direction: str):
        """同步目錄內容 (非遞迴，僅檔案)。 direction 是 'to_local' 或 'to_remote'."""
        if not source_dir.exists():
            self.logger.warning(f"同步 ({direction}): 來源目錄 '{source_dir}' 不存在，跳過。")
            return

        dest_dir.mkdir(parents=True, exist_ok=True) # Ensure destination directory exists

        for item in source_dir.iterdir():
            if item.is_file():
                self._sync_file(item, dest_dir / item.name, direction)
            # Optionally handle subdirectories if recursive sync is needed for some folders

    def run(self):
        """執行完整數據管線，採用本地優先工作流程。"""
        start_time = time.time()
        self.logger.info(f"====== 數據管線 (本地優先) 開始執行 @ {datetime.now():%Y-%m-%d %H:%M:%S} ======")
        if self.debug_mode:
            self.logger.debug(get_hardware_usage("管線啟動前 (本地優先)"))

        try:
            # A. 建立本地工作区和必要目录
            self._setup_local_directories()
            # 確保遠端目錄也存在，以便同步
            self._create_remote_directories_if_not_exist()


            # B. 啟動時同步 (Sync from Remote to Local)
            self.logger.info("--- 開始啟動時同步 (遠端 -> 本地) ---")
            # 同步輸入檔案
            self._sync_directory_content(self.remote_input_path, self.local_input_path, "to_local")
            # 同步資料庫檔案
            self._sync_file(self.remote_database_file, self.local_database_file, "to_local")
            # 同步 Manifest 檔案
            self._sync_file(self.remote_manifest_file, self.local_manifest_file, "to_local")
            self.logger.info("--- 啟動時同步完成 ---")

            # C. 本地執行 (Current Logic on Local Paths)
            self.logger.info("正在載入處理紀錄 (manifest) 從本地...")
            self.manifest_manager.load_or_create_manifest() # Operates on local_manifest_file

            self.logger.info(f"正在掃描本地輸入資料夾：{self.local_input_path}")
            if not self.local_input_path.exists():
                self.logger.warning(f"本地輸入資料夾 {self.local_input_path} 不存在，無法處理。")
                return # Early exit if no input dir

            all_files_in_input = [f.name for f in self.local_input_path.iterdir() if f.is_file()]

            if self.target_files:
                files_to_process = [f for f in all_files_in_input if f in self.target_files]
                self.logger.info(f"根據指定，將處理 {len(files_to_process)} 個本地檔案。")
            else:
                files_to_process = all_files_in_input
                self.logger.info(f"將處理全部 {len(files_to_process)} 個本地檔案。")

            if not files_to_process:
                self.logger.warning("本地輸入資料夾中沒有找到任何要處理的檔案。")
                # No return here, as we still want to sync back results (e.g. empty db, logs)

            for filename in files_to_process:
                local_file_path = self.local_input_path / filename
                # ... (rest of the file processing logic from the original run method) ...
                # IMPORTANT: Ensure all paths used within this loop are LOCAL paths
                # e.g., self.local_processed_path, self.local_quarantine_path

                self.logger.info(f"--- 開始處理檔案 (本地): {filename} ---")
                file_hash = ManifestManager.get_file_hash(str(local_file_path))
                if not file_hash:
                    self.logger.error(f"無法計算檔案雜湊值 (本地): {local_file_path}。跳過。")
                    self.manifest_manager.update_manifest(str(local_file_path), STATUS_ERROR, "無法計算檔案雜湊值", original_filename=filename)
                    try:
                        shutil.move(str(local_file_path), str(self.local_quarantine_path / filename))
                        self.logger.warning(f"檔案 '{filename}' 因無法取得雜湊值已移動至本地隔離區。")
                    except Exception as move_e:
                        self.logger.error(f"移動檔案 '{filename}' 至本地隔離區失敗: {move_e}")
                    continue

                if self.manifest_manager.has_been_processed(file_hash):
                    self.logger.warning(f"檔案 '{filename}' (雜湊: {file_hash}) 已被處理過，將跳過。")
                    continue

                # parse_file now takes local_processed_path for its internal temp Parquet generation
                parse_result = self.file_parser.parse_file(str(local_file_path), str(self.local_processed_path))
                overall_status = parse_result.get(constants.KEY_STATUS, constants.STATUS_ERROR)
                overall_message = parse_result.get(constants.KEY_REASON, f"檔案 '{filename}' 處理時遇到未知狀況。")
                move_to_local_processed = False

                if overall_status == constants.STATUS_GROUP_RESULT:
                    # ... (logic for group results, ensuring paths are local) ...
                    all_sub_results = parse_result.get(constants.KEY_RESULTS, [])
                    successful_sub_files = 0
                    failed_sub_files = 0
                    at_least_one_db_loaded = False
                    for item_res in all_sub_results:
                        item_status = item_res.get(constants.KEY_STATUS)
                        item_parquet_path_str = item_res.get(constants.KEY_PATH) # This is a path to a .parquet file
                        item_table = item_res.get(constants.KEY_TABLE)
                        item_name = item_res.get(constants.KEY_FILE, "未知子項目")
                        if item_status == constants.STATUS_SUCCESS and item_parquet_path_str and item_table:
                            try:
                                self.db_loader.load_parquet(item_table, item_parquet_path_str) # db_loader uses local_database_file
                                self.logger.info(f"子項目 '{item_name}' 資料成功載入本地資料庫。")
                                successful_sub_files +=1
                                at_least_one_db_loaded = True
                            except Exception as db_e:
                                self.logger.error(f"子項目 '{item_name}' 資料載入本地資料庫失敗: {db_e}")
                                failed_sub_files +=1
                        # ... (handle other sub-item statuses) ...
                    if at_least_one_db_loaded:
                        move_to_local_processed = True
                        overall_message = f"ZIP '{filename}' 部分或全部成功處理。成功: {successful_sub_files}, 失敗: {failed_sub_files}."
                        overall_status = constants.STATUS_SUCCESS # Mark parent ZIP as success if one sub-item succeeded
                    else:
                        overall_message = f"ZIP '{filename}' 所有子項目處理失敗或無資料可載入。"
                        overall_status = constants.STATUS_ERROR

                elif overall_status == constants.STATUS_SUCCESS:
                    parquet_path_str = parse_result.get(constants.KEY_PATH)
                    table_name = parse_result.get(constants.KEY_TABLE)
                    if parquet_path_str and table_name:
                        try:
                            self.db_loader.load_parquet(table_name, parquet_path_str)
                            self.logger.info(f"檔案 '{filename}' 資料成功載入本地資料庫。")
                            move_to_local_processed = True
                        except Exception as db_e:
                            self.logger.error(f"檔案 '{filename}' 資料載入本地資料庫失敗: {db_e}")
                            overall_status = constants.STATUS_ERROR
                            overall_message = f"資料庫載入失敗: {db_e}"
                    else: # Should not happen if status is success
                        overall_status = constants.STATUS_ERROR
                        overall_message = "成功狀態但缺少 Parquet 路徑或表格名稱。"

                if move_to_local_processed:
                    try:
                        shutil.move(str(local_file_path), str(self.local_processed_path / filename))
                        self.logger.info(f"檔案 '{filename}' 已處理並移動至本地 -> {self.local_processed_path}")
                    except Exception as move_e:
                        self.logger.error(f"移動檔案 '{filename}' 至本地 {self.local_processed_path} 失敗: {move_e}")
                        overall_status = constants.STATUS_ERROR
                        overall_message += f" 但移至本地已處理資料夾失敗: {move_e}"
                else: # Error or skipped
                    try:
                        shutil.move(str(local_file_path), str(self.local_quarantine_path / filename))
                        self.logger.warning(f"檔案 '{filename}' 移動至本地隔離區 -> {self.local_quarantine_path} (原因: {overall_message})")
                    except Exception as move_e:
                        self.logger.error(f"移動檔案 '{filename}' 至本地 {self.local_quarantine_path} 失敗: {move_e}")

                self.manifest_manager.update_manifest(str(local_file_path), overall_status, overall_message, original_filename=filename, file_hash_override=file_hash)
                self.logger.info(f"--- 檔案處理完畢 (本地): {filename} (狀態: {overall_status}) ---")

            # D. 結束時同步 (Sync from Local to Remote) - After successful processing
            self.logger.info("--- 開始結束時同步 (本地 -> 遠端) ---")
            self._sync_file(self.local_database_file, self.remote_database_file, "to_remote")
            self._sync_file(self.local_manifest_file, self.remote_manifest_file, "to_remote")
            self._sync_directory_content(self.local_processed_path, self.remote_processed_path, "to_remote")
            self._sync_directory_content(self.local_quarantine_path, self.remote_quarantine_path, "to_remote")
            # Log sync will be in finally
            self.logger.info("--- 結束時同步完成 (資料檔案) ---")

        except Exception as e:
            self.logger.critical(f"管線執行過程中發生無法恢復的錯誤: {e}", exc_info=True)
            # Depending on the error, some local files might be inconsistent.
            # The current setup syncs logs in finally, but other data only on success.
        finally:
            # Always try to sync logs back
            self.logger.info("--- 開始日誌同步 (本地 -> 遠端) ---")
            self._sync_directory_content(self.local_log_path, self.remote_log_path, "to_remote")
            self.logger.info("--- 日誌同步完成 ---")

            # E. 清理 (Cleanup Local Workspace)
            if self.local_project_path.exists():
                try:
                    shutil.rmtree(self.local_project_path)
                    self.logger.info(f"✅ 本地工作區 {self.local_project_path} 已成功清理。")
                except Exception as e:
                    self.logger.error(f"清理本地工作區 {self.local_project_path} 失敗: {e}")
            else:
                self.logger.info(f"本地工作區 {self.local_project_path} 未找到，無需清理。")

            end_time = time.time()
            self.logger.info(f"====== 數據管線 (本地優先) 執行完畢 @ {datetime.now():%Y-%m-%d %H:%M:%S} ======")
            self.logger.info(f"總耗時: {end_time - start_time:.2f} 秒")
            if self.debug_mode:
                self.logger.debug(get_hardware_usage("管線結束後 (本地優先)"))

            if self.db_loader: # Ensure db_loader was initialized
                self.db_loader.close_connection()
                self.logger.info("本地資料庫連接已關閉。")
