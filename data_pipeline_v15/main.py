# -*- coding: utf-8 -*-
"""數據整合平台 v15 命令列執行入口。

此腳本負責解析命令列參數，建構設定字典 (config)，
並初始化及執行 PipelineOrchestrator 來啟動整個數據處理流程。

主要功能包括：
- 解析 --project_name, --run_mode, --conflict_strategy, --workspace_path 等參數。
- 根據參數及設定檔 (schemas.json) 建構傳遞給 Orchestrator 的 config。
- 實例化 PipelineOrchestrator 並呼叫其 run() 方法。

執行範例 (假設已安裝 Poetry 並在專案根目錄):
  poetry run python main.py --project_name MyTaifexProject
  poetry run python main.py --project_name AnotherProject --run_mode BACKFILL --conflict_strategy IGNORE --workspace_path ./custom_workspace
"""

import argparse
import os
import json # 用於載入 schemas.json 及 config 輸出 (若需要)
from pathlib import Path # 用於處理檔案路徑
import sys # 用於錯誤輸出 exit
from src.data_pipeline_v15.pipeline_orchestrator import PipelineOrchestrator
from src.data_pipeline_v15.core import constants # 修改：導入常數


def parse_arguments() -> argparse.Namespace:
    """解析命令列傳入的參數。

    使用 argparse 模組定義並解析 --project_name, --run_mode,
    --conflict_strategy 和 --workspace_path 等參數。

    :return: 解析後的命令列參數命名空間物件。
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="數據整合平台 v15 命令列執行器。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,  # 顯示預設值
    )
    parser.add_argument(
        "--project_name",
        type=str,
        required=True,
        help="專案名稱，用於區分不同的數據處理專案及其相關路徑。",
    )
    parser.add_argument(
        "--run_mode",
        type=str,
        choices=[constants.RUN_MODE_NORMAL, constants.RUN_MODE_BACKFILL], # 修改：使用常數
        default=constants.RUN_MODE_NORMAL, # 修改：使用常數
        help="執行模式：'NORMAL' (僅處理新檔案) 或 'BACKFILL' (重新處理所有檔案)。",
    )
    parser.add_argument(
        "--conflict_strategy",
        type=str,
        choices=[constants.CONFLICT_STRATEGY_REPLACE, constants.CONFLICT_STRATEGY_IGNORE], # 修改：使用常數
        default=constants.CONFLICT_STRATEGY_REPLACE, # 修改：使用常數
        help="數據衝突時的解決策略：'REPLACE' (取代舊資料) 或 'IGNORE' (忽略新資料)。",
    )
    parser.add_argument(
        "--workspace_path",
        type=str,
        default=None,  # Default is None, handled in build_config_from_args
        help="指定本地工作區的根路徑。若未提供，則預設在當前目錄下，以專案名稱加上 '_workspace_v15' 後綴建立資料夾。",
    )
    args = parser.parse_args()
    return args


def build_config_from_args(args: argparse.Namespace) -> dict:
    """根據解析後的命令列參數和預設值來建構設定字典 (config)。

    此函式會整合命令列輸入與程式內建的預設設定（如資料綱要、
    DuckDB 參數、路徑子目錄名稱等）來產生 PipelineOrchestrator
    所需的完整組態。

    :param args: 由 argparse 解析產生的命令列參數物件。
    :type args: argparse.Namespace
    :return: 建構完成的設定字典。
    :rtype: dict
    """

    project_name = args.project_name

    if args.workspace_path:
        local_workspace_root = os.path.abspath(args.workspace_path)
    else:
        local_workspace_root = os.path.abspath(
            os.path.join(".", f"{project_name}_workspace_v15")
        )

    loaded_schemas = None
    try:
        schemas_file_path = Path(__file__).parent / 'config' / 'schemas.json'
        with open(schemas_file_path, 'r', encoding='utf-8') as f:
            loaded_schemas = json.load(f)
        # 從外部檔案 config/schemas.json 成功載入資料綱要設定
    except FileNotFoundError:
        print(f"錯誤：資料綱要設定檔 {schemas_file_path} 未找到。", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"錯誤：解析資料綱要設定檔 {schemas_file_path} 時發生錯誤：{e}", file=sys.stderr)
        sys.exit(1)

    config = {
        "project_name": project_name,
        "run_mode": args.run_mode.upper(),
        "conflict_strategy": args.conflict_strategy.upper(),
        "paths": {
            "local_workspace": local_workspace_root,
            "input_dir_name": constants.DIR_NAME_INPUT, # 修改：使用常數
            "staging_dir_name": constants.DIR_NAME_STAGING, # 修改：使用常數
            "database_dir_name": constants.DIR_NAME_DATABASE, # 修改：使用常數
            "manifests_dir_name": constants.DIR_NAME_MANIFESTS, # 修改：使用常數
            "failed_files_dir_name": constants.DIR_NAME_FAILED, # 修改：使用常數
            "logs_dir_name": constants.DIR_NAME_LOGS, # 修改：使用常數
        },
        "db_filename": f"{project_name.lower().replace(' ', '_')}_output.duckdb",
        "duckdb_settings": {
            "memory_limit_gb": int(
                os.environ.get("DUCKDB_MEMORY_LIMIT_GB", 4)
            ),
            "threads": int(
                os.environ.get("DUCKDB_THREADS", os.cpu_count() or 2)
            ),
        },
        "schemas": loaded_schemas,
        "micro_batch_size": 20,
        "hardware_monitor_interval": 5,
        "recreate_workspace_on_run": True,
        "cleanup_workspace_on_finish": False,
    }
    return config


def main():
    """主執行函式，協調整個應用程式的啟動流程。

    步驟包括：
    1. 解析命令列參數。
    2. 根據參數建構設定檔。
    3. 實例化 PipelineOrchestrator。
    4. 呼叫 Orchestrator 的 run() 方法以啟動數據處理管道。
    5. 處理潛在的例外並輸出最終訊息。
    """
    try:
        args = parse_arguments()
        config = build_config_from_args(args)
        orchestrator = PipelineOrchestrator(config=config)
        orchestrator.run()
        final_log_path = getattr(
            orchestrator, "main_log_file_path", orchestrator.log_file_path
        )
        print(f"INFO: 管道執行成功完成。主要日誌檔案位於: {final_log_path}")

    except SystemExit:
        pass
    except Exception as e:
        print(f"CRITICAL: 管道執行過程中遭遇無法恢復的錯誤: {e}")
        print(f"CRITICAL: 請檢查日誌檔案以獲取詳細的錯誤追蹤訊息。")
        sys.exit(1)


if __name__ == "__main__":
    main()
