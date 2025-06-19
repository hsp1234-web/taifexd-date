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
# import json # 用於載入 schemas.json 及 config 輸出 (若需要) # REMOVED
# from pathlib import Path # 用於處理檔案路徑 # REMOVED
import sys # 用於錯誤輸出 exit
from src.data_pipeline_v15.pipeline_orchestrator import PipelineOrchestrator
# from src.data_pipeline_v15.core import constants # 修改：導入常數 # REMOVED


def parse_arguments() -> argparse.Namespace:
    """解析命令列傳入的參數。

    使用 argparse 模組定義並解析 --project-folder-name, --database-name,
    --log-name, 和 --zip-files 等參數。

    :return: 解析後的命令列參數命名空間物件。
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="數據整合平台 v15 命令列執行器。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,  # 顯示預設值
    )
    parser.add_argument(
        "--project-folder-name",
        type=str,
        default='MyTaifexDataProject',
        help="專案資料夾的名稱，所有輸出（資料庫、日誌等）將存放在此資料夾下。",
    )
    parser.add_argument(
        "--database-name",
        type=str,
        default='processed_data.duckdb',
        help="輸出資料庫檔案的名稱。",
    )
    parser.add_argument(
        "--log-name",
        type=str,
        default='pipeline.log',
        help="輸出日誌檔案的名稱。",
    )
    parser.add_argument(
        "--zip-files",
        type=str,
        default='',
        help="指定要處理的ZIP檔案列表，以逗號分隔。如果為空，則處理所有找到的ZIP檔案。",
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='啟用詳細的除錯日誌與硬體監控模式。'
    )
    args = parser.parse_args()
    return args


# REMOVED build_config_from_args function


def main():
    """主執行函式，協調整個應用程式的啟動流程。

    步驟包括：
    1. 解析命令列參數。
    2. 實例化 PipelineOrchestrator 並直接傳遞參數。
    3. 呼叫 Orchestrator 的 run() 方法以啟動數據處理管道。
    4. 處理潛在的例外並輸出最終訊息。
    """
    try:
        args = parse_arguments()
        orchestrator = PipelineOrchestrator(
            project_folder_name=args.project_folder_name,
            database_name=args.database_name,
            log_name=args.log_name,
            zip_files=args.zip_files,
            # TODO: Add other necessary orchestrator params like run_mode, conflict_strategy, schemas, etc.
            # These might come from a config file or have defaults in orchestrator itself.
            debug_mode=args.debug # Pass the debug flag
        )
        orchestrator.run()
        # Assuming log_file_path is an accessible attribute after run or initialization
        final_log_path = orchestrator.log_file_path
        print(f"INFO: 管道執行成功完成。主要日誌檔案位於: {final_log_path}")

    except SystemExit:
        pass
    except Exception as e:
        print(f"CRITICAL: 管道執行過程中遭遇無法恢復的錯誤: {e}")
        # Consider logging this to the (now dynamic) log file if possible, or stderr.
        print(f"CRITICAL: 請檢查日誌檔案以獲取詳細的錯誤追蹤訊息。")
        sys.exit(1)


if __name__ == "__main__":
    main()
