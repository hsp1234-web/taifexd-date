import argparse
import os
from src.data_pipeline_v15.pipeline_orchestrator import PipelineOrchestrator

def parse_arguments():
    """
    解析命令列參數。

    Returns:
        argparse.Namespace: 包含解析後參數的物件。
    """
    parser = argparse.ArgumentParser(description="數據整合管線 v16 - 命令列啟動器")
    parser.add_argument(
        "--project-folder-name",
        type=str,
        default="MyTaifexDataProject",
        help="在 Google Drive 或本地環境中建立的專案主資料夾名稱。"
    )
    parser.add_argument(
        "--database-name",
        type=str,
        default="processed_data.duckdb",
        help="輸出的 DuckDB 資料庫檔案名稱。"
    )
    parser.add_argument(
        "--log-name",
        type=str,
        default="pipeline.log",
        help="輸出的日誌檔案名稱。"
    )
    parser.add_argument(
        "--zip-files",
        type=str,
        default="",
        help="指定要處理的特定ZIP檔案，多個檔案請用逗號分隔。留空表示處理全部。"
    )
    parser.add_argument(
        "--no-gdrive",
        action='store_true',
        help="啟用此旗標以在 Colab 本地臨時空間執行，不與 Google Drive 同步。"
    )
    parser.add_argument(
        "--debug",
        action='store_true',
        help="啟用除錯模式，將會輸出更詳細的日誌資訊。"
    )
    return parser.parse_args()

def main():
    """
    主執行函式。
    """
    args = parse_arguments()

    # 根據 --no-gdrive 旗標決定基礎路徑
    if args.no_gdrive:
        base_path = "/content"
        print("ℹ️ 偵測到 --no-gdrive 旗標，將在本地模式下執行。")
    else:
        base_path = "/content/drive/MyDrive"
        print("ℹ️ 未偵測到 --no-gdrive 旗標，將在 Google Drive 整合模式下執行。")
    
    # 建立管線協調器並執行
    orchestrator = PipelineOrchestrator(
        base_path=base_path,
        project_folder_name=args.project_folder_name,
        database_name=args.database_name,
        log_name=args.log_name,
        target_zip_files=args.zip_files,
        debug_mode=args.debug
    )
    orchestrator.run()

if __name__ == "__main__":
    main()
