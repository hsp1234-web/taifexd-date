# -*- coding: utf-8 -*-
import pytest
import os
import shutil
import zipfile
import hashlib
import json
import pandas as pd
import duckdb
from datetime import datetime

# 從 src 目錄匯入 PipelineOrchestrator 和相關常數
# (假設 PipelineOrchestrator 在 src/data_pipeline_v15/pipeline_orchestrator.py)
# (假設 constants 在 src/data_pipeline_v15/core/constants.py)
from src.data_pipeline_v15.pipeline_orchestrator import PipelineOrchestrator
from src.data_pipeline_v15.core import constants as pipeline_constants

# 之後會在此處加入測試輔助函式和測試函式

def _create_dummy_csv_content(file_type: str = "normal", rows: int = 3) -> str:
    """
    產生模擬 CSV 檔案的內容。

    Args:
        file_type (str): "normal" 表示正常符合 schema 的 CSV，
                         "corrupted_format" 表示包含無法解碼字元的 CSV，
                         "corrupted_structure" 表示欄位完全不符的 CSV。
        rows (int): 要產生的資料行數 (不含標頭)。

    Returns:
        str: CSV 格式的字串。
    """
    if file_type == "corrupted_format":
        # 包含 Big5 和 UTF-8 都可能無法直接處理的字元組合，或刻意製造的編碼錯誤
        # 例如，一個有效的 Big5 字元後面跟著一個無效的 Big5 序列
        # 這裡用一個簡單的例子，實際可能需要更複雜的构造
        return "欄位1,欄位2\n測試\xc3\x28資料,123\n另一行,\xff\xfe\n"

    header = "交易日期,契約,開盤價,最高價,最低價,收盤價,成交量"
    if file_type == "corrupted_structure":
        header = "完全不相關的欄位A,另一個怪欄位B,奇怪的C"
        lines = [header]
        for i in range(rows):
            lines.append(f"隨便內容{i},亂寫的{i*2},測試{i*3}")
        return "\n".join(lines)

    # normal file_type
    lines = [header]
    for i in range(rows):
        date_str = f"202301{i+1:02d}"
        lines.append(f"{date_str},TXF,1500{i},1501{i},1499{i},1500{i},{1000+i*100}")
    return "\n".join(lines)

def _create_dummy_zip_file(zip_path: str, csv_filename_in_zip: str, csv_content: str, encoding: str = "utf-8"):
    """
    建立一個包含單一 CSV 檔案的模擬 ZIP 檔案。

    Args:
        zip_path (str): 要建立的 ZIP 檔案的完整路徑。
        csv_filename_in_zip (str): ZIP 檔案中 CSV 的名稱。
        csv_content (str): 要寫入 CSV 檔案的內容。
        encoding (str): CSV 檔案內容的編碼。
    """
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_filename_in_zip, csv_content.encode(encoding))

# 之後會在此處加入測試函式

# --- 測試主函式 ---
def test_pipeline_full_run(tmp_path):
    """
    端到端整合測試，模擬 PipelineOrchestrator 的完整執行流程。
    """
    project_root_name = f"test_project_pipeline_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    base_path = tmp_path
    project_path = base_path / project_root_name

    # 3.a. 建立一個臨時的專案根目錄
    project_path.mkdir()

    # 3.b. 在其中建立 Input, processed, quarantine, logs, archive, db 等子目錄
    # (PipelineOrchestrator 的 _setup_directories 會處理 logs, archive, db，但 Input, processed, quarantine 需要預先準備好，
    # 或者確保 Orchestrator 會建立它們。根據 Orchestrator 的 _setup_directories，它會建立所有這些。
    # 為了確保，我們在此手動建立 Input，其他的讓 Orchestrator 處理或驗證其建立。)

    input_dir = project_path / pipeline_constants.INPUT_DIR
    processed_dir = project_path / pipeline_constants.PROCESSED_DIR
    quarantine_dir = project_path / pipeline_constants.QUARANTINE_DIR
    # archive_dir 和 db_dir 會由 orchestrator 內部路徑設定自動指向 project_path 下的相應子目錄
    # log_dir 也一樣

    input_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True) # 通常開始時是空的
    quarantine_dir.mkdir(parents=True, exist_ok=True) # 通常開始時是空的

    # 讓 Orchestrator 建立 archive, db, logs 目錄
    # archive_dir_path = project_path / pipeline_constants.ARCHIVE_DIR
    # db_dir_path = project_path / pipeline_constants.DB_DIR
    # log_dir_path = project_path / pipeline_constants.LOG_DIR
    # archive_dir_path.mkdir(parents=True, exist_ok=True)
    # db_dir_path.mkdir(parents=True, exist_ok=True)
    # log_dir_path.mkdir(parents=True, exist_ok=True)


    # --- 4. 準備模擬輸入檔案 ---

    # 模擬檔案的檔名
    normal_zip_filename = "normal_data_daily_20230101.zip"
    corrupted_zip_filename = "corrupted_data_daily_20230102.zip"
    processed_zip_filename = "already_processed_daily_20230103.zip"

    csv_in_normal_zip = "normal_data_daily_20230101.csv"
    csv_in_corrupted_zip = "corrupted_data_daily_20230102.csv"
    csv_in_processed_zip = "already_processed_daily_20230103.csv"

    # 4.a. 正常檔案
    normal_csv_content = _create_dummy_csv_content(file_type="normal")
    _create_dummy_zip_file(input_dir / normal_zip_filename, csv_in_normal_zip, normal_csv_content, encoding="utf-8")

    # 4.b. 格式錯誤檔案 (使用 UTF-8 無法正確解碼的 Big5 字元序列)
    # 注意：這裡的 "corrupted_format" 內容需要確保 FileParser 會判定為錯誤
    # 一個簡單的方式是使用 FileParser 無法辨識的編碼，或者混合編碼導致解碼失敗
    # _create_dummy_csv_content 中的 "corrupted_format" 旨在產生這種情況
    corrupted_csv_content = _create_dummy_csv_content(file_type="corrupted_format")
    _create_dummy_zip_file(input_dir / corrupted_zip_filename, csv_in_corrupted_zip, corrupted_csv_content, encoding="big5") # 假設用 big5 存，但內容可能讓解析器混淆

    # 4.c. 已處理檔案
    processed_csv_content = _create_dummy_csv_content(file_type="normal", rows=2) # 內容本身是正常的
    _create_dummy_zip_file(input_dir / processed_zip_filename, csv_in_processed_zip, processed_csv_content, encoding="utf-8")

    # 為 "已處理檔案" 建立模擬的 manifest.json
    # PipelineOrchestrator 會在 project_path / ARCHIVE_DIR / manifest.json 尋找或建立
    # 所以我們需要預先建立這個檔案
    archive_dir_path = project_path / pipeline_constants.ARCHIVE_DIR
    archive_dir_path.mkdir(parents=True, exist_ok=True) # 確保 archive 目錄存在

    manifest_content = {
        "files": {
            processed_zip_filename: {
                "status": "success", # 或者 file_parser 回傳的成功狀態
                "message": "Simulated as already processed",
                "timestamp": datetime.now().isoformat(),
                # 根據 ManifestManager，可能還需要 hash 或其他欄位，
                # 但 PipelineOrchestrator 的 is_file_processed(filename) 似乎只看檔名是否存在於 manifest
                # 為了安全起見，我們添加一個模擬的 hash
                # 注意：這個 hash 應該是 ZIP 檔案本身的 hash，而不是 CSV 內容的 hash
                # 然而，PipelineOrchestrator.ManifestManager 的 is_file_processed 是基於檔名
                # 所以此處的 hash 僅為模擬，實際的 Orchestrator 可能不會用到它來判斷 "是否已處理"
                "hash": hashlib.sha256(processed_zip_filename.encode('utf-8')).hexdigest()
            }
        }
    }
    manifest_file_path = archive_dir_path / "manifest.json" # 假設 manifest 檔名固定為 manifest.json
    with open(manifest_file_path, "w", encoding="utf-8") as mf:
        json.dump(manifest_content, mf, indent=4)

    # 驗證模擬檔案已建立
    assert (input_dir / normal_zip_filename).exists()
    assert (input_dir / corrupted_zip_filename).exists()
    assert (input_dir / processed_zip_filename).exists()
    assert manifest_file_path.exists()

    content_check = json.loads(manifest_file_path.read_text(encoding="utf-8"))
    assert processed_zip_filename in content_check.get("files", {})

    # --- 5. 實例化並執行 PipelineOrchestrator ---

    # 設定 Orchestrator 參數
    db_name = "test_output_database.duckdb"
    log_name = "test_pipeline_run.log"

    orchestrator = PipelineOrchestrator(
        base_path=str(base_path), # tmp_path object needs to be string
        project_folder_name=project_root_name,
        database_name=db_name,
        log_name=log_name,
        target_zip_files="", # 處理 Input 目錄中的所有檔案
        debug_mode=True # 啟用除錯模式以便獲取更詳細日誌，有助於測試
    )

    # 執行管線
    orchestrator.run()

    # --- 6. 驗證結果 (Assertions) ---

    # 6.a. 驗證檔案移動
    assert not (input_dir / normal_zip_filename).exists(), "正常檔案應已移出 Input"
    assert (processed_dir / normal_zip_filename).exists(), "正常檔案應已移至 processed"

    assert not (input_dir / corrupted_zip_filename).exists(), "錯誤檔案應已移出 Input"
    assert (quarantine_dir / corrupted_zip_filename).exists(), "錯誤檔案應已移至 quarantine"

    # 已處理的檔案應該被跳過，並保留在 Input 目錄中
    # 修正：根據 PipelineOrchestrator 的邏輯，如果 is_file_processed() 為 true，它會 log 並 continue，所以檔案會留在 Input
    assert (input_dir / processed_zip_filename).exists(), "已處理檔案應保留在 Input 目錄"
    assert not (processed_dir / processed_zip_filename).exists(), "已處理檔案不應進入 processed"
    assert not (quarantine_dir / processed_zip_filename).exists(), "已處理檔案不應進入 quarantine"

    # 6.b. 驗證 Manifest 更新
    # archive_dir_path 已經在準備模擬檔案時定義過
    # manifest_file_path 也已經定義過
    assert manifest_file_path.exists(), "Manifest 檔案應存在"

    updated_manifest_data = json.loads(manifest_file_path.read_text(encoding="utf-8"))
    manifest_files_info = updated_manifest_data.get("files", {})

    # 驗證正常檔案的紀錄
    assert normal_zip_filename in manifest_files_info, "Manifest 中應包含正常檔案的紀錄"
    # 假設成功狀態是 'success'，這需要與 FileParser 的回傳一致
    # 也需要與 PipelineOrchestrator 中 update_manifest 傳入的 status 一致
    # 根據 PipelineOrchestrator.py 程式碼，status 是從 file_parser.parse_file 回傳的
    # file_parser.py 中的 _parse_single_csv 成功時回傳 constants.STATUS_SUCCESS
    # 我們需要確保 pipeline_constants.STATUS_SUCCESS 的值 (例如 "success")
    # 為了方便，我們直接使用 FileParser 成功時回傳的 status 字串 "success"
    assert manifest_files_info[normal_zip_filename].get("status") == "success", "正常檔案在 Manifest 中的狀態應為成功"

    # 驗證錯誤檔案的紀錄
    assert corrupted_zip_filename in manifest_files_info, "Manifest 中應包含錯誤檔案的紀錄"
    # 類似地，錯誤狀態可能是 "error" 或 "failed"
    # file_parser.py 中的 _parse_single_csv 失敗時回傳 constants.STATUS_ERROR
    # 我們使用字串 "error"
    assert manifest_files_info[corrupted_zip_filename].get("status") == "error", "錯誤檔案在 Manifest 中的狀態應為錯誤"
    assert "無法使用支援的編碼解碼" in manifest_files_info[corrupted_zip_filename].get("message", "") or \
           "ZIP 檔中未找到任何 CSV 檔案" in manifest_files_info[corrupted_zip_filename].get("message", "") or \
           "讀取 ZIP 中 CSV 時發生錯誤" in manifest_files_info[corrupted_zip_filename].get("message", "") or \
           "損壞的 ZIP 檔案" in manifest_files_info[corrupted_zip_filename].get("message", "") or \
           "處理 ZIP 時發生未知錯誤" in manifest_files_info[corrupted_zip_filename].get("message", "") or \
           "解析 CSV 時發生未知錯誤" in manifest_files_info[corrupted_zip_filename].get("message", ""), \
           "錯誤檔案的訊息應包含預期的錯誤原因"


    # 驗證已處理檔案的紀錄 (其原始訊息不應被覆蓋，或至少狀態仍表示之前的成功)
    assert processed_zip_filename in manifest_files_info, "Manifest 中應包含已處理檔案的紀錄"
    assert manifest_files_info[processed_zip_filename].get("message") == "Simulated as already processed", "已處理檔案的訊息不應改變"
    # 狀態也應該是原始的 "success"
    assert manifest_files_info[processed_zip_filename].get("status") == "success", "已處理檔案的狀態不應改變"


    # 6.c. 驗證 DuckDB 資料庫
    db_file_path = project_path / pipeline_constants.DB_DIR / db_name
    assert db_file_path.exists(), "DuckDB 資料庫檔案應已建立"

    try:
        con = duckdb.connect(database=str(db_file_path), read_only=True)

        # 預期正常檔案的數據會進入 'default_daily' 表 (基於檔名和 schema.json 的關鍵字)
        # 輔助函式 _create_dummy_csv_content 產生的欄位是 '交易日期,契約,開盤價,最高價,最低價,收盤價,成交量'
        # 這些欄位會對應到 'default_daily' schema
        # FileParser 會將 schema 名稱 (例如 'default_daily') 作為 table name 傳回
        # DatabaseLoader 會用這個 table name (schema_type) 來決定資料表名稱

        # 首先檢查資料表是否存在
        tables_query = con.execute("SHOW TABLES;").fetchall()
        db_tables = [table[0] for table in tables_query]
        expected_table_name = "default_daily" # 根據 normal_zip_filename 和 schemas.json

        assert expected_table_name in db_tables, f"資料表 '{expected_table_name}' 應已在資料庫中建立"

        # 查詢 'default_daily' 表的內容
        # _create_dummy_csv_content(file_type="normal", rows=3) 產生3筆資料
        result_df = con.execute(f"SELECT COUNT(*) FROM {expected_table_name}").fetchone()
        assert result_df is not None and result_df[0] == 3, f"資料表 '{expected_table_name}' 中應包含來自正常檔案的 3 筆數據"

        # 可以進一步驗證數據內容，例如查詢特定欄位
        sample_data_query = con.execute(f"SELECT 契約, 收盤價 FROM {expected_table_name} WHERE 交易日期 = '2023-01-01'").fetchone()
        assert sample_data_query is not None, "應能查詢到特定日期數據"
        assert sample_data_query[0] == "TXF", "契約應為 TXF"
        assert sample_data_query[1] == 15000.0, "收盤價應為 15000.0 (假設 FileParser/DBLoader 處理為 float)"
        # 注意：CSV 中的 15000，根據 schema "default_daily" -> "close" -> "db_type": "DOUBLE"，所以會是 float

    except Exception as e:
        pytest.fail(f"操作 DuckDB 時發生錯誤: {e}")
    finally:
        if 'con' in locals() and con:
            con.close()

    # 清理步驟由 pytest 的 tmp_path fixture 自動處理，無需手動 shutil.rmtree()

    # 簡單驗證日誌和資料庫目錄是否已由 Orchestrator 建立 (這些已在執行後立即檢查，此處可作為最終確認)
    log_dir_path = project_path / pipeline_constants.LOG_DIR
    db_dir_path = project_path / pipeline_constants.DB_DIR
    assert log_dir_path.exists()
    assert (log_dir_path / log_name).exists()
    assert db_dir_path.exists()

    print(f"臨時專案目錄已建立於: {project_path}") # 除錯用

    # 暫時在此處結束，以便逐步檢查
    assert project_path.exists()
    assert input_dir.exists() # 'already_processed' 檔案應仍在此
    assert processed_dir.exists()
    assert quarantine_dir.exists()
