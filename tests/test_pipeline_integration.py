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

import pathlib # Added for path manipulation

# --- 測試主函式 ---
import yaml

# Removing old _create_dummy_csv_content and _create_dummy_zip_file as they are replaced by fixtures.
# If some specific dummy generation is needed for other tests, they can be kept or refactored.
def test_pipeline_full_run(tmp_path):
    """
    端到端整合測試，模擬 PipelineOrchestrator 的完整執行流程（本地優先工作流程）。
    """
    # --- 1. 準備臨時的 config.yaml ---
    test_project_folder_name = f"test_project_pipeline_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    local_workspace_root_path = tmp_path / "local_workspace"
    remote_drive_base_path = tmp_path / "remote_drive" # This will be the 'base_path' for the orchestrator

    # Define directory names (consistent with what PipelineOrchestrator expects from config)
    dir_config = {
        "input": "00_input_test",
        "processed": "01_processed_test",
        "archive": "02_archive_test",
        "quarantine": "03_quarantine_test",
        "db": "98_database_test",
        "log": "99_logs_test"
    }
    test_db_name = "test_integration_db.duckdb"
    test_log_name = "test_integration_pipeline.log"

    config_data = {
        "project_folder": test_project_folder_name,
        "database_name": test_db_name,
        "log_name": test_log_name,
        "local_workspace": str(local_workspace_root_path),
        "remote_base_path": str(remote_drive_base_path),
        "max_workers": 4,
        "directories": dir_config,
        "validation_rules": { # Added validation rules
            "default_daily": { # Assuming 'default_daily' is the matched_schema_name for these CSVs
                "trading_date": {"non_null": True},
                "volume": {"min_value": 0},
                "close": {"non_null": True}
            }
        }
    }
    temp_config_file = tmp_path / "test_config.yaml"
    with open(temp_config_file, 'w', encoding='utf-8') as f:
        yaml.dump(config_data, f)

    # --- 2. 模擬遠端目錄結構和準備輸入檔案 ---
    # This is the path where the orchestrator will create the project structure on the "remote"
    # It's remote_drive_base_path / test_project_folder_name
    simulated_remote_project_path = remote_drive_base_path / test_project_folder_name

    simulated_remote_input_dir = simulated_remote_project_path / dir_config["input"]
    simulated_remote_processed_dir = simulated_remote_project_path / dir_config["processed"]
    simulated_remote_quarantine_dir = simulated_remote_project_path / dir_config["quarantine"]
    simulated_remote_archive_dir = simulated_remote_project_path / dir_config["archive"]
    simulated_remote_db_dir = simulated_remote_project_path / dir_config["db"]
    simulated_remote_log_dir = simulated_remote_project_path / dir_config["log"]

    # Create these simulated remote dirs (orchestrator's _create_remote_directories_if_not_exist will also do this)
    simulated_remote_input_dir.mkdir(parents=True, exist_ok=True)
    simulated_remote_processed_dir.mkdir(parents=True, exist_ok=True)
    simulated_remote_quarantine_dir.mkdir(parents=True, exist_ok=True)
    simulated_remote_archive_dir.mkdir(parents=True, exist_ok=True)
    simulated_remote_db_dir.mkdir(parents=True, exist_ok=True)
    simulated_remote_log_dir.mkdir(parents=True, exist_ok=True)

    fixture_expectations = {
        "normal_daily_direct": {
            "source_fixture": "csvs/normal_utf8.csv", "input_filename": "normal_daily_direct.csv",
            "outcome": pipeline_constants.STATUS_SUCCESS, "table": "fact_daily_ohlc", "rows": 2, "in_processed": True
        },
        "daily_no_keywords_fail_required": {
            "source_fixture": "csvs/no_matching_schema_keywords.csv", "input_filename": "daily_no_keywords.csv",
            "outcome": pipeline_constants.STATUS_ERROR,
            "reason_contains": "欄位重命名後，檔案 'daily_no_keywords.csv' 內容與 schema 'default_daily' 的目標欄位不符", "in_quarantine": True
        },
         "zip_with_normal_daily_content_fails": { # Renamed for clarity
            "source_fixture": "zips/zip_normal_single_utf8.zip", "input_filename": "zip_with_normal_daily_content_fails.zip",
            "outcome": pipeline_constants.STATUS_ERROR,
            "in_quarantine": True,
            "reason_contains": "所有子項目處理失敗或無資料可載入",
            "subfile_results": [{ # This structure implies we might want to check manifest for sub-file details if available
                    "subfile_name_contains": "normal_utf8.csv", # The name of the file *inside* the zip
                    "status": pipeline_constants.STATUS_ERROR,
                    "reason_contains": "必要欄位缺失或完全為空: volume"
            }]
        },
        "unidentifiable_csv": {
            "source_fixture": "csvs/completely_unidentifiable.csv", "input_filename": "completely_unidentifiable.csv",
            "outcome": pipeline_constants.STATUS_ERROR,
            "reason_contains": "欄位重命名後，檔案 'completely_unidentifiable.csv' 內容與 schema 'default_daily' 的目標欄位不符",
            "in_quarantine": True
        },
        "invalid_data_test_daily": { # New entry for the file with invalid data
            "source_fixture": "csvs/normal_utf8_with_invalid_data.csv",
            "input_filename": "normal_utf8_with_invalid_data.csv",
            # Expect overall success if at least one row is valid and loaded.
            # The orchestrator's final status for a file depends on if *any* part of it was successfully processed
            # and loaded, even if other parts were quarantined.
            "outcome": pipeline_constants.STATUS_SUCCESS,
            "table": "fact_daily_ohlc", # Main table for valid data
            "rows": 1, # Only 1 out of 3 rows is valid in the new fixture
            "in_processed": True, # The original file should be moved to processed
            "quarantine_table_name": "quarantine_data", # Table for invalid data
            "quarantined_rows": 2 # 2 out of 3 rows are invalid
        }
    }

    current_file_path = pathlib.Path(__file__)
    fixtures_base_dir = current_file_path.parent / "fixtures"
    input_filename_to_expectation_key = {}

    for key, expectation in fixture_expectations.items():
        source_fixture_path = fixtures_base_dir / expectation["source_fixture"]
        assert source_fixture_path.exists(), f"Fixture file {source_fixture_path} does not exist for key '{key}'."
        # Copy to simulated remote input directory
        destination_in_simulated_remote_input = simulated_remote_input_dir / expectation["input_filename"]
        shutil.copy(source_fixture_path, destination_in_simulated_remote_input)
        assert destination_in_simulated_remote_input.exists()
        input_filename_to_expectation_key[expectation["input_filename"]] = key

    # --- Copy schemas.json to a location accessible by the test ---
    original_schemas_path = pathlib.Path("config/schemas.json") # Assuming this is at repo root/config
    temp_test_schemas_dir = tmp_path / "test_schemas_dir" # A general place in tmp_path for test schemas
    temp_test_schemas_dir.mkdir(exist_ok=True)
    temp_schemas_file_for_test = temp_test_schemas_dir / "schemas.json"
    if original_schemas_path.exists():
        shutil.copy(original_schemas_path, temp_schemas_file_for_test)
        assert temp_schemas_file_for_test.exists(), "Failed to copy schemas.json for test."
    else:
        pytest.fail(f"Original schemas.json not found at {original_schemas_path.resolve()}")


    # --- 3. 實例化 PipelineOrchestrator ---
    orchestrator = PipelineOrchestrator(
        config_file_path=str(temp_config_file),
        base_path=str(remote_drive_base_path), # This is the root for the "remote" project path
        # project_folder_name_override, database_name_override, log_name_override are NOT passed
        # to test that they are correctly read from the temp_config_file.
        target_zip_files="", # Process all files from simulated remote input
        debug_mode=True,
        schemas_file_path=str(temp_schemas_file_for_test) # Explicit path to test schemas
    )
    orchestrator.run()

    # --- 4. 驗證檔案同步和操作 ---

    # Verify local workspace cleanup
    expected_local_project_path_in_workspace = local_workspace_root_path / test_project_folder_name
    assert not expected_local_project_path_in_workspace.exists(), \
        f"本地工作區專案資料夾 {expected_local_project_path_in_workspace} 應已被清理"

    # Verify manifest on simulated remote
    simulated_remote_manifest_file = simulated_remote_archive_dir / pipeline_constants.MANIFEST_FILE
    assert simulated_remote_manifest_file.exists(), "Manifest 檔案應已同步回模擬遠端"
    manifest_data = json.loads(simulated_remote_manifest_file.read_text(encoding="utf-8"))
    manifest_files_info = manifest_data.get("files", {})
    # print("DEBUG: Remote Manifest content:", json.dumps(manifest_files_info, indent=2))


    # Helper to find manifest entry (updated for flexibility)
    def find_manifest_entry(filename, manifest_entries_dict):
        # Attempt 1: Direct match by filename (if filename was used as key, e.g. for pre-hash errors)
        if filename in manifest_entries_dict:
            return manifest_entries_dict[filename]
        # Attempt 2: Check 'original_filename' field within entries (if keys are hashes)
        for entry_data in manifest_entries_dict.values():
            if isinstance(entry_data, dict) and entry_data.get("original_filename") == filename:
                return entry_data
        return None

    # Verify database on simulated remote
    simulated_remote_db_file = simulated_remote_db_dir / test_db_name
    assert simulated_remote_db_file.exists(), "DuckDB 資料庫檔案應已同步回模擬遠端"
    con = None
    try:
        con = duckdb.connect(database=str(simulated_remote_db_file), read_only=True)

        for input_filename, expectation_key in input_filename_to_expectation_key.items():
            expectation = fixture_expectations[expectation_key]

            # Verify file movement to simulated remote processed/quarantine
            if expectation.get("in_processed"):
                assert not (simulated_remote_input_dir / input_filename).exists(), \
                    f"檔案 {input_filename} 應已從模擬遠端 Input 移出 (或在本地處理後未同步回Input)"
                assert (simulated_remote_processed_dir / input_filename).exists(), \
                    f"檔案 {input_filename} 應已同步至模擬遠端 processed"
            elif expectation.get("in_quarantine"):
                assert not (simulated_remote_input_dir / input_filename).exists(), \
                    f"檔案 {input_filename} 應已從模擬遠端 Input 移出 (或在本地處理後未同步回Input)"
                assert (simulated_remote_quarantine_dir / input_filename).exists(), \
                    f"檔案 {input_filename} 應已同步至模擬遠端 quarantine"

            # Verify Manifest content for this file on simulated remote
            manifest_entry = find_manifest_entry(input_filename, manifest_files_info)
            assert manifest_entry is not None, f"Manifest entry for {input_filename} ({expectation_key}) not found in remote manifest."
            assert manifest_entry.get("status") == expectation["outcome"], \
                f"Manifest status for {input_filename} mismatch. Expected {expectation['outcome']}, got {manifest_entry.get('status')}"
            if "reason_contains" in expectation:
                assert expectation["reason_contains"] in manifest_entry.get("message", ""), \
                    f"Manifest message for {input_filename} did not contain '{expectation['reason_contains']}'. Got: '{manifest_entry.get('message', '')}'"

            # If there are subfile_results to check (e.g. for ZIPs)
            if "subfile_results" in expectation and manifest_entry.get("status") == pipeline_constants.STATUS_ERROR: # Only check if parent is error
                 # The overall message for the ZIP file should be checked against reason_contains
                 # To check sub-file details, the manifest format for ZIPs would need to be more specific.
                 # For now, the main reason_contains on the ZIP's manifest entry is the primary check.
                 # If manifest stores detailed sub-file errors, that could be parsed here.
                 pass


        # Verify Database Content (Aggregated) from simulated remote DB
        expected_rows_main_tables = {}
        expected_rows_quarantine_table = 0

        for key, expec_details in fixture_expectations.items():
            if expec_details.get("outcome") == pipeline_constants.STATUS_SUCCESS: # Or other status indicating some processing occurred
                if "table" in expec_details and "rows" in expec_details:
                    table_name = expec_details["table"]
                    rows = expec_details["rows"]
                    if rows > 0 : # Only add if expecting rows in main table
                         expected_rows_main_tables[table_name] = expected_rows_main_tables.get(table_name, 0) + rows

                # Accumulate expected quarantined rows from this file
                if "quarantined_rows" in expec_details and expec_details["quarantined_rows"] > 0:
                    expected_rows_quarantine_table += expec_details["quarantined_rows"]

            # For sub-files in ZIPs that might contribute to main/quarantine tables
            # This part needs careful thought if a ZIP can have partially valid/invalid content
            # and how that's aggregated or reported.
            # For now, the `zip_with_normal_daily_content_fails` fixture expects the whole ZIP to be quarantined
            # and its sub-file failure is part of the reason.
            # If a ZIP could have some valid data loaded and some quarantined, `fixture_expectations` would need
            # to be more granular for ZIPs. The current `invalid_data_test_daily` is a direct CSV.

        tables_in_db_query_res = con.execute("SHOW TABLES;").fetchall()
        db_tables_present = [tbl[0] for tbl in tables_in_db_query_res]

        # Verify main tables
        if not expected_rows_main_tables:
            # If no files were expected to produce valid data for main tables
            # We might still have the tables created (e.g. fact_daily_ohlc) but they'd be empty.
            # Or, if a schema was never matched, the table might not be created.
            # For this test, 'fact_daily_ohlc' should be created by 'normal_daily_direct' or 'invalid_data_test_daily'.
            pass
        else:
            for table_name, total_expected_rows in expected_rows_main_tables.items():
                assert table_name in db_tables_present, f"主資料表 '{table_name}' 應已在模擬遠端資料庫中建立"
                result_row_count_query = con.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()
                assert result_row_count_query is not None, f"無法從主資料表 '{table_name}' 取得筆數"
                actual_rows = result_row_count_query[0]
                assert actual_rows == total_expected_rows, \
                    f"主資料表 '{table_name}' 中應包含 {total_expected_rows} 筆數據, 實際為 {actual_rows} (在模擬遠端)"

        # Verify quarantine_data table
        quarantine_table_name_const = "quarantine_data" # As defined in schemas.json
        if expected_rows_quarantine_table > 0:
            assert quarantine_table_name_const in db_tables_present, \
                f"隔離資料表 '{quarantine_table_name_const}' 應已建立，因為預期有 {expected_rows_quarantine_table} 行隔離數據。"
            q_count_query = con.execute(f"SELECT COUNT(*) FROM \"{quarantine_table_name_const}\"").fetchone()
            assert q_count_query is not None, f"無法從隔離資料表 '{quarantine_table_name_const}' 取得筆數"
            actual_quarantined_rows = q_count_query[0]
            assert actual_quarantined_rows == expected_rows_quarantine_table, \
                f"隔離資料表 '{quarantine_table_name_const}' 中應包含 {expected_rows_quarantine_table} 筆數據, 實際為 {actual_quarantined_rows}"

            # Optional: Verify content of a quarantined row
            if "invalid_data_test_daily" in fixture_expectations: # Check one specific file's quarantined data
                q_rows_df = con.execute(f"SELECT source_file, quarantine_reason FROM \"{quarantine_table_name_const}\" WHERE source_file = 'normal_utf8_with_invalid_data.csv'").df()
                assert len(q_rows_df) == fixture_expectations["invalid_data_test_daily"]["quarantined_rows"]
                # Check specific reasons (example for the first quarantined row from that file)
                first_q_reason = q_rows_df['quarantine_reason'].iloc[0]
                assert ("Column 'volume': is less than 0" in first_q_reason or \
                        "Column 'trading_date': is null" in first_q_reason)

        elif quarantine_table_name_const in db_tables_present : # Table exists but should be empty
             q_count_query = con.execute(f"SELECT COUNT(*) FROM \"{quarantine_table_name_const}\"").fetchone()
             if q_count_query is not None and q_count_query[0] > 0:
                  pytest.fail(f"隔離資料表 '{quarantine_table_name_const}' 應為空，但找到 {q_count_query[0]} 行。")

    except Exception as e:
        # print full manifest content for debugging if an assertion fails
        if simulated_remote_manifest_file.exists():
            print("DEBUG (on failure): Remote Manifest content:\n", json.dumps(json.loads(simulated_remote_manifest_file.read_text(encoding="utf-8")), indent=2))
        pytest.fail(f"測試執行或驗證過程中發生錯誤: {e}")
    finally:
        if con:
            con.close()

    # Verify logs on simulated remote
    simulated_remote_log_file = simulated_remote_log_dir / test_log_name
    assert simulated_remote_log_dir.exists(), "模擬遠端日誌資料夾應存在"
    assert simulated_remote_log_file.exists(), "日誌檔案應已同步回模擬遠端"
    # Check if log file has content (basic check)
    assert simulated_remote_log_file.stat().st_size > 0, "日誌檔案不應為空"

    # Final check that essential remote directories exist (input might be empty if all processed/quarantined)
    assert simulated_remote_project_path.exists()
    assert simulated_remote_input_dir.exists() # Input dir itself should exist
    assert simulated_remote_processed_dir.exists()
    assert simulated_remote_quarantine_dir.exists()
    assert simulated_remote_archive_dir.exists()
    assert simulated_remote_db_dir.exists()
