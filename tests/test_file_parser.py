# -*- coding: utf-8 -*-
# tests/test_file_parser.py

"""
針對 src.data_pipeline_v15.file_parser 模組的單元測試.
"""

import pytest
import pandas as pd
import numpy as np # 匯入 numpy 以便於比較 NaN 值
import os
from io import BytesIO
import zipfile # 雖然 conftest 有 zip 相關 fixture，這裡可能也需要直接用
from unittest.mock import patch, MagicMock
import copy # 用於深拷貝 schema 內容，以防萬一 fixture 的 scope 不是 function

# 從專案原始碼中導入待測函式和常數
from src.data_pipeline_v15.file_parser import worker_process_file
from src.data_pipeline_v15.core import constants

# 測試函式將會定義於此

def test_parse_zip_with_big5_csv_success(create_zip_in_memory, schemas_json_content, tmp_path):
    """
    測試 worker_process_file 函式能否成功處理一個包含 Big5 編碼 CSV 檔案的 ZIP 檔案。
    預期：
    - ZIP 檔案被正確讀取。
    - CSV 檔案使用 Big5 編碼解析。
    - 檔案內容根據 schema (假設為 weekly_report) 轉換並正規化。
    - 結果以 Parquet 格式儲存。
    - 回傳的狀態和計數正確。
    - Parquet 檔案的內容符合預期。
    """
    # 1. 準備 CSV 內容 (Big5 編碼)
    #    注意：schema 中的 "weekly_report" 的關鍵字是 "weekly_report"
    #    檔頭和資料需符合 schemas.json 中 "weekly_report" schema 的別名和目標欄位
    #    例如，"日期", "商品名稱", "身份別", "多方交易口數", "多方交易金額", "空方交易口數", "空方交易金額"
    #    對應到 "trade_date", "product_name", "trader_type", "long_contracts", "long_amount", "short_contracts", "short_amount"

    csv_header = "日期,商品名稱,身份別,多方交易口數,多方交易金額,空方交易口數,空方交易金額"
    csv_row1 = "2023/10/26,臺股期貨,自營商,100,1000000,50,500000"
    csv_row2 = "2023/10/27,電子期貨,外資,200,2000000,150,1500000"
    csv_content_str = f"{csv_header}\n{csv_row1}\n{csv_row2}"
    csv_data_big5 = csv_content_str.encode('big5')

    # 2. 建立記憶體中的 ZIP 檔案
    #    檔名包含 'weekly_report' 以匹配 schema
    internal_csv_filename = 'weekly_report_data_big5_inside.csv'
    zip_files_map = {internal_csv_filename: csv_data_big5}
    zip_bytes_io = create_zip_in_memory(zip_files_map)

    # 3. 模擬檔案路徑並修補 zipfile.ZipFile
    #    worker_process_file 預期一個 .zip 檔案的路徑字串
    mock_zip_filename = "mocked_weekly_report_archive.zip" # 傳給 worker_process_file 的假檔名

    #    使用 patch 來攔截 zipfile.ZipFile 的實例化
    #    確保修補的路徑 'src.data_pipeline_v15.file_parser.zipfile.ZipFile' 正確
    with patch('src.data_pipeline_v15.file_parser.zipfile.ZipFile') as mock_zipfile_constructor:
        # 設定當 ZipFile 被呼叫時，回傳一個使用我們 BytesIO 物件初始化的 ZipFile 實例
        # 我們需要確保回傳的 mock 物件的行為像一個真正的 ZipFile 物件，特別是作為一個上下文管理器
        mock_zip_instance = zipfile.ZipFile(zip_bytes_io, 'r')
        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance

        # 4. 執行 worker_process_file
        #    tmp_path 是 pytest 提供的 fixture，型別為 pathlib.Path
        result = worker_process_file(mock_zip_filename, str(tmp_path), schemas_json_content)

    # 5. 驗證回傳結果字典
    assert result[constants.KEY_STATUS] == constants.STATUS_GROUP_RESULT
    assert result[constants.KEY_FILE] == mock_zip_filename

    # 檢查 results 列表
    assert len(result[constants.KEY_RESULTS]) == 1
    single_file_result = result[constants.KEY_RESULTS][0]

    assert single_file_result[constants.KEY_STATUS] == constants.STATUS_SUCCESS
    assert single_file_result[constants.KEY_TABLE] == "weekly_report" # 根據 schema 關鍵字匹配
    assert single_file_result[constants.KEY_COUNT] == 2 # CSV 中的資料行數 (不含檔頭)

    # 檢查 Parquet 檔案路徑是否存在
    # 輸出路徑格式: {staging_dir}/{schema_name}/{hash}.parquet
    expected_parquet_dir = tmp_path / "weekly_report"
    assert expected_parquet_dir.exists()
    assert expected_parquet_dir.is_dir()

    # 由於檔名是 hash 過的，我們檢查目錄下是否有 .parquet 檔案
    parquet_files = list(expected_parquet_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    generated_parquet_path = parquet_files[0]
    assert generated_parquet_path.name == single_file_result[constants.KEY_PATH].split(os.sep)[-1] # 比較檔名部分
    assert os.path.exists(single_file_result[constants.KEY_PATH]) # 完整路徑也應存在

    # 6. (建議) 驗證 Parquet 檔案內容
    df_parquet = pd.read_parquet(generated_parquet_path)

    # 驗證欄位名稱 (根據 weekly_report schema 的目標欄位)
    # 這些是 schemas.json 中 "weekly_report" 的 "columns_map" の鍵
    expected_columns = [
        "trade_date", "product_name", "trader_type",
        "long_contracts", "long_amount", "short_contracts", "short_amount"
    ]
    assert list(df_parquet.columns) == expected_columns

    # 驗證資料內容 (注意型別轉換和正規化)
    # worker_process_file 內部會做正規化，例如日期格式、數值型別等
    # 原始 CSV: "2023/10/26,臺股期貨,自營商,100,1000000,50,500000"
    # 預期 Parquet (假設日期被轉為 datetime, 數值被轉為 int/float):
    #   trade_date: pd.Timestamp (或字串，取決於 schema 如何定義轉換)
    #   product_name: "臺股期貨"
    #   trader_type: "自營商"
    #   long_contracts: 100
    #   long_amount: 1000000
    #   short_contracts: 50
    #   short_amount: 500000

    # 假設 schema 中沒有明確的日期轉換，它可能仍是字串
    # 假設 schema 中沒有明確的數值轉換，pd.read_csv 可能已推斷
    # file_parser.py 對欄位名稱做了 .strip().lower()，並用 column_map 重新命名
    # 接著 reindex(columns=target_columns) 會確保順序並補齊缺失欄位 (NaN)

    assert df_parquet.shape[0] == 2 # 資料行數

    # 驗證第一行資料
    # 注意：Parquet 讀取後，數值型別可能是 numpy.int64 或類似
    assert str(df_parquet.loc[0, "trade_date"]) == "2023/10/26" # 假設日期保持字串
    assert df_parquet.loc[0, "product_name"] == "臺股期貨"
    assert df_parquet.loc[0, "trader_type"] == "自營商"
    assert int(df_parquet.loc[0, "long_contracts"]) == 100
    assert int(df_parquet.loc[0, "long_amount"]) == 1000000
    assert int(df_parquet.loc[0, "short_contracts"]) == 50
    assert int(df_parquet.loc[0, "short_amount"]) == 500000

    # 驗證第二行資料
    assert str(df_parquet.loc[1, "trade_date"]) == "2023/10/27"
    assert df_parquet.loc[1, "product_name"] == "電子期貨"
    assert df_parquet.loc[1, "trader_type"] == "外資" # Big5 "外資"
    assert int(df_parquet.loc[1, "long_contracts"]) == 200
    assert int(df_parquet.loc[1, "long_amount"]) == 2000000
    assert int(df_parquet.loc[1, "short_contracts"]) == 150
    assert int(df_parquet.loc[1, "short_amount"]) == 1500000

    # 清理：pytest 的 tmp_path fixture 會自動處理暫存目錄的清理
    # zip_bytes_io 也會在其生命週期結束時被垃圾回收

def test_parse_zip_with_utf8_csv_success(create_zip_in_memory, schemas_json_content, tmp_path):
    """
    測試 worker_process_file 函式能否成功處理一個包含 UTF-8 編碼 CSV 檔案的 ZIP 檔案，
    並使用 default_daily schema 進行處理。
    預期：
    - ZIP 檔案被正確讀取。
    - CSV 檔案使用 UTF-8 編碼解析。
    - 檔案內容根據 schema (default_daily) 轉換並正規化。
    - 即使 CSV 中缺少 schema 中的某些欄位，reindex 仍會補齊這些欄位 (值為 NaN)。
    - 結果以 Parquet 格式儲存。
    - 回傳的狀態和計數正確。
    - Parquet 檔案的內容符合預期。
    """
    # 1. 準備 CSV 內容 (UTF-8 編碼)
    #    檔頭符合 default_daily schema 的部分別名。
    #    'default_daily' schema 的關鍵字是 'MarketInformation', 'DailyQuotes', 'TradingData' 或檔名本身不包含特殊關鍵字時的後備
    #    我們將使用檔名不包含特殊關鍵字來觸發 default_daily。
    #    `default_daily` schema 的欄位 (取自 schemas.json):
    #    "trade_date", "product_code", "expiry_month_week", "open_price", "high_price", "low_price",
    #    "close_price", "volume", "settlement_price", "open_interest", "change_in_oi",
    #    "trading_session"
    #    CSV 檔頭可以只包含其中一部分，例如:
    csv_header = "交易日期,契約,到期月份(週別),開盤價,最高價,最低價,收盤價,成交量" # 對應到 schema 的別名
    csv_row1 = "2023/10/27,TX,202311,16000,16100,15900,16050,1000"
    csv_row2 = "2023/10/28,MTX,,1623,1625,1620,1622,532" # 到期月份(週別) 為空
    csv_content_str = f"{csv_header}\n{csv_row1}\n{csv_row2}"
    csv_data_utf8 = csv_content_str.encode('utf-8')

    # 2. 建立記憶體中的 ZIP 檔案
    #    檔名設計成不包含 'weekly_report' 等特殊關鍵字，以便觸發 'default_daily' schema
    internal_csv_filename = 'market_daily_data_utf8.csv'
    zip_files_map = {internal_csv_filename: csv_data_utf8}
    zip_bytes_io = create_zip_in_memory(zip_files_map)

    # 3. 模擬檔案路徑並修補 zipfile.ZipFile
    mock_zip_filename = "mocked_daily_market_archive.zip"

    with patch('src.data_pipeline_v15.file_parser.zipfile.ZipFile') as mock_zipfile_constructor:
        mock_zip_instance = zipfile.ZipFile(zip_bytes_io, 'r')
        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance

        # 4. 執行 worker_process_file
        result = worker_process_file(mock_zip_filename, str(tmp_path), schemas_json_content)

    # 5. 驗證回傳結果字典
    assert result[constants.KEY_STATUS] == constants.STATUS_GROUP_RESULT
    assert result[constants.KEY_FILE] == mock_zip_filename

    assert len(result[constants.KEY_RESULTS]) == 1
    single_file_result = result[constants.KEY_RESULTS][0]

    assert single_file_result[constants.KEY_STATUS] == constants.STATUS_SUCCESS
    assert single_file_result[constants.KEY_TABLE] == "default_daily" # 預期匹配 default_daily
    assert single_file_result[constants.KEY_COUNT] == 2 # 資料行數

    # 檢查 Parquet 檔案路徑
    expected_parquet_dir = tmp_path / "default_daily"
    assert expected_parquet_dir.exists()
    assert expected_parquet_dir.is_dir()

    parquet_files = list(expected_parquet_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    generated_parquet_path = parquet_files[0]
    assert os.path.exists(single_file_result[constants.KEY_PATH])

    # 6. 驗證 Parquet 檔案內容
    df_parquet = pd.read_parquet(generated_parquet_path)

    # 驗證欄位名稱 (根據 default_daily schema 的所有目標欄位)
    expected_columns = list(schemas_json_content["default_daily"]["columns_map"].keys())
    assert list(df_parquet.columns) == expected_columns
    assert df_parquet.shape[0] == 2 # 資料行數

    # 驗證第一行資料
    # CSV: "2023/10/27,TX,202311,16000,16100,15900,16050,1000"
    # Parquet 欄位: trade_date, product_code, expiry_month_week, open_price, high_price, low_price, close_price, volume
    # 其他 default_daily 欄位 (settlement_price, open_interest, change_in_oi, trading_session) 應為 NaN
    assert str(df_parquet.loc[0, "trade_date"]) == "2023/10/27"
    assert df_parquet.loc[0, "product_code"] == "TX"
    assert str(df_parquet.loc[0, "expiry_month_week"]) == "202311" # 假設字串處理
    assert float(df_parquet.loc[0, "open_price"]) == 16000.0
    assert float(df_parquet.loc[0, "high_price"]) == 16100.0
    assert float(df_parquet.loc[0, "low_price"]) == 15900.0
    assert float(df_parquet.loc[0, "close_price"]) == 16050.0
    assert float(df_parquet.loc[0, "volume"]) == 1000.0

    # 驗證 reindex 補齊的欄位為 NaN
    assert pd.isna(df_parquet.loc[0, "settlement_price"])
    assert pd.isna(df_parquet.loc[0, "open_interest"])
    # "change_in_oi" 在 schema 中別名是 "漲跌%"，若CSV無此欄，則補 NaN
    assert pd.isna(df_parquet.loc[0, "change_in_oi"])
    # "trading_session" 在 schema 中別名是 "盤別"，若CSV無此欄，則補 NaN
    assert pd.isna(df_parquet.loc[0, "trading_session"])


    # 驗證第二行資料
    # CSV: "2023/10/28,MTX,,1623,1625,1620,1622,532" (到期月份為空)
    assert str(df_parquet.loc[1, "trade_date"]) == "2023/10/28"
    assert df_parquet.loc[1, "product_code"] == "MTX"
    # 空字串讀入 pandas 後可能變成 NaN (取決於 read_csv 參數)，或者保持空字串
    # 如果 CSV 中的空值被 pandas.read_csv 解讀為 NaN，或者後續處理轉為 NaN
    # 則 Parquet 中對應的欄位也會是 NaN。
    # 假設 pd.read_csv 將連續分隔符間的空值視為空字串，若該欄位為數值型別，則可能轉為 NaN。
    # 若為 object/string 型別，則可能保持空字串 '' 或轉為 np.nan。
    # 測試中 expiry_month_week 應為字串。
    # 在 file_parser.py 中，欄位是先讀取，然後 reindex。
    # 如果 schema 要求 expiry_month_week 是字串，空字串 '' 會保留。
    # 如果 schema 要求它是數值，'' 會變成 NaN。
    # default_daily schema 未指定型別，pandas 會推斷。'' 通常是 object。
    # 此處 expiry_month_week 在 schema 中沒有指定型別，pandas read_csv 會將 '' 視為字串。
    # 如果 schema.json 中有定義該欄位的 dtype (例如 "string" or "object")，則會是 ''
    # 如果是 "float" or "Int64" (nullable int), 則會是 pd.NA 或 np.nan
    # 假設它被當作 object/string 類型欄位處理
    assert df_parquet.loc[1, "expiry_month_week"] == '' or pd.isna(df_parquet.loc[1, "expiry_month_week"])


    assert float(df_parquet.loc[1, "open_price"]) == 1623.0
    assert float(df_parquet.loc[1, "high_price"]) == 1625.0
    assert float(df_parquet.loc[1, "low_price"]) == 1620.0
    assert float(df_parquet.loc[1, "close_price"]) == 1622.0
    assert float(df_parquet.loc[1, "volume"]) == 532.0

    assert pd.isna(df_parquet.loc[1, "settlement_price"])
    assert pd.isna(df_parquet.loc[1, "open_interest"])
    assert pd.isna(df_parquet.loc[1, "change_in_oi"])
    assert pd.isna(df_parquet.loc[1, "trading_session"])

    # 清理：pytest 的 tmp_path fixture 會自動處理暫存目錄的清理
    # zip_bytes_io 也會在其生命週期結束時被垃圾回收

def test_parse_empty_zip_file(create_zip_in_memory, schemas_json_content, tmp_path):
    """
    測試當提供一個空的 ZIP 檔案（不包含任何檔案）時，worker_process_file 的行為。
    預期：
    - 函式應回傳一個錯誤狀態。
    - 錯誤原因應指明 ZIP 檔案中未找到 CSV 檔案。
    - 不應在暫存目錄中產生任何 Parquet 檔案或 schema 子目錄。
    """
    # 1. 建立一個空的記憶體 ZIP 檔案
    #    傳入空字典給 create_zip_in_memory fixture
    empty_zip_bytes_io = create_zip_in_memory({})

    # 2. 模擬檔案路徑並修補 zipfile.ZipFile
    mock_zip_filename = "empty_archive.zip"

    with patch('src.data_pipeline_v15.file_parser.zipfile.ZipFile') as mock_zipfile_constructor:
        # 設定 mock 使其回傳一個用 BytesIO 初始化的 (空的) ZipFile 實例
        mock_zip_instance = zipfile.ZipFile(empty_zip_bytes_io, 'r')
        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance

        # 3. 執行 worker_process_file
        result = worker_process_file(mock_zip_filename, str(tmp_path), schemas_json_content)

    # 4. 驗證結果
    assert result[constants.KEY_STATUS] == constants.STATUS_ERROR
    assert result[constants.KEY_FILE] == mock_zip_filename
    assert "ZIP 檔中未找到任何 CSV 檔案" in result[constants.KEY_REASON]

    # 檢查 KEY_RESULTS 是否不存在或為空 (根據 file_parser.py 的實作)
    # 當 ZIP 檔案中未找到 CSV 檔案時，KEY_RESULTS 可能不存在於回傳的字典中
    assert constants.KEY_RESULTS not in result or not result[constants.KEY_RESULTS]


    # 5. 確認沒有在 tmp_path 下產生任何 Parquet 檔案或對應的 schema 子目錄
    #    tmp_path 本身會存在，但其內部應該是空的，或者不包含預期的 schema 子目錄
    #    我們可以檢查 tmp_path 下是否沒有任何檔案或目錄被建立
    #    或者更具體地，檢查是否沒有 'default_daily', 'weekly_report' 等目錄被建立

    # 列出 tmp_path 下的內容
    items_in_tmp_path = list(tmp_path.iterdir())
    assert not items_in_tmp_path, f"暫存目錄 {tmp_path} 應為空，但找到了: {items_in_tmp_path}"

    # 或者，如果 tmp_path 可能包含其他非預期檔案（例如 .DS_Store），
    # 則更精確的檢查是確認沒有 schema 子目錄被建立：
    # for schema_name in schemas_json_content.keys():
    #     assert not (tmp_path / schema_name).exists(), \
    #         f"不應為空的 ZIP 檔案建立 schema 目錄 '{tmp_path / schema_name}'"

def test_parse_zip_csv_no_schema_match_no_default(create_zip_in_memory, schemas_json_content, tmp_path):
    """
    測試當 ZIP 中的 CSV 檔案名稱不匹配任何 schema 關鍵字，且不存在 default_daily schema 時的行為。
    預期：
    - 對於該 CSV 檔案的處理結果應為錯誤。
    - 錯誤原因應指明找不到匹配的 schema。
    - 不應產生 Parquet 檔案。
    """
    # 1. 修改 Schema (移除 default_daily)
    #    由於 pytest fixture 預設 scope 為 function，每次測試 schemas_json_content 都會是新的副本。
    #    若要更保險，或 fixture scope 不同，可以使用 copy.deepcopy()。
    modified_schemas = copy.deepcopy(schemas_json_content)
    if "default_daily" in modified_schemas:
        del modified_schemas["default_daily"]

    # 2. 準備 CSV 內容 (UTF-8) - 內容本身不重要，檔名才重要
    csv_header = "任意欄位A,任意欄位B"
    csv_row1 = "值1,值2"
    csv_content_str = f"{csv_header}\n{csv_row1}"
    csv_data_utf8 = csv_content_str.encode('utf-8')

    # 3. 建立記憶體中的 ZIP 檔案
    #    CSV 檔名設計為不匹配任何剩餘 schema 的關鍵字 (例如 'weekly_report')
    internal_csv_filename = 'completely_unknown_data_type.csv'
    zip_files_map = {internal_csv_filename: csv_data_utf8}
    zip_bytes_io = create_zip_in_memory(zip_files_map)

    # 4. 模擬檔案路徑與修補 zipfile.ZipFile
    mock_zip_filename = "no_schema_match_archive.zip"

    with patch('src.data_pipeline_v15.file_parser.zipfile.ZipFile') as mock_zipfile_constructor:
        mock_zip_instance = zipfile.ZipFile(zip_bytes_io, 'r')
        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance

        # 5. 執行 worker_process_file，傳入修改後的 schema
        result = worker_process_file(mock_zip_filename, str(tmp_path), modified_schemas)

    # 6. 驗證結果
    #    整體狀態是 STATUS_GROUP_RESULT 因為 ZIP 本身是有效的，但內部的檔案處理失敗
    assert result[constants.KEY_STATUS] == constants.STATUS_GROUP_RESULT
    assert result[constants.KEY_FILE] == mock_zip_filename

    # 應有一個結果，表示對內部 CSV 檔案的處理嘗試
    assert len(result[constants.KEY_RESULTS]) == 1
    single_file_result = result[constants.KEY_RESULTS][0]

    assert single_file_result[constants.KEY_STATUS] == constants.STATUS_ERROR
    # KEY_FILE 應為 ZIP 內的 CSV 檔名，格式為 "zip_filename/internal_csv_filename"
    expected_internal_file_display_name = f"{mock_zip_filename}/{internal_csv_filename}"
    assert single_file_result[constants.KEY_FILE] == expected_internal_file_display_name

    # 驗證錯誤原因
    # 根據 file_parser.py 的邏輯: "找不到任何匹配的 schema (無 default_daily 後備)"
    assert "找不到任何匹配的 schema" in single_file_result[constants.KEY_REASON]
    assert "(無 default_daily 後備)" in single_file_result[constants.KEY_REASON]

    # 7. 確認沒有在 tmp_path 下產生任何 Parquet 檔案或對應的 schema 子目錄
    items_in_tmp_path = list(tmp_path.iterdir())
    assert not items_in_tmp_path, f"暫存目錄 {tmp_path} 應為空，但找到了: {items_in_tmp_path}"

def test_parse_zip_csv_no_schema_match_with_default(create_zip_in_memory, schemas_json_content, tmp_path):
    """
    測試當 ZIP 中的 CSV 檔案名稱不匹配任何特定 schema 關鍵字，但存在 default_daily schema 時的行為。
    預期：
    - CSV 檔案應成功使用 default_daily schema 進行處理。
    - 結果以 Parquet 格式儲存到 default_daily 子目錄。
    - Parquet 檔案內容符合預期。
    """
    # 1. 準備 CSV 內容 (UTF-8)，內容應能被 default_daily schema 處理
    #    使用 default_daily schema 中的一些別名作為檔頭
    csv_header = "交易日期,契約,開盤價,成交量" # 例如："trade_date", "product_code", "open_price", "volume"
    csv_row1 = "2023/11/01,TXF,16200,1500"
    csv_row2 = "2023/11/01,TE,780,800"
    csv_content_str = f"{csv_header}\n{csv_row1}\n{csv_row2}"
    csv_data_utf8 = csv_content_str.encode('utf-8')

    # 2. 建立記憶體中的 ZIP 檔案
    #    CSV 檔名設計為不匹配 weekly_report 等特定 schema 的關鍵字
    internal_csv_filename = 'other_generic_data.csv'
    zip_files_map = {internal_csv_filename: csv_data_utf8}
    zip_bytes_io = create_zip_in_memory(zip_files_map)

    # 3. 模擬檔案路徑與修補 zipfile.ZipFile
    mock_zip_filename = "fallback_to_default_archive.zip"

    # 使用未經修改的 schemas_json_content (其中應包含 default_daily)
    with patch('src.data_pipeline_v15.file_parser.zipfile.ZipFile') as mock_zipfile_constructor:
        mock_zip_instance = zipfile.ZipFile(zip_bytes_io, 'r')
        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance

        # 4. 執行 worker_process_file
        result = worker_process_file(mock_zip_filename, str(tmp_path), schemas_json_content)

    # 5. 驗證結果 (成功，使用 default_daily)
    assert result[constants.KEY_STATUS] == constants.STATUS_GROUP_RESULT
    assert result[constants.KEY_FILE] == mock_zip_filename

    assert len(result[constants.KEY_RESULTS]) == 1
    single_file_result = result[constants.KEY_RESULTS][0]

    assert single_file_result[constants.KEY_STATUS] == constants.STATUS_SUCCESS
    assert single_file_result[constants.KEY_TABLE] == "default_daily" # 應成功使用後備 schema
    assert single_file_result[constants.KEY_COUNT] == 2 # 資料行數

    # 檢查 Parquet 檔案路徑是否存在於 default_daily 子目錄中
    expected_parquet_dir = tmp_path / "default_daily"
    assert expected_parquet_dir.exists()
    assert expected_parquet_dir.is_dir()

    parquet_files = list(expected_parquet_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    generated_parquet_path = parquet_files[0]
    assert os.path.exists(single_file_result[constants.KEY_PATH])

    # 6. 驗證 Parquet 內容
    df_parquet = pd.read_parquet(generated_parquet_path)

    # 驗證欄位名稱是否符合 default_daily schema 中定義的所有目標欄位
    expected_columns = list(schemas_json_content["default_daily"]["columns_map"].keys())
    assert list(df_parquet.columns) == expected_columns
    assert df_parquet.shape[0] == 2 # 資料行數

    # 驗證第一行資料
    # CSV: "2023/11/01,TXF,16200,1500"
    # 對應 default_daily: trade_date, product_code, open_price, volume
    # 其他 default_daily 欄位應為 NaN
    assert str(df_parquet.loc[0, "trade_date"]) == "2023/11/01"
    assert df_parquet.loc[0, "product_code"] == "TXF"
    assert float(df_parquet.loc[0, "open_price"]) == 16200.0
    assert float(df_parquet.loc[0, "volume"]) == 1500.0

    # 驗證 default_daily schema 中其他未在 CSV 提供的欄位是否為 NaN
    assert pd.isna(df_parquet.loc[0, "expiry_month_week"])
    assert pd.isna(df_parquet.loc[0, "high_price"])
    assert pd.isna(df_parquet.loc[0, "low_price"])
    assert pd.isna(df_parquet.loc[0, "close_price"])
    assert pd.isna(df_parquet.loc[0, "settlement_price"])
    assert pd.isna(df_parquet.loc[0, "open_interest"])
    assert pd.isna(df_parquet.loc[0, "change_in_oi"])
    assert pd.isna(df_parquet.loc[0, "trading_session"])

    # 驗證第二行資料
    # CSV: "2023/11/01,TE,780,800"
    assert str(df_parquet.loc[1, "trade_date"]) == "2023/11/01"
    assert df_parquet.loc[1, "product_code"] == "TE"
    assert float(df_parquet.loc[1, "open_price"]) == 780.0
    assert float(df_parquet.loc[1, "volume"]) == 800.0

    assert pd.isna(df_parquet.loc[1, "expiry_month_week"])
    assert pd.isna(df_parquet.loc[1, "high_price"])
    # ... 其他 NaN 欄位可以類似地斷言 ...

    # 清理：pytest 的 tmp_path fixture 會自動處理暫存目錄的清理
    # zip_bytes_io 也會在其生命週期結束時被垃圾回收

def test_parse_zip_csv_incomplete_fields(create_zip_in_memory, schemas_json_content, tmp_path):
    """
    測試當 ZIP 中的 CSV 檔案欄位不完整時，是否能根據 schema (例如 default_daily) 成功補齊欄位。
    預期：
    - CSV 檔案成功使用 default_daily schema 處理。
    - 輸出的 Parquet 檔案包含 schema 中定義的所有欄位，原始 CSV 中缺失的欄位值為 NaN。
    """
    # 1. 準備 CSV 內容 (UTF-8, 欄位不齊全)
    #    使用 default_daily schema，但只提供部分欄位的別名
    csv_header = "交易日期,契約,收盤價" # 對應 "trade_date", "product_code", "close_price"
    csv_row1 = "2023/11/02,TXO,16300"
    csv_row2 = "2023/11/02,TEO,785"
    csv_content_str = f"{csv_header}\n{csv_row1}\n{csv_row2}"
    csv_data_utf8 = csv_content_str.encode('utf-8')

    # 2. 建立記憶體中的 ZIP 檔案
    #    檔名可以設為匹配 default_daily (或讓其回退)
    internal_csv_filename = 'daily_incomplete_data.csv' # 檔名包含 "daily" 可能有助於匹配，或依賴後備
    zip_files_map = {internal_csv_filename: csv_data_utf8}
    zip_bytes_io = create_zip_in_memory(zip_files_map)

    # 3. 模擬檔案路徑與修補 zipfile.ZipFile
    mock_zip_filename = "incomplete_fields_archive.zip"

    with patch('src.data_pipeline_v15.file_parser.zipfile.ZipFile') as mock_zipfile_constructor:
        mock_zip_instance = zipfile.ZipFile(zip_bytes_io, 'r')
        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance

        # 4. 執行 worker_process_file
        result = worker_process_file(mock_zip_filename, str(tmp_path), schemas_json_content)

    # 5. 驗證結果 (成功，欄位被補齊)
    assert result[constants.KEY_STATUS] == constants.STATUS_GROUP_RESULT
    assert result[constants.KEY_FILE] == mock_zip_filename

    assert len(result[constants.KEY_RESULTS]) == 1
    single_file_result = result[constants.KEY_RESULTS][0]

    assert single_file_result[constants.KEY_STATUS] == constants.STATUS_SUCCESS
    assert single_file_result[constants.KEY_TABLE] == "default_daily" # 假設匹配 default_daily
    assert single_file_result[constants.KEY_COUNT] == 2 # 資料行數

    # 檢查 Parquet 檔案路徑
    expected_parquet_dir = tmp_path / "default_daily"
    assert expected_parquet_dir.exists()

    parquet_files = list(expected_parquet_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    generated_parquet_path = parquet_files[0]
    assert os.path.exists(single_file_result[constants.KEY_PATH])

    # 6. 驗證 Parquet 內容 (欄位補齊)
    df_parquet = pd.read_parquet(generated_parquet_path)

    # 關鍵驗證：DataFrame 的欄位名稱完全符合 default_daily schema 中定義的所有目標欄位
    expected_columns = list(schemas_json_content["default_daily"]["columns_map"].keys())
    assert list(df_parquet.columns) == expected_columns
    assert df_parquet.shape[0] == 2

    # 驗證第一行資料
    # CSV: "2023/11/02,TXO,16300" (trade_date, product_code, close_price)
    assert str(df_parquet.loc[0, "trade_date"]) == "2023/11/02"
    assert df_parquet.loc[0, "product_code"] == "TXO"
    assert float(df_parquet.loc[0, "close_price"]) == 16300.0

    # 驗證 default_daily schema 中其他未在 CSV 提供的欄位是否為 NaN
    assert pd.isna(df_parquet.loc[0, "expiry_month_week"])
    assert pd.isna(df_parquet.loc[0, "open_price"])
    assert pd.isna(df_parquet.loc[0, "high_price"])
    assert pd.isna(df_parquet.loc[0, "low_price"])
    assert pd.isna(df_parquet.loc[0, "volume"])
    assert pd.isna(df_parquet.loc[0, "settlement_price"])
    assert pd.isna(df_parquet.loc[0, "open_interest"])
    assert pd.isna(df_parquet.loc[0, "change_in_oi"])
    assert pd.isna(df_parquet.loc[0, "trading_session"])

    # 驗證第二行資料
    # CSV: "2023/11/02,TEO,785"
    assert str(df_parquet.loc[1, "trade_date"]) == "2023/11/02"
    assert df_parquet.loc[1, "product_code"] == "TEO"
    assert float(df_parquet.loc[1, "close_price"]) == 785.0

    # 再次驗證補齊的欄位
    assert pd.isna(df_parquet.loc[1, "open_price"])
    assert pd.isna(df_parquet.loc[1, "volume"])
    # ... 其他 NaN 欄位可以類似地斷言 ...

    # 清理：pytest 的 tmp_path fixture 會自動處理暫存目錄的清理
    # zip_bytes_io 也會在其生命週期結束時被垃圾回收

def test_parse_zip_csv_undecodable_content(create_zip_in_memory, schemas_json_content, tmp_path):
    """
    測試當 ZIP 中的 CSV 檔案包含無法被任何支援編碼（UTF-8, Big5 等）解碼的內容時的行為。
    預期：
    - 對於該 CSV 檔案的處理結果應為錯誤。
    - 錯誤原因應指明無法解碼。
    - 不應產生 Parquet 檔案。
    """
    # 1. 準備無法解碼的 CSV 內容
    #    使用一個不太可能在 UTF-8 或 Big5 中有效的位元組序列。
    #    例如，一個 Latin-1 編碼的字串，其中包含 UTF-8 和 Big5 都難以正確處理的字元。
    #    或者一個隨機的位元組序列。
    #    Python 的 `\xff` (255) 在 UTF-8 中是非法的起始位元組。
    #    在 Big5 中，許多高位元組是合法的，但與特定低位元組組合。
    #    一個簡單的方法是混合一些 Latin-1 特有的高 ASCII 字元。
    #    `file_parser.py` 會嘗試 'utf-8', 'big5', 'ms950', 'cp950'

    # 包含 Latin-1 特有的字元 'é' (0xE9) 和 'ü' (0xFC) 以及一個無效的 UTF-8 序列 \xff
    raw_bytes = b"header1,header2\nval1,val2\ninvalid_char,\xe9\xfc\xff"
    # 如果需要更強的不可解碼性，可以考慮使用更複雜的序列，
    # 但上述序列應該足以讓 utf-8, big5, ms950, cp950 都解碼失敗或產生不可預期結果。

    # 2. 建立記憶體中的 ZIP 檔案
    internal_csv_filename = 'undecodable_data.csv'
    zip_files_map = {internal_csv_filename: raw_bytes}
    zip_bytes_io = create_zip_in_memory(zip_files_map)

    # 3. 模擬檔案路徑與修補 zipfile.ZipFile
    mock_zip_filename = "undecodable_archive.zip"

    with patch('src.data_pipeline_v15.file_parser.zipfile.ZipFile') as mock_zipfile_constructor:
        mock_zip_instance = zipfile.ZipFile(zip_bytes_io, 'r')
        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance

        # 4. 執行 worker_process_file
        result = worker_process_file(mock_zip_filename, str(tmp_path), schemas_json_content)

    # 5. 驗證結果 (錯誤，無法解碼)
    assert result[constants.KEY_STATUS] == constants.STATUS_GROUP_RESULT
    assert result[constants.KEY_FILE] == mock_zip_filename

    assert len(result[constants.KEY_RESULTS]) == 1
    single_file_result = result[constants.KEY_RESULTS][0]

    assert single_file_result[constants.KEY_STATUS] == constants.STATUS_ERROR

    expected_internal_file_display_name = f"{mock_zip_filename}/{internal_csv_filename}"
    assert single_file_result[constants.KEY_FILE] == expected_internal_file_display_name

    # 關鍵驗證：錯誤原因應指明無法使用支援的編碼解碼
    # 根據 file_parser.py 的邏輯，訊息類似 "無法使用支援的編碼解碼，或檔案為空. Errors: ..."
    assert "無法使用支援的編碼解碼" in single_file_result[constants.KEY_REASON]
    # 也可以檢查是否提及了嘗試過的編碼，例如 'utf-8' 和 'big5'
    assert "utf-8 編碼解碼失敗" in single_file_result[constants.KEY_REASON]
    assert "big5 編碼解碼失敗" in single_file_result[constants.KEY_REASON]
    assert "ms950 編碼解碼失敗" in single_file_result[constants.KEY_REASON]
    assert "cp950 編碼解碼失敗" in single_file_result[constants.KEY_REASON]


    # 6. 確認沒有在 tmp_path 下產生任何 Parquet 檔案或對應的 schema 子目錄
    items_in_tmp_path = list(tmp_path.iterdir())
    assert not items_in_tmp_path, f"暫存目錄 {tmp_path} 應為空，但找到了: {items_in_tmp_path}"

def test_parse_zip_with_multiple_csv_files_partial_success(create_zip_in_memory, schemas_json_content, tmp_path):
    """
    測試處理包含多個 CSV 檔案的 ZIP，其中部分成功，部分失敗。
    預期：
    - 整體狀態為 STATUS_GROUP_RESULT。
    - 結果列表包含每個 CSV 的獨立處理結果。
    - 成功的 CSV 應產生 Parquet 檔案並回報 SUCCESS。
    - 失敗的 CSV 應回報 ERROR 及相應原因，且不產生 Parquet。
    """
    mock_zip_filename = "multi_csv_archive.zip"

    # 1. 準備 CSV 內容
    csv_ok_weekly_name = 'weekly_data_ok.csv'
    csv_ok_weekly_content = ("日期,商品名稱,身份別,多方交易口數\n"
                             "2023/11/01,臺股期貨,投信,200").encode('big5')

    csv_bad_cols_weekly_name = 'weekly_data_bad_cols.csv' # 包含 'weekly' 關鍵字
    csv_bad_cols_weekly_content = ("亂給的欄位1,亂給的欄位2\n"
                                   "aaa,bbb").encode('utf-8')

    csv_ok_daily_name = 'daily_data_ok.csv'
    csv_ok_daily_content = ("交易日期,契約,成交量\n"
                            "2023/11/02,TXO,5000").encode('utf-8')

    csv_undecodable_name = 'undecodable_internal.csv'
    csv_undecodable_content = b"header1\ninvalid,\xe9\xfc\xff" # Latin-1 with invalid UTF-8/Big5 bytes

    zip_files_map = {
        csv_ok_weekly_name: csv_ok_weekly_content,
        csv_bad_cols_weekly_name: csv_bad_cols_weekly_content,
        csv_ok_daily_name: csv_ok_daily_content,
        csv_undecodable_name: csv_undecodable_content,
    }
    zip_bytes_io = create_zip_in_memory(zip_files_map)

    # 2. 修補並執行
    with patch('src.data_pipeline_v15.file_parser.zipfile.ZipFile') as mock_zipfile_constructor:
        mock_zip_instance = zipfile.ZipFile(zip_bytes_io, 'r')
        mock_zipfile_constructor.return_value.__enter__.return_value = mock_zip_instance
        result = worker_process_file(mock_zip_filename, str(tmp_path), schemas_json_content)

    # 3. 驗證總體結果
    assert result[constants.KEY_STATUS] == constants.STATUS_GROUP_RESULT
    assert result[constants.KEY_FILE] == mock_zip_filename
    assert len(result[constants.KEY_RESULTS]) == 4

    # 輔助函式，用於從結果列表中按檔名查找特定結果
    def find_result_by_name(results_list, internal_filename):
        full_search_name = f"{mock_zip_filename}/{internal_filename}"
        for r in results_list:
            if r[constants.KEY_FILE] == full_search_name:
                return r
        return None

    # 4. 逐一驗證每個 CSV 的處理結果
    # CSV 1 (成功, Big5, weekly_report)
    res_ok_weekly = find_result_by_name(result[constants.KEY_RESULTS], csv_ok_weekly_name)
    assert res_ok_weekly is not None
    assert res_ok_weekly[constants.KEY_STATUS] == constants.STATUS_SUCCESS
    assert res_ok_weekly[constants.KEY_TABLE] == "weekly_report"
    assert res_ok_weekly[constants.KEY_COUNT] == 1
    assert os.path.exists(res_ok_weekly[constants.KEY_PATH])
    assert (tmp_path / "weekly_report").exists()

    # CSV 2 (失敗, 欄位不匹配 schema, weekly_report 關鍵字但欄位亂給)
    res_bad_cols_weekly = find_result_by_name(result[constants.KEY_RESULTS], csv_bad_cols_weekly_name)
    assert res_bad_cols_weekly is not None
    assert res_bad_cols_weekly[constants.KEY_STATUS] == constants.STATUS_ERROR
    assert "欄位重命名後，檔案內容與所有已知綱要的目標欄位完全不符" in res_bad_cols_weekly[constants.KEY_REASON]
    assert constants.KEY_PATH not in res_bad_cols_weekly # 不應有 Parquet 路徑

    # CSV 3 (成功, UTF-8, default_daily)
    res_ok_daily = find_result_by_name(result[constants.KEY_RESULTS], csv_ok_daily_name)
    assert res_ok_daily is not None
    assert res_ok_daily[constants.KEY_STATUS] == constants.STATUS_SUCCESS
    assert res_ok_daily[constants.KEY_TABLE] == "default_daily"
    assert res_ok_daily[constants.KEY_COUNT] == 1
    assert os.path.exists(res_ok_daily[constants.KEY_PATH])
    assert (tmp_path / "default_daily").exists()

    # CSV 4 (失敗, 無法解碼)
    res_undecodable = find_result_by_name(result[constants.KEY_RESULTS], csv_undecodable_name)
    assert res_undecodable is not None
    assert res_undecodable[constants.KEY_STATUS] == constants.STATUS_ERROR
    assert "無法使用支援的編碼解碼" in res_undecodable[constants.KEY_REASON]
    assert constants.KEY_PATH not in res_undecodable # 不應有 Parquet 路徑

    # 驗證只有成功的檔案產生了 Parquet 檔案
    # 總共應有2個 Parquet 檔案 (一個在 weekly_report, 一個在 default_daily)
    total_parquet_files = []
    if (tmp_path / "weekly_report").exists():
        total_parquet_files.extend(list((tmp_path / "weekly_report").glob("*.parquet")))
    if (tmp_path / "default_daily").exists():
        total_parquet_files.extend(list((tmp_path / "default_daily").glob("*.parquet")))
    assert len(total_parquet_files) == 2
