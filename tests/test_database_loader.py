# -*- coding: utf-8 -*-
import pytest
import pandas as pd
import json
import re # Added import
from unittest.mock import patch, MagicMock, mock_open

# Adjust the import path based on your project structure.
# Assuming 'src' is a top-level directory and your tests run from the project root.
from src.data_pipeline_v15.database_loader import DatabaseLoader

VALID_SCHEMAS_CONTENT = {
    "test_schema_A": {"db_table_name": "table_A"},
    "test_schema_B": {"db_table_name": "table_B"},
    "schema_missing_name": {} # Schema without db_table_name
}

MALFORMED_SCHEMAS_CONTENT = "this is not valid json"

@pytest.fixture
def mock_logger():
    """Provides a MagicMock instance for logger."""
    return MagicMock()

@patch('src.data_pipeline_v15.database_loader.duckdb.connect') # Prevent actual DB connection
def test_initialization_loads_allowed_tables(mock_duckdb_connect, mock_logger):
    """
    Tests that DatabaseLoader correctly loads allowed table names from a valid schemas.json.
    """
    m_open = mock_open(read_data=json.dumps(VALID_SCHEMAS_CONTENT))
    with patch('builtins.open', m_open):
        with patch('json.load', return_value=VALID_SCHEMAS_CONTENT) as mock_json_load:
            loader = DatabaseLoader(database_file="dummy.db", logger=mock_logger)

            expected_tables = {"table_A", "table_B"}
            assert loader.allowed_table_names == expected_tables

            found_log_correct_content = False
            for call_args in mock_logger.info.call_args_list:
                log_message = call_args[0][0]
                if log_message.startswith(f"Successfully loaded {len(expected_tables)} allowed table names:"):
                    match = re.search(r"(\{.*\})", log_message)
                    if match:
                        set_str = match.group(1)
                        try:
                            # Replace single quotes with double quotes for valid JSON parsing if eval is problematic
                            # Or, more safely, parse with ast.literal_eval after ensuring it's a valid literal
                            import ast
                            logged_set = ast.literal_eval(set_str)
                            if isinstance(logged_set, set) and logged_set == expected_tables:
                                found_log_correct_content = True
                                break
                        except (ValueError, SyntaxError):
                            # Fallback to direct string comparison for the known variations if ast.literal_eval fails
                            # This part is less ideal but can catch the common string representations of a 2-element set
                            # For more complex sets, ast.literal_eval is preferred.
                            if set_str == "{'table_A', 'table_B'}" or set_str == "{'table_B', 'table_A'}":
                                found_log_correct_content = True
                                break
            assert found_log_correct_content, f"Log message with correct table set not found. Expected: {expected_tables}. Calls: {mock_logger.info.call_args_list}"

            # Check that a warning is logged for the schema missing 'db_table_name'
            mock_logger.warning.assert_any_call(
                "Schema 'schema_missing_name' in 'config/schemas.json' is missing 'db_table_name'."
            )


@patch('src.data_pipeline_v15.database_loader.duckdb.connect')
def test_initialization_file_not_found(mock_duckdb_connect, mock_logger):
    """
    Tests behavior when schemas.json is not found.
    allowed_table_names should be empty and an error logged.
    """
    m_open = mock_open()
    m_open.side_effect = FileNotFoundError("File not found")
    with patch('builtins.open', m_open):
        loader = DatabaseLoader(database_file="dummy.db", logger=mock_logger)
        assert loader.allowed_table_names == set()
        mock_logger.error.assert_called_with(
            "Schemas file 'config/schemas.json' not found. No table names will be allowed."
        )

@patch('src.data_pipeline_v15.database_loader.duckdb.connect')
def test_initialization_json_decode_error(mock_duckdb_connect, mock_logger):
    """
    Tests behavior when schemas.json is malformed.
    allowed_table_names should be empty and an error logged.
    """
    m_open = mock_open(read_data=MALFORMED_SCHEMAS_CONTENT)
    with patch('builtins.open', m_open):
        # Mock json.load directly to control the error precisely
        with patch('json.load', side_effect=json.JSONDecodeError("Error", "doc", 0)) as mock_json_load:
            loader = DatabaseLoader(database_file="dummy.db", logger=mock_logger)
            assert loader.allowed_table_names == set()
            mock_logger.error.assert_called_with(
                "Error decoding JSON from 'config/schemas.json': Error: line 1 column 1 (char 0). No table names will be allowed."
            )

@patch('src.data_pipeline_v15.database_loader.duckdb.connect')
def test_initialization_generic_error_loading_schemas(mock_duckdb_connect, mock_logger):
    """
    Tests behavior with a generic error during schema loading.
    """
    m_open = mock_open()
    m_open.side_effect = Exception("Some generic error")
    with patch('builtins.open', m_open):
        loader = DatabaseLoader(database_file="dummy.db", logger=mock_logger)
        assert loader.allowed_table_names == set()
        mock_logger.error.assert_called_with(
            "An unexpected error occurred while loading allowed table names from 'config/schemas.json': Some generic error",
            exc_info=True
        )


# Patch _connect for all load_data tests to avoid actual DB operations
# @patch.object(DatabaseLoader, '_connect', MagicMock())
# class TestLoadData:
#
#     @pytest.fixture
#     def loader_with_tables(self, mock_logger):
#         """
#         Provides a DatabaseLoader instance with pre-set allowed_table_names
#         and a mocked connection.
#         """
#         # Mock the schema loading part for these specific tests
#         with patch.object(DatabaseLoader, '_load_allowed_table_names', MagicMock()) as mock_load_names:
#             loader = DatabaseLoader(database_file="dummy.db", logger=mock_logger)
#             loader.allowed_table_names = {"table_A", "table_B"}
#             loader.connection = MagicMock() # Mock the connection object itself
#             mock_load_names.assert_called_once() # Ensure it was called during init
#             return loader
#
#     def test_load_data_allowed_table(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"schema_type": ["table_A"], "data": [1]})
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_not_called()
#         loader_with_tables.connection.register.assert_called_once_with("temp_df_view", df)
#         assert loader_with_tables.connection.execute.call_count == 2
#         loader_with_tables.connection.execute.assert_any_call(
#             '\n                CREATE TABLE IF NOT EXISTS "table_A" AS SELECT * FROM temp_df_view LIMIT 0;\n            '
#         )
#         loader_with_tables.connection.execute.assert_any_call(
#             '\n                INSERT INTO "table_A" SELECT * FROM temp_df_view;\n            '
#         )
#
#     def test_load_data_table_not_in_whitelist(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"schema_type": ["table_C"], "data": [1]}) # table_C is not allowed
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_called_once()
#         assert "Table name 'table_C' (derived from schema_type) is not in the allowed list" in mock_logger.error.call_args[0][0]
#         loader_with_tables.connection.execute.assert_not_called()
#         loader_with_tables.connection.register.assert_not_called()
#
#
#     def test_load_data_missing_schema_type_column(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"data": [1]}) # Missing 'schema_type'
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_called_once_with(
#             "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
#         )
#         loader_with_tables.connection.execute.assert_not_called()
#         loader_with_tables.connection.register.assert_not_called()
#
#     def test_load_data_multiple_schema_types_in_df(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"schema_type": ["table_A", "table_B"], "data": [1, 2]})
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_called_once_with(
#             "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
#         )
#         loader_with_tables.connection.execute.assert_not_called()
#         loader_with_tables.connection.register.assert_not_called()
#
#     def test_load_data_empty_dataframe_valid_schema_type(self, loader_with_tables, mock_logger):
#         # DataFrame with schema_type column but no rows
#         df = pd.DataFrame({"schema_type": pd.Series(dtype='str'), "data": pd.Series(dtype='int')})
#
#         loader_with_tables.load_data(df)
#
#         # This scenario (empty df but schema_type column exists) would lead to an error
#         # when trying df["schema_type"].iloc[0] because there's no row 0.
#         # The existing check for nunique() != 1 might catch it if column is empty,
#         # or iloc[0] will raise IndexError.
#         # The current code structure:
#         # 1. Checks for connection
#         # 2. Checks for "schema_type" column and nunique != 1
#         # If df["schema_type"] is empty, nunique() is 0, so it passes the "!=1" but might fail later.
#         # If the column exists but has no data, iloc[0] will fail.
#         # Let's assume the existing check `df["schema_type"].nunique() != 1` handles Series with no values.
#         # If nunique is 0, it's not 1.
#         # The error "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
#         # should be logged.
#         mock_logger.error.assert_called_once_with(
#             "傳入的 DataFrame 缺少 'schema_type' 欄位，或包含多個不同的 schema_type，無法決定目標資料表。"
#         )
#         loader_with_tables.connection.execute.assert_not_called()
#         loader_with_tables.connection.register.assert_not_called()
#
#     def test_load_data_connection_is_none(self, loader_with_tables, mock_logger):
#         loader_with_tables.connection = None # Simulate connection lost/not established
#         df = pd.DataFrame({"schema_type": ["table_A"], "data": [1]})
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_called_once_with("資料庫連線不存在，無法載入資料。")
#         # Can't check execute on None, but it shouldn't be reached.
#
#     def test_load_data_db_operation_exception(self, loader_with_tables, mock_logger):
#         df = pd.DataFrame({"schema_type": ["table_A"], "data": [1]})
#         loader_with_tables.connection.execute.side_effect = Exception("DB write error")
#
#         loader_with_tables.load_data(df)
#
#         mock_logger.error.assert_any_call("❌ 載入資料至資料表 'table_A' 時發生錯誤: DB write error", exc_info=True)
#         loader_with_tables.connection.unregister.assert_called_once_with("temp_df_view") # finally block

# Example of how to run this with pytest:
# Ensure pytest is installed: pip install pytest
# Navigate to the directory containing 'tests' and 'src'
# Run: pytest
#
# To run with coverage:
# pip install pytest-cov
# pytest --cov=src.data_pipeline_v15.database_loader --cov-report=html
# (then open htmlcov/index.html in a browser)

# A test for _connect could also be added, but it's simple and mostly calls duckdb.connect
# Testing _connect would involve patching duckdb.connect itself.
@patch('src.data_pipeline_v15.database_loader.duckdb.connect')
def test_connect_success(mock_duckdb_connect, mock_logger):
    loader = DatabaseLoader(database_file="test.db", logger=mock_logger)
    # _connect is called in __init__
    mock_duckdb_connect.assert_called_once_with(database="test.db", read_only=False)
    mock_logger.info.assert_any_call("✅ DuckDB 資料庫連線成功。")
    assert loader.connection is not None


@patch('src.data_pipeline_v15.database_loader.duckdb.connect', side_effect=Exception("Connection Failed"))
def test_connect_failure(mock_duckdb_connect_failed, mock_logger):
    with pytest.raises(Exception, match="Connection Failed"): # Expecting it to re-raise
        DatabaseLoader(database_file="test.db", logger=mock_logger)

    mock_duckdb_connect_failed.assert_called_once_with(database="test.db", read_only=False)
    mock_logger.critical.assert_called_once_with(
        "❌ 無法連線至 DuckDB 資料庫！錯誤：Connection Failed", exc_info=True
    )

def test_close_connection(mock_logger):
    # Need to simulate an existing connection
    loader = DatabaseLoader(database_file=":memory:", logger=mock_logger) # uses real duckdb for a moment

    # Replace the real connection with a mock to check close()
    mock_conn = MagicMock()
    loader.connection = mock_conn #

    loader.close_connection()
    mock_conn.close.assert_called_once()
    mock_logger.info.assert_any_call("資料庫連線已成功關閉。")
    assert loader.connection is None

def test_close_connection_no_connection(mock_logger):
    with patch('src.data_pipeline_v15.database_loader.duckdb.connect'): # prevent __init__ from connecting
        loader = DatabaseLoader(database_file="dummy.db", logger=mock_logger)
        loader.connection = None # Ensure no connection

        loader.close_connection()
        # Assert logger was not called with "closing" or "closed" if there's no connection
        # This depends on the exact logging messages. Current code only logs if self.connection is truthy.
        # So, no calls to info about closing/closed.
        # Check that logger.info wasn't called with the closing messages
        for call_args in mock_logger.info.call_args_list:
            assert "正在關閉 DuckDB 資料庫連線..." not in call_args[0][0]
            assert "資料庫連線已成功關閉。" not in call_args[0][0]


# --- Tests for load_parquet with Idempotency ---

@pytest.fixture
def sample_parquet_file_for_idempotency(tmp_path):
    """Creates a sample Parquet file for idempotency testing."""
    data = {
        "trading_date": pd.to_datetime(["2023-01-01", "2023-01-01", "2023-01-02"]),
        "product_id": ["TXF", "TXF", "MXF"],
        "expiry_month": ["202301", "202302", "202301"],
        "strike_price": [14000.0, 14000.0, 7000.0],
        "option_type": ["C", "P", "C"],
        "open": [100.0, 50.0, 200.0],
        "high": [110.0, 55.0, 210.0],
        "low": [90.0, 45.0, 190.0],
        "close": [105.0, 52.0, 205.0],
        "volume": [1000, 500, 800],
        "open_interest": [5000, 2000, 3000],
        "delta": [0.5, -0.4, 0.6]
    }
    df = pd.DataFrame(data)
    parquet_path = tmp_path / "sample_fact_daily_ohlc.parquet"
    df.to_parquet(parquet_path)
    return parquet_path

# Patch _connect for all load_parquet tests to avoid actual DB operations for most,
# but for idempotency test, we need a real in-memory DB.
def test_load_parquet_idempotent(tmp_path, mock_logger, sample_parquet_file_for_idempotency):
    """
    Tests the idempotent behavior of load_parquet when primary keys are defined.
    """
    db_file = tmp_path / "test_idempotent.db"
    table_name = "fact_daily_ohlc" # Matches the sample parquet data's intended table

    # 1. Setup DatabaseLoader with mocked schema config to include primary key
    # We are testing the DatabaseLoader's interaction with a real DB here.
    # So, we don't mock duckdb.connect itself, but we control the schema interpretation.

    # loader = DatabaseLoader(database_file=str(db_file), logger=mock_logger)
    # We need to ensure _load_schema_configurations is called *after* we can mock it,
    # or that it uses a schemas file path we can control.
    # The easiest is to use @patch on the class or its method.

    with patch.object(DatabaseLoader, '_load_schema_configurations', autospec=True) as mock_load_schemas:

        loader = DatabaseLoader(database_file=str(db_file), logger=mock_logger)

        # Configure the mock behavior inside the context
        loader.allowed_table_names = {table_name} # Manually set after mock
        loader.table_primary_keys = {
            table_name: ["trading_date", "product_id", "expiry_month", "strike_price", "option_type"]
        }
        mock_load_schemas.assert_called_once() # Ensure it was called during init

        # Ensure connection is established (it's called in __init__)
        assert loader.connection is not None, "Database connection should be established."

        # 2. First load
        loader.load_parquet(table_name, str(sample_parquet_file_for_idempotency))

        count_after_first_load = loader.connection.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
        assert count_after_first_load == 3, "Row count after first load should be 3."
        mock_logger.info.assert_any_call(f"資料表 '{table_name}' 已使用定義的欄位和主鍵建立。")
        mock_logger.info.assert_any_call(f"資料已使用 ON CONFLICT DO NOTHING 策略嘗試載入至資料表 '{table_name}'。")


        # 3. Second load (same data)
        loader.load_parquet(table_name, str(sample_parquet_file_for_idempotency))

        count_after_second_load = loader.connection.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
        assert count_after_second_load == 3, "Row count after second load should remain 3 due to idempotency."

        # Check that no critical errors were logged during the second attempt.
        # Errors like "file not found" or "table not allowed" are fine if they are part of other tests,
        # but for this specific load_parquet call, after the first successful one,
        # a second identical call should not produce new operational errors.
        # We can check the log calls specific to the second load if needed, but for now, a general error check.
        # Filter out "Table already exists" type warnings if they occur and are expected.
        # For now, let's check that logger.error was not called after the initial setup phase.
        # Count error calls before second load
        error_calls_before_second_load = mock_logger.error.call_count
        loader.load_parquet(table_name, str(sample_parquet_file_for_idempotency)) # This is the third call, but second *effective* data load call for testing idempotency
        count_after_third_load = loader.connection.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
        assert count_after_third_load == 3, "Row count after third load should still remain 3."

        # Check if any new errors were logged specifically by the third call.
        # This is a bit tricky as load_parquet itself logs.
        # The key is that `ON CONFLICT DO NOTHING` should not raise an error.
        # We can check the number of error calls hasn't increased beyond what's expected.
        # Let's assume the setup and first load might log legitimate errors if files were missing (not in this test).
        # The critical part is that the second (and third) identical load does not itself cause an exception or error log
        # related to data insertion.
        # A simple check:
        # Store error calls before the *very first* load_parquet in this test.
        # This is tricky as __init__ itself might log.
        # Alternative: Check specific log messages.

        # Check that the "ON CONFLICT DO NOTHING" message was logged again for the subsequent calls.
        log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert log_calls.count(f"資料已使用 ON CONFLICT DO NOTHING 策略嘗試載入至資料表 '{table_name}'。") >= 3 # Init + 3 calls to load_parquet

        # Verify table structure includes PRIMARY KEY (example for DuckDB)
        table_info = loader.connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        # DuckDB's PRAGMA table_info returns columns: cid, name, type, notnull, dflt_value, pk
        # pk is 1 for a column that is part of the primary key, 0 otherwise.
        # For composite PK, multiple columns will have pk > 0, and the value indicates their order in PK.
        pk_columns_from_db = {row[1] for row in table_info if row[5] > 0}
        expected_pk_columns = {"trading_date", "product_id", "expiry_month", "strike_price", "option_type"}
        assert pk_columns_from_db == expected_pk_columns, \
            f"Primary key columns in DB {pk_columns_from_db} do not match expected {expected_pk_columns}"

        loader.close_connection()
