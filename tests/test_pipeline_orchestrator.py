# tests/test_pipeline_orchestrator.py
# Standard library imports
import datetime
import logging # For mock_logger spec
import os # For os.path spec in mock_os_tools

# Third-party imports
import pytest
from unittest import mock # Already imported but good to note

# Local application/library specific imports
from src.data_pipeline_v15.pipeline_orchestrator import PipelineOrchestrator
from src.data_pipeline_v15.file_parser import FileParser # Needed for spec
from src.data_pipeline_v15.manifest_manager import ManifestManager # Needed for spec
from src.data_pipeline_v15.database_loader import DatabaseLoader # Needed for spec
from src.data_pipeline_v15.core import constants
# from src.data_pipeline_v15.utils.logger import setup_logger # For mock spec if needed, or mock it directly

# --- Base Test Configuration ---
BASE_PATH = "/tmp/test_project" # Default, instance will use tmp_path
PROJECT_FOLDER_NAME = "data_pipeline_project"
DATABASE_NAME = "test_db.duckdb"
LOG_NAME = "test_pipeline.log"
TARGET_ZIP_FILES = "" # Default to no specific target files for most tests
DEBUG_MODE = False

# --- Mock Fixtures ---

@pytest.fixture
def mock_file_parser():
    parser = mock.MagicMock(spec=FileParser)
    # Default return for parse_file, can be overridden in tests
    parser.parse_file.return_value = {
        constants.KEY_STATUS: constants.STATUS_SUCCESS,
        constants.KEY_FILE: "test_file.csv",
        constants.KEY_TABLE: "test_table",
        constants.KEY_COUNT: 100,
        constants.KEY_PATH: "/tmp/test_project/processed/test_table/test_file.parquet", # Example path
        constants.KEY_REASON: "Successfully processed"
    }
    return parser

@pytest.fixture
def mock_manifest_manager():
    manager = mock.MagicMock(spec=ManifestManager)
    manager.has_been_processed.return_value = False # Default: file is not processed
    manager.load_or_create_manifest.return_value = None # Simple return
    manager.update_manifest.return_value = None # Simple return
    manager.archive_path = "/tmp/test_project/archive"
    return manager

@pytest.fixture
def mock_db_loader():
    loader = mock.MagicMock(spec=DatabaseLoader)
    loader.load_parquet.return_value = None # Changed from load_data to load_parquet
    loader.close_connection.return_value = None # Simple return
    return loader

@pytest.fixture
def mock_logger_setup(monkeypatch): # Mocks the setup_logger function
    mock_actual_logger = mock.MagicMock(spec=logging.Logger)
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.setup_logger", lambda log_path, log_name, debug_mode: mock_actual_logger)
    return mock_actual_logger # Return the logger instance that will be used

@pytest.fixture
def mock_os_tools(monkeypatch):
    mock_os_module = mock.MagicMock(spec=os)
    mock_os_path = mock.MagicMock(spec=os.path)
    mock_os_path.join.side_effect = lambda *args: os.path.join(*[str(arg) for arg in args])
    mock_os_path.exists.return_value = True
    mock_os_path.isfile.return_value = True
    mock_os_path.dirname.side_effect = os.path.dirname
    mock_os_module.path = mock_os_path
    mock_os_module.listdir.return_value = ["test_file.csv"]
    # Allow the original os.makedirs to be called while still tracking calls via the mock
    original_makedirs = os.makedirs
    mock_os_module.makedirs.side_effect = lambda path, exist_ok=False: original_makedirs(path, exist_ok=exist_ok)
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.os", mock_os_module)
    return mock_os_module

@pytest.fixture
def mock_shutil_move(monkeypatch):
    mock_move = mock.MagicMock()
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.shutil.move", mock_move)
    return mock_move

@pytest.fixture
def mock_datetime_now(monkeypatch):
    fixed_datetime_instance = datetime.datetime(2024, 1, 1, 12, 0, 0)
    mock_datetime_class = mock.MagicMock(spec=datetime.datetime)
    mock_datetime_class.now.return_value = fixed_datetime_instance
    mock_datetime_class.strptime = datetime.datetime.strptime
    monkeypatch.setattr("src.data_pipeline_v15.pipeline_orchestrator.datetime", mock_datetime_class)
    return mock_datetime_class


# --- PipelineOrchestrator Instance Fixture ---

@pytest.fixture
def orchestrator_instance(
    mock_file_parser,
    mock_manifest_manager,
    mock_db_loader,
    mock_logger_setup,
    mock_os_tools,
    mock_shutil_move,
    mock_datetime_now,
    tmp_path
):
    test_base_path = str(tmp_path / "project_root")
    orchestrator = PipelineOrchestrator(
        base_path=test_base_path,
        project_folder_name=PROJECT_FOLDER_NAME,
        database_name=DATABASE_NAME,
        log_name=LOG_NAME,
        target_zip_files=TARGET_ZIP_FILES,
        debug_mode=DEBUG_MODE
    )
    orchestrator.file_parser = mock_file_parser
    orchestrator.manifest_manager = mock_manifest_manager
    orchestrator.db_loader = mock_db_loader
    # The logger used by the orchestrator instance will be the one returned by mock_logger_setup
    orchestrator.logger = mock_logger_setup
    return orchestrator

def test_run_success_new_file(orchestrator_instance, mock_os_tools, mock_shutil_move, mock_manifest_manager, mock_file_parser, mock_db_loader, monkeypatch):
    # Arrange: Configure mocks for a successful file processing scenario
    test_filename = "new_file.csv"
    test_file_hash = "dummy_hash_for_new_file"

    mock_os_tools.listdir.return_value = [test_filename]

    # Mock ManifestManager.get_file_hash to return a consistent hash for the input file path
    # This is called by the orchestrator before has_been_processed
    mock_get_hash = mock.MagicMock(return_value=test_file_hash)
    monkeypatch.setattr("src.data_pipeline_v15.manifest_manager.ManifestManager.get_file_hash", mock_get_hash)

    mock_manifest_manager.has_been_processed.return_value = False

    parsed_file_path_in_staging = "/tmp/project_root/data_pipeline_project/processed/test_table/new_file.parquet"
    mock_file_parser.parse_file.return_value = {
        constants.KEY_STATUS: constants.STATUS_SUCCESS,
        constants.KEY_FILE: test_filename,
        constants.KEY_TABLE: "test_table",
        constants.KEY_COUNT: 120,
        constants.KEY_PATH: parsed_file_path_in_staging, # Path to the (mocked) processed file
        constants.KEY_REASON: "Successfully parsed"
    }

    # Act: Run the orchestrator
    orchestrator_instance.run()

    # Assert
    # 1. Directories were set up
    mock_os_tools.makedirs.assert_any_call(orchestrator_instance.input_path, exist_ok=True)
    mock_os_tools.makedirs.assert_any_call(orchestrator_instance.processed_path, exist_ok=True)
    # Add more checks for other dirs if vital, e.g., quarantine_path, db_path, log_path

    # 2. Manifest was loaded
    mock_manifest_manager.load_or_create_manifest.assert_called_once()

    # 3. File was checked in manifest
    mock_manifest_manager.has_been_processed.assert_called_once_with(test_file_hash)

    # 4. File parser was called
    # Verify that get_file_hash was called by orchestrator with the full input file path
    expected_input_file_path_for_hash = os.path.join(orchestrator_instance.input_path, test_filename)
    mock_get_hash.assert_any_call(expected_input_file_path_for_hash) # Called by orchestrator

    # The update_manifest in ManifestManager also calls get_file_hash.
    # If it's called with just filename, it might need separate handling or the mock needs to be more specific.
    # For now, let's assume the above call is the primary one we care about for has_been_processed.
    # If update_manifest's internal get_file_hash(filename) is problematic, that's a separate issue in ManifestManager.
    # We can make the mock more robust if needed:
    def side_effect_get_hash(path_arg):
        if path_arg == expected_input_file_path_for_hash:
            return test_file_hash
        # If ManifestManager's update_manifest calls get_file_hash(basename)
        # and we want to provide a hash for that too for consistency in its own logic:
        # elif path_arg == test_filename:
        #     return "another_dummy_hash_for_basename_in_update"
        return None # Default for other unexpected calls
    mock_get_hash.side_effect = side_effect_get_hash

    expected_input_file_path = os.path.join(orchestrator_instance.input_path, test_filename)
    mock_file_parser.parse_file.assert_called_once_with(
        expected_input_file_path,
        orchestrator_instance.processed_path # This is the staging_dir for parser
    )

    # 5. Database loader was called
    mock_db_loader.load_parquet.assert_called_once_with("test_table", parsed_file_path_in_staging)

    # 6. File was moved to processed directory
    expected_processed_file_path = os.path.join(orchestrator_instance.processed_path, test_filename)
    mock_shutil_move.assert_called_once_with(expected_input_file_path, expected_processed_file_path)

    # 7. Manifest was updated with success status
    mock_manifest_manager.update_manifest.assert_called_once_with(
        expected_input_file_path,
        constants.STATUS_SUCCESS,
        mock.ANY, # The exact success message can be flexible
        original_filename=test_filename
    )

    # 8. Database connection closed
    mock_db_loader.close_connection.assert_called_once()

def test_run_failure_new_file(orchestrator_instance, mock_os_tools, mock_shutil_move, mock_manifest_manager, mock_file_parser, mock_db_loader, monkeypatch):
    # Arrange: Configure mocks for a failed file processing scenario
    test_filename = "error_file.csv"
    test_file_hash = "dummy_hash_for_error_file"
    mock_os_tools.listdir.return_value = [test_filename]

    mock_get_hash = mock.MagicMock(return_value=test_file_hash)
    monkeypatch.setattr("src.data_pipeline_v15.manifest_manager.ManifestManager.get_file_hash", mock_get_hash)

    mock_manifest_manager.has_been_processed.return_value = False

    mock_file_parser.parse_file.return_value = {
        constants.KEY_STATUS: constants.STATUS_ERROR,
        constants.KEY_FILE: test_filename,
        constants.KEY_REASON: "Failed to parse file due to format error"
        # No KEY_PATH, KEY_TABLE, KEY_COUNT for error status from parser
    }

    # Act: Run the orchestrator
    orchestrator_instance.run()

    # Assert
    # 1. Basic setup calls (makedirs, load_manifest) - similar to success test, can be refactored to a common check if many tests need it
    mock_os_tools.makedirs.assert_any_call(orchestrator_instance.input_path, exist_ok=True)
    mock_manifest_manager.load_or_create_manifest.assert_called_once()

    # 2. File was checked in manifest
    mock_manifest_manager.has_been_processed.assert_called_once_with(test_file_hash)

    # 3. File parser was called
    expected_input_file_path_for_hash = os.path.join(orchestrator_instance.input_path, test_filename)
    # mock_get_hash.assert_any_call(expected_input_file_path_for_hash) # Check first call
    def side_effect_get_hash_failure(path_arg):
        if path_arg == expected_input_file_path_for_hash:
            return test_file_hash
        return None # Default for other calls like from update_manifest
    mock_get_hash.side_effect = side_effect_get_hash_failure

    expected_input_file_path = os.path.join(orchestrator_instance.input_path, test_filename)
    mock_file_parser.parse_file.assert_called_once_with(
        expected_input_file_path,
        orchestrator_instance.processed_path
    )

    # 4. Database loader should NOT be called
    mock_db_loader.load_parquet.assert_not_called()

    # 5. File was moved to quarantine directory
    expected_quarantine_file_path = os.path.join(orchestrator_instance.quarantine_path, test_filename)
    mock_shutil_move.assert_called_once_with(expected_input_file_path, expected_quarantine_file_path)

    # 6. Manifest was updated with error status
    mock_manifest_manager.update_manifest.assert_called_once_with(
        expected_input_file_path,
        constants.STATUS_ERROR,
        "Failed to parse file due to format error", # Exact reason from parser's result
        original_filename=test_filename
    )

    # 7. Database connection closed
    mock_db_loader.close_connection.assert_called_once()

def test_run_skip_already_processed_file(orchestrator_instance, mock_os_tools, mock_shutil_move, mock_manifest_manager, mock_file_parser, mock_db_loader, monkeypatch):
    # Arrange: Configure mocks for a scenario where the file is already processed
    test_filename = "processed_file.csv"
    test_file_hash = "dummy_hash_for_processed_file"
    mock_os_tools.listdir.return_value = [test_filename]

    mock_get_hash = mock.MagicMock(return_value=test_file_hash)
    monkeypatch.setattr("src.data_pipeline_v15.manifest_manager.ManifestManager.get_file_hash", mock_get_hash)

    mock_manifest_manager.has_been_processed.return_value = True # Key for this test

    # Act: Run the orchestrator
    orchestrator_instance.run()

    # Assert
    # 1. Basic setup calls (makedirs, load_manifest)
    mock_os_tools.makedirs.assert_any_call(orchestrator_instance.input_path, exist_ok=True)
    mock_manifest_manager.load_or_create_manifest.assert_called_once()

    # 2. File was checked in manifest
    mock_manifest_manager.has_been_processed.assert_called_once_with(test_file_hash)

    expected_input_file_path_for_hash = os.path.join(orchestrator_instance.input_path, test_filename)
    mock_get_hash.assert_called_once_with(expected_input_file_path_for_hash)


    # 3. File parser should NOT be called
    mock_file_parser.parse_file.assert_not_called()

    # 4. Database loader should NOT be called
    mock_db_loader.load_parquet.assert_not_called()

    # 5. File should NOT be moved
    mock_shutil_move.assert_not_called()

    # 6. Manifest should NOT be updated
    mock_manifest_manager.update_manifest.assert_not_called()

    # 7. Database connection closed (still called in finally block)
    mock_db_loader.close_connection.assert_called_once()

def test_run_empty_input_directory(orchestrator_instance, mock_os_tools, mock_shutil_move, mock_manifest_manager, mock_file_parser, mock_db_loader):
    # Arrange: Configure os.listdir to return an empty list
    mock_os_tools.listdir.return_value = [] # Key for this test

    # Act: Run the orchestrator
    orchestrator_instance.run()

    # Assert
    # 1. Basic setup calls (makedirs, load_manifest)
    mock_os_tools.makedirs.assert_any_call(orchestrator_instance.input_path, exist_ok=True)
    mock_manifest_manager.load_or_create_manifest.assert_called_once()

    # 2. os.listdir was called for the input path
    mock_os_tools.listdir.assert_called_once_with(orchestrator_instance.input_path)

    # 3. No processing should occur, so these should not be called:
    mock_manifest_manager.has_been_processed.assert_not_called()
    mock_file_parser.parse_file.assert_not_called()
    mock_db_loader.load_parquet.assert_not_called()
    mock_shutil_move.assert_not_called()
    mock_manifest_manager.update_manifest.assert_not_called()

    # 4. Logger should indicate no files found
    assert any(
        "輸入資料夾中沒有找到任何要處理的檔案" in call_args[0][0]
        for call_args in orchestrator_instance.logger.warning.call_args_list
    ), "Expected warning log for empty directory not found"

    # 5. Database connection closed (still called in finally block)
    mock_db_loader.close_connection.assert_called_once()

def test_run_success_zip_multiple_successful_sub_items(orchestrator_instance, mock_os_tools, mock_shutil_move, mock_manifest_manager, mock_file_parser, mock_db_loader, monkeypatch):
    # Arrange
    zip_filename = "archive_multiple_ok.zip"
    zip_file_hash = "dummy_hash_for_zip_multiple_ok"
    mock_os_tools.listdir.return_value = [zip_filename]

    mock_get_hash = mock.MagicMock(return_value=zip_file_hash)
    monkeypatch.setattr("src.data_pipeline_v15.manifest_manager.ManifestManager.get_file_hash", mock_get_hash)

    mock_manifest_manager.has_been_processed.return_value = False

    sub_item1_name = f"{zip_filename}/file1.csv"
    sub_item1_staging_path = "/tmp/project_root/data_pipeline_project/processed/table1/file1.parquet"
    sub_item2_name = f"{zip_filename}/file2.csv"
    sub_item2_staging_path = "/tmp/project_root/data_pipeline_project/processed/table2/file2.parquet"

    mock_file_parser.parse_file.return_value = {
        constants.KEY_STATUS: constants.STATUS_GROUP_RESULT,
        constants.KEY_FILE: zip_filename,
        constants.KEY_RESULTS: [
            {
                constants.KEY_STATUS: constants.STATUS_SUCCESS,
                constants.KEY_FILE: sub_item1_name,
                constants.KEY_TABLE: "table1",
                constants.KEY_COUNT: 10,
                constants.KEY_PATH: sub_item1_staging_path,
                constants.KEY_REASON: "Parsed file1.csv"
            },
            {
                constants.KEY_STATUS: constants.STATUS_SUCCESS,
                constants.KEY_FILE: sub_item2_name,
                constants.KEY_TABLE: "table2",
                constants.KEY_COUNT: 20,
                constants.KEY_PATH: sub_item2_staging_path,
                constants.KEY_REASON: "Parsed file2.csv"
            }
        ]
    }

    # Act
    orchestrator_instance.run()

    # Assert
    mock_manifest_manager.has_been_processed.assert_called_once_with(zip_file_hash)

    expected_zip_input_path = os.path.join(orchestrator_instance.input_path, zip_filename)
    # mock_get_hash.assert_any_call(expected_zip_input_path) # Main call by orchestrator
    def side_effect_get_hash_zip_multi_succ(path_arg):
        if path_arg == expected_zip_input_path:
            return zip_file_hash
        return None # Default for other calls like from update_manifest
    mock_get_hash.side_effect = side_effect_get_hash_zip_multi_succ

    mock_file_parser.parse_file.assert_called_once_with(
        expected_zip_input_path,
        orchestrator_instance.processed_path
    )

    # Check DatabaseLoader calls for each successful sub-item
    mock_db_loader.load_parquet.assert_any_call("table1", sub_item1_staging_path)
    mock_db_loader.load_parquet.assert_any_call("table2", sub_item2_staging_path)
    assert mock_db_loader.load_parquet.call_count == 2

    # Check file moved to processed directory
    expected_zip_processed_path = os.path.join(orchestrator_instance.processed_path, zip_filename)
    mock_shutil_move.assert_called_once_with(expected_zip_input_path, expected_zip_processed_path)

    # Check manifest updated with overall success for the ZIP
    mock_manifest_manager.update_manifest.assert_called_once_with(
        expected_zip_input_path, # Changed from zip_filename
        constants.STATUS_SUCCESS,
        mock.ANY, # Message can be flexible, e.g., "ZIP 'archive_multiple_ok.zip' 處理完成..."
        original_filename=zip_filename
    )
    mock_db_loader.close_connection.assert_called_once()

def test_run_zip_partial_success_sub_items(orchestrator_instance, mock_os_tools, mock_shutil_move, mock_manifest_manager, mock_file_parser, mock_db_loader, monkeypatch):
    # Arrange
    zip_filename = "archive_partial_ok.zip"
    zip_file_hash = "dummy_hash_for_zip_partial_ok"
    mock_os_tools.listdir.return_value = [zip_filename]

    mock_get_hash = mock.MagicMock(return_value=zip_file_hash)
    monkeypatch.setattr("src.data_pipeline_v15.manifest_manager.ManifestManager.get_file_hash", mock_get_hash)

    mock_manifest_manager.has_been_processed.return_value = False

    sub_item_success_name = f"{zip_filename}/file_ok.csv"
    sub_item_success_staging_path = "/tmp/project_root/data_pipeline_project/processed/table_ok/file_ok.parquet"
    sub_item_fail_name = f"{zip_filename}/file_bad.csv"

    mock_file_parser.parse_file.return_value = {
        constants.KEY_STATUS: constants.STATUS_GROUP_RESULT,
        constants.KEY_FILE: zip_filename,
        constants.KEY_RESULTS: [
            {
                constants.KEY_STATUS: constants.STATUS_SUCCESS,
                constants.KEY_FILE: sub_item_success_name,
                constants.KEY_TABLE: "table_ok",
                constants.KEY_COUNT: 50,
                constants.KEY_PATH: sub_item_success_staging_path,
                constants.KEY_REASON: "Parsed file_ok.csv"
            },
            {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: sub_item_fail_name,
                constants.KEY_REASON: "Failed to parse file_bad.csv"
            }
        ]
    }

    # Act
    orchestrator_instance.run()

    # Assert
    mock_manifest_manager.has_been_processed.assert_called_once_with(zip_file_hash)

    expected_zip_input_path = os.path.join(orchestrator_instance.input_path, zip_filename)
    # mock_get_hash.assert_any_call(expected_zip_input_path)
    def side_effect_get_hash_zip_partial(path_arg):
        if path_arg == expected_zip_input_path:
            return zip_file_hash
        return None
    mock_get_hash.side_effect = side_effect_get_hash_zip_partial

    mock_file_parser.parse_file.assert_called_once_with(
        expected_zip_input_path,
        orchestrator_instance.processed_path
    )

    # Check DatabaseLoader calls only for the successful sub-item
    mock_db_loader.load_parquet.assert_called_once_with("table_ok", sub_item_success_staging_path)

    # Check file moved to processed directory (because at least one sub-item was successful)
    expected_zip_processed_path = os.path.join(orchestrator_instance.processed_path, zip_filename)
    mock_shutil_move.assert_called_once_with(expected_zip_input_path, expected_zip_processed_path)

    # Check manifest updated with overall success for the ZIP
    mock_manifest_manager.update_manifest.assert_called_once_with(
        expected_zip_input_path,
        constants.STATUS_SUCCESS, # Still overall success if at least one sub-file is loaded
        mock.ANY,
        original_filename=zip_filename
    )
    mock_db_loader.close_connection.assert_called_once()

def test_run_zip_all_sub_items_fail(orchestrator_instance, mock_os_tools, mock_shutil_move, mock_manifest_manager, mock_file_parser, mock_db_loader, monkeypatch):
    # Arrange
    zip_filename = "archive_all_fail.zip"
    zip_file_hash = "dummy_hash_for_zip_all_fail"
    mock_os_tools.listdir.return_value = [zip_filename]

    mock_get_hash = mock.MagicMock(return_value=zip_file_hash)
    monkeypatch.setattr("src.data_pipeline_v15.manifest_manager.ManifestManager.get_file_hash", mock_get_hash)

    mock_manifest_manager.has_been_processed.return_value = False

    sub_item1_fail_name = f"{zip_filename}/file1_bad.csv"
    sub_item2_fail_name = f"{zip_filename}/file2_bad.csv"

    mock_file_parser.parse_file.return_value = {
        constants.KEY_STATUS: constants.STATUS_GROUP_RESULT,
        constants.KEY_FILE: zip_filename,
        constants.KEY_RESULTS: [
            {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: sub_item1_fail_name,
                constants.KEY_REASON: "Failed to parse file1_bad.csv"
            },
            {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: sub_item2_fail_name,
                constants.KEY_REASON: "Failed to parse file2_bad.csv"
            }
        ]
    }

    # Act
    orchestrator_instance.run()

    # Assert
    mock_manifest_manager.has_been_processed.assert_called_once_with(zip_file_hash)

    expected_zip_input_path = os.path.join(orchestrator_instance.input_path, zip_filename)
    # mock_get_hash.assert_any_call(expected_zip_input_path)
    def side_effect_get_hash_zip_all_fail(path_arg):
        if path_arg == expected_zip_input_path:
            return zip_file_hash
        return None
    mock_get_hash.side_effect = side_effect_get_hash_zip_all_fail

    mock_file_parser.parse_file.assert_called_once_with(
        expected_zip_input_path,
        orchestrator_instance.processed_path
    )

    # Check DatabaseLoader is NOT called
    mock_db_loader.load_parquet.assert_not_called()

    # Check file moved to quarantine directory
    expected_zip_quarantine_path = os.path.join(orchestrator_instance.quarantine_path, zip_filename)
    mock_shutil_move.assert_called_once_with(expected_zip_input_path, expected_zip_quarantine_path)

    # Check manifest updated with overall error for the ZIP
    mock_manifest_manager.update_manifest.assert_called_once_with(
        expected_zip_input_path,
        constants.STATUS_ERROR, # Overall status should be error
        mock.ANY,
        original_filename=zip_filename
    )
    mock_db_loader.close_connection.assert_called_once()
