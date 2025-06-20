import pytest
import duckdb
import pandas as pd
from data_pipeline_v15.data_transformer import DataTransformer
from data_pipeline_v15.utils.logger import setup_logger # Changed import
import os # For path operations

# Initialize logger for the test module
# Ensure logs directory exists for test logs
LOG_DIR_TEST = "logs_test"
LOG_FILE_TEST = "test_data_transformer.log"
if not os.path.exists(LOG_DIR_TEST):
    os.makedirs(LOG_DIR_TEST)
logger = setup_logger(LOG_DIR_TEST, LOG_FILE_TEST, debug_mode=True)


@pytest.fixture
def db_connection():
    """Fixture to set up an in-memory DuckDB for testing."""
    con = duckdb.connect(database=':memory:', read_only=False)
    # Create a sample valid_data table
    sample_data = {
        'id': [1, 2, 3, 4],
        'name': ['test1', 'test2', 'test3', 'test4'],
        'amount': [100, -200, 300, -400],
        'timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'])
    }
    df = pd.DataFrame(sample_data)
    con.execute("CREATE TABLE valid_data AS SELECT * FROM df")
    logger.info("Test database setup: 'valid_data' table created with sample data.")
    yield con
    logger.info("Test database teardown: Closing connection.")
    con.close()

def test_data_transformer_integration(db_connection):
    """
    Tests the DataTransformer's transform_data method.
    """
    logger.info("Starting test_data_transformer_integration.")
    # Initialize DataTransformer with the in-memory database connection
    # DataTransformer expects a db_path, so we use ':memory:'
    # For context manager usage, we'll manually manage connection for test transformer

    transformer = DataTransformer(db_path=':memory:')
    transformer.con = db_connection # Assign the connection from fixture

    try:
        logger.info("Attempting to run data transformation.")
        transformer.transform_data()
        logger.info("Data transformation process completed.")

        # Check if 'transformed_data' table exists
        table_check_result = db_connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transformed_data';").fetchall()
        logger.info(f"Query for 'transformed_data' table existence returned: {table_check_result}")
        assert len(table_check_result) > 0, "The 'transformed_data' table was not created."

        # Verify the content of 'transformed_data'
        transformed_df = db_connection.execute("SELECT * FROM transformed_data ORDER BY id").fetchdf()
        logger.info(f"Transformed data fetched:\n{transformed_df}")

        assert not transformed_df.empty, "Transformed data should not be empty."
        assert 'absolute_amount' in transformed_df.columns, "'absolute_amount' column is missing."
        assert 'transaction_type' in transformed_df.columns, "'transaction_type' column is missing."

        # Expected values
        expected_absolutes = [100, 200, 300, 400]
        expected_types = ['credit', 'debit', 'credit', 'debit']

        pd.testing.assert_series_equal(transformed_df['absolute_amount'], pd.Series(expected_absolutes, name='absolute_amount', dtype='float64'), check_dtype=False)
        logger.info("Assertion for 'absolute_amount' passed.")
        pd.testing.assert_series_equal(transformed_df['transaction_type'], pd.Series(expected_types, name='transaction_type'), check_dtype=False)
        logger.info("Assertion for 'transaction_type' passed.")

        logger.info("All assertions for transformed data passed.")

    except Exception as e:
        logger.error(f"Exception during test_data_transformer_integration: {e}", exc_info=True)
        pytest.fail(f"Data transformation test failed due to an exception: {e}")
    finally:
        # The transformer's own __exit__ would close its connection,
        # but here we assigned an external one. The fixture handles closure.
        # If transformer created its own connection, we'd call transformer.__exit__(None, None, None)
        pass

    logger.info("test_data_transformer_integration completed successfully.")

@pytest.fixture
def failing_db_transformer():
    """Fixture to create a DataTransformer that will fail during transformation."""
    # This transformer will use its own connection to a non-existent table initially
    # to simulate an operational error.
    transformer = DataTransformer(db_path=':memory:') # Uses its own in-memory DB

    # Setup: connect and create a situation that might cause an error
    # For example, drop 'valid_data' if it's expected by transform_data
    conn = duckdb.connect(':memory:')
    # conn.execute("CREATE TABLE some_other_table (id INTEGER)") # No valid_data table
    transformer.con = conn # Override connection
    logger.info("Failing DB transformer setup: Connected to a clean in-memory DB (no 'valid_data').")
    yield transformer
    logger.info("Failing DB transformer teardown: Closing connection.")
    if transformer.con:
        transformer.con.close()


def test_data_transformer_error_handling(failing_db_transformer):
    """
    Tests the DataTransformer's error handling during the transform_data method.
    It expects transform_data to raise an exception if 'valid_data' table doesn't exist.
    """
    logger.info("Starting test_data_transformer_error_handling.")
    transformer = failing_db_transformer

    with pytest.raises(Exception) as excinfo:
        logger.info("Attempting to run data transformation (expected to fail).")
        transformer.transform_data() # This should fail as valid_data does not exist

    logger.info(f"Transformation failed as expected. Exception: {excinfo.value}")
    # Updated expected error message to be more aligned with DuckDB's output
    expected_error_msg_part = "table with name valid_data does not exist"
    assert expected_error_msg_part in str(excinfo.value).lower(), \
        f"Expected an error message containing '{expected_error_msg_part}', but got: {str(excinfo.value).lower()}"

    logger.info("test_data_transformer_error_handling completed successfully.")

# Example of how to run tests with pytest in a notebook or script:
# if __name__ == "__main__":
#     logger.info("Running pytest for DataTransformer tests.")
#     # You would typically run pytest from the command line.
#     # This is just for illustration if running this file directly.
#     # Note: Running pytest.main() from script can have side effects.
#     # It's better to use `poetry run pytest tests/test_data_transformer.py`
#     # pytest.main(['-v', __file__])
#     pass
