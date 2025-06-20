import pandas as pd
import duckdb
from data_pipeline_v15.utils.logger import setup_logger # Changed import
import os # For path operations if needed for logger setup

# Configure logger for this module
# Using fixed names for log directory and file for simplicity here.
# In a real scenario, these might come from config or be structured better.
LOG_DIR = "logs" # Example log directory
LOG_FILE = "data_transformer.log"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
logger = setup_logger(LOG_DIR, LOG_FILE, debug_mode=True)


class DataTransformer:
    def __init__(self, db_path='dummy.db'):
        self.db_path = db_path
        self.con = None

    def __enter__(self):
        self.con = duckdb.connect(database=self.db_path, read_only=False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.con:
            self.con.close()

    def transform_data(self):
        """
        Transforms data in the 'valid_data' table and stores it in a new table 'transformed_data'.
        """
        if not self.con:
            raise ConnectionError("Database connection is not established.")

        try:
            logger.info("Starting data transformation process.")

            # Example transformation: Convert 'amount' column to absolute values
            # and add a new column 'transaction_type' based on 'amount'
            self.con.execute("""
                CREATE TABLE transformed_data AS
                SELECT
                    *,
                    ABS(amount) as absolute_amount,
                    CASE
                        WHEN amount < 0 THEN 'debit'
                        ELSE 'credit'
                    END as transaction_type
                FROM valid_data
            """)
            logger.info("Successfully transformed data and created 'transformed_data' table.")

            # Example: Log some info about the transformed data
            count_query = "SELECT COUNT(*) FROM transformed_data"
            transformed_count = self.con.execute(count_query).fetchone()[0]
            logger.info(f"Number of rows in transformed_data: {transformed_count}")

            if transformed_count > 0:
                sample_query = "SELECT * FROM transformed_data LIMIT 5"
                sample_data = self.con.execute(sample_query).df()
                logger.info(f"Sample of transformed data:\n{sample_data}")


        except Exception as e:
            logger.error(f"Error during data transformation: {e}")
            raise

        return True # Indicate success
