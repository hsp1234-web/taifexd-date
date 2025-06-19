# -*- coding: utf-8 -*-
import logging
import sys
from datetime import datetime
import pytz # For timezone handling

class TaipeiFormatter(logging.Formatter):
    """
    Custom logging formatter that uses Asia/Taipei timezone for timestamps.
    """
    def formatTime(self, record, datefmt=None):
        """
        Return the creation time of the specified LogRecord as a formatted string.
        This method is overridden to use Asia/Taipei timezone.
        """
        dt = datetime.fromtimestamp(record.created, tz=pytz.timezone('Asia/Taipei'))
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                # More precise ISO format with milliseconds
                s = dt.isoformat(timespec='milliseconds')
            except TypeError: # Fallback for Python versions older than 3.6
                s = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
                s = s[:-3] # Approximate milliseconds by trimming microseconds
        return s

class Logger:
    """
    Custom Logger class that sets up a logger with specific handlers and formatting.
    It logs to both a file (always at DEBUG level) and the console (configurable level).
    Timestamps are in Asia/Taipei timezone.
    """
    _log_level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
        "step": logging.INFO, # Custom level, maps to INFO
        "substep": logging.DEBUG, # Custom level, maps to DEBUG
        "success": logging.INFO, # Custom level, maps to INFO
    }

    def __init__(self, name='data_pipeline', log_file_path='pipeline.log', level='INFO'):
        """
        Initializes the Logger.

        Args:
            name (str): The name of the logger.
            log_file_path (str): The path to the log file.
            level (str): The logging level for the console stream handler (e.g., 'INFO', 'DEBUG').
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)  # Set logger to the lowest level; handlers control effective level.
        self.logger.propagate = False # Prevent duplicate logs in parent loggers, if any.

        # Use a more detailed formatter
        formatter = TaipeiFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S.%f%z' # Example date format, or use default ISO in formatTime
        )

        # File Handler - always logs at DEBUG level
        try:
            file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        except Exception as e:
            # Fallback to stderr if file handler fails
            sys.stderr.write(f"Failed to initialize file handler for logger: {e}\n")


        # Stream Handler (Console) - logs at the specified level (e.g., INFO or DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        # Convert string level to logging level constant, default to INFO
        console_level = self._log_level_map.get(level.lower(), logging.INFO)
        stream_handler.setLevel(console_level)
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

        # Initial log to confirm setup, if not too verbose for all instances
        # self.debug(f"Logger '{name}' initialized. File: '{log_file_path}', Console Level: {level.upper()}")

    def log(self, message, level="info", **kwargs):
        """
        Logs a message with the specified level.

        Args:
            message (str): The message to log.
            level (str): The logging level (e.g., 'info', 'debug', 'warning', 'error', 'critical', 'step', 'substep', 'success').
            **kwargs: Additional arguments to pass to the underlying logging method (e.g., exc_info).
        """
        log_level_constant = self._log_level_map.get(level.lower(), logging.INFO)

        # For custom levels like 'step', 'substep', 'success', we might want to add a prefix or specific handling.
        # For now, they map to standard levels. If specific formatting is needed, this is where it would go.
        if level.lower() in ["step", "success"] and not message.startswith("==="):
             message = f"=== {message} ==="

        self.logger.log(log_level_constant, message, **kwargs)

    # Convenience methods (optional, but good practice)
    def debug(self, message, **kwargs):
        self.log(message, level="debug", **kwargs)

    def info(self, message, **kwargs):
        self.log(message, level="info", **kwargs)

    def warning(self, message, **kwargs):
        self.log(message, level="warning", **kwargs)

    def error(self, message, **kwargs):
        self.log(message, level="error", **kwargs)

    def critical(self, message, **kwargs):
        self.log(message, level="critical", **kwargs)

    # Methods for custom levels used in PipelineOrchestrator
    def step(self, message, **kwargs):
        self.log(message, level="step", **kwargs)

    def substep(self, message, **kwargs):
        self.log(message, level="substep", **kwargs)

    def success(self, message, **kwargs):
        self.log(message, level="success", **kwargs)

def setup_logger(log_path_dir: str, log_filename: str, debug_mode: bool):
    """
    Sets up and returns a logger instance configured by the Logger class.

    Args:
        log_path_dir (str): The directory where the log file will be stored.
        log_filename (str): The name of the log file.
        debug_mode (bool): If True, sets console logging level to DEBUG, otherwise INFO.

    Returns:
        logging.Logger: The configured logger instance.
    """
    import os # Ensure os is imported here if not globally available in this exact scope (it is in this file)

    # Determine logging level for the console
    level_str = "DEBUG" if debug_mode else "INFO"

    # Construct the full log file path
    # The Logger class itself doesn't create the directory, so it's assumed log_path_dir exists
    # or will be created by the orchestrator.
    full_log_path = os.path.join(log_path_dir, log_filename)

    # Instantiate the custom Logger class
    # The 'name' of the logger can be derived from log_filename or be a fixed name like 'data_pipeline'
    # Using log_filename without extension might be cleaner for logger name.
    logger_name = os.path.splitext(log_filename)[0]

    custom_logger_instance = Logger(name=logger_name, log_file_path=full_log_path, level=level_str)

    # The orchestrator expects to use the returned logger directly with .info(), .debug() etc.
    # The Logger class has self.logger which is the actual logging.Logger instance.
    return custom_logger_instance.logger


if __name__ == '__main__':
    # Example Usage:
    # Create a dummy log file path for testing
    import os
    test_log_dir = "temp_test_logs"
    os.makedirs(test_log_dir, exist_ok=True)
    test_log_file = os.path.join(test_log_dir, "test_logger.log")

    print(f"--- Test Case 1: Default Logger (INFO level for console) ---")
    logger_info = Logger(name="TestInfoLogger", log_file_path=test_log_file, level="INFO")
    logger_info.debug("This is a DEBUG message. (Should be in file only)")
    logger_info.info("This is an INFO message. (Should be in file and console)")
    logger_info.warning("This is a WARNING message. (Should be in file and console)")
    logger_info.error("This is an ERROR message. (Should be in file and console)")
    logger_info.critical("This is a CRITICAL message. (Should be in file and console)")
    logger_info.step("This is a STEP message. (INFO level, console)")
    logger_info.substep("This is a SUBSTEP message. (DEBUG level, file only)")
    logger_info.success("This is a SUCCESS message. (INFO level, console)")
    print(f"Log file for Test Case 1: {os.path.abspath(test_log_file)}")
    with open(test_log_file, 'r', encoding='utf-8') as f:
        print("--- Content of log file (Test Case 1) ---")
        print(f.read())
    print("---------------------------------------------\n")

    # Clean up the log file for the next test or clear content
    open(test_log_file, 'w').close()

    print(f"--- Test Case 2: Debug Logger (DEBUG level for console) ---")
    logger_debug = Logger(name="TestDebugLogger", log_file_path=test_log_file, level="DEBUG")
    logger_debug.debug("This is a DEBUG message. (Should be in file and console)")
    logger_debug.info("This is an INFO message. (Should be in file and console)")
    logger_debug.step("This is a STEP message. (INFO level, console)")
    logger_debug.substep("This is a SUBSTEP message. (DEBUG level, console)")
    logger_debug.success("This is a SUCCESS message. (INFO level, console)")
    print(f"Log file for Test Case 2: {os.path.abspath(test_log_file)}")
    with open(test_log_file, 'r', encoding='utf-8') as f:
        print("--- Content of log file (Test Case 2) ---")
        print(f.read())
    print("---------------------------------------------\n")

    print(f"Note: pytz library is a dependency for this logger. Ensure it is added to pyproject.toml.")
    print(f"Example: poetry add pytz")
    # Consider removing the temp_test_logs directory after testing if desired.
    # shutil.rmtree(test_log_dir)
    # print(f"Cleaned up {test_log_dir}")
