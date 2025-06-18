import threading
import psutil
import time
from .logger import Logger


class HardwareMonitor:
    def __init__(self, logger, interval=2):
        self.logger = logger
        self._interval = interval
        self._thread = None
        self._stop_event = threading.Event()

    def _monitor(self):
        """Internal method to monitor hardware resources."""
        while not self._stop_event.is_set():
            try:
                cpu = psutil.cpu_percent()
                mem = psutil.virtual_memory()
                disk = psutil.disk_usage("/")
                # Ensure the status message is printed on a single line and then cleared
                # The \r (carriage return) moves the cursor to the beginning of the line
                status = f"⏱️  CPU: {cpu:5.1f}% | RAM: {mem.percent:5.1f}% ({mem.used/1024**3:.2f}/{mem.total/1024**3:.2f} GB) | Disk: {disk.percent:5.1f}% \r"
                print(status, end="")
                # Flush stdout to ensure it's printed immediately, especially if run in some environments
                # sys.stdout.flush() # Requires import sys, consider if needed based on logger behavior

                # Wait for the specified interval or until the stop event is set
                self._stop_event.wait(timeout=self._interval)
            except Exception as e:
                # Optionally log the exception if the logger is available and it's desirable
                # self.logger.log(f"Error in monitor thread: {e}", "error")
                pass  # Silently continue or handle error

    def start(self):
        """Starts the hardware monitoring thread."""
        if (
            not self._thread or not self._thread.is_alive()
        ):  # Check if thread is alive too
            self.logger.log("啟動高頻率即時硬體監控...", "info")
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._monitor, daemon=True)
            self._thread.start()
        else:
            self.logger.log("硬體監控已經在運行中。", "info")

    def stop(self):
        """Stops the hardware monitoring thread."""
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join(
                timeout=self._interval + 0.5
            )  # Give a bit more time for thread to join
            # Clear the line where status was printed
            # Ensure it's long enough to cover the previous status message
            print(
                "\r" + " " * 100 + "\r", end=""
            )  # Carriage return, spaces, carriage return again
            # sys.stdout.flush() # if using sys.stdout.flush() in _monitor
            self.logger.log("停止高頻率即時硬體監控...", "info")
            self._thread = None
        else:
            self.logger.log("硬體監控尚未啟動或已停止。", "info")


class HardwareMonitor:
    """硬體資源監控器。

    這個類別提供了一個背景執行緒來定期監控 CPU、記憶體和磁碟使用情況，
    並將狀態輸出到控制台。它需要一個 Logger 物件來記錄啟動和停止訊息。

    :ivar logger: 用於記錄日誌的 Logger 物件執行個體。
    :vartype logger: data_pipeline_v15.utils.logger.Logger
    :ivar _interval: 監控狀態的更新間隔（秒）。
    :vartype _interval: int
    :ivar _thread: 背景監控執行緒的 Thread 物件。
    :vartype _thread: threading.Thread
    :ivar _stop_event: 用於通知監控執行緒停止的 Event 物件。
    :vartype _stop_event: threading.Event

    使用範例:
        >>> from data_pipeline_v15.utils.logger import Logger # 修正導入路徑
        >>> logger_instance = Logger('monitor_test.log')
        >>> monitor = HardwareMonitor(logger=logger_instance, interval=5)
        >>> monitor.start()
        >>> # ... 執行一些操作 ...
        >>> monitor.stop()
    """

    def __init__(self, logger, interval=2):
        """初始化 HardwareMonitor 物件。

        :param logger: 用於記錄啟動和停止訊息的 Logger 物件。
        :type logger: data_pipeline_v15.utils.logger.Logger
        :param interval: 監控資訊更新的間隔時間（秒），預設為 2 秒。
        :type interval: int, optional
        """
        self.logger = logger
        self._interval = interval
        self._thread = None
        self._stop_event = threading.Event()

    def _monitor(self):
        """內部監控迴圈，定期獲取並顯示硬體狀態。

        這個方法會在一個獨立的執行緒中執行，直到 _stop_event 被設定。
        它會捕捉所有預期的例外，以避免執行緒意外終止。
        """
        """Internal method to monitor hardware resources."""
        while not self._stop_event.is_set():
            try:
                cpu = psutil.cpu_percent()
                mem = psutil.virtual_memory()
                disk = psutil.disk_usage("/")
                # Ensure the status message is printed on a single line and then cleared
                # The \r (carriage return) moves the cursor to the beginning of the line
                status = f"⏱️  CPU: {cpu:5.1f}% | RAM: {mem.percent:5.1f}% ({mem.used/1024**3:.2f}/{mem.total/1024**3:.2f} GB) | Disk: {disk.percent:5.1f}% \r"
                print(status, end="")
                # Flush stdout to ensure it's printed immediately, especially if run in some environments
                # sys.stdout.flush() # Requires import sys, consider if needed based on logger behavior

                # Wait for the specified interval or until the stop event is set
                self._stop_event.wait(timeout=self._interval)
            except Exception as e:
                # Optionally log the exception if the logger is available and it's desirable
                # self.logger.log(f"Error in monitor thread: {e}", "error")
                pass  # Silently continue or handle error

    def start(self):
        """啟動背景硬體監控執行緒。

        如果監控執行緒尚未啟動或已停止，此方法會初始化並啟動一個新的 daemon 執行緒
        來執行 _monitor 方法。會使用 Logger 記錄啟動訊息。
        """
        """Starts the hardware monitoring thread."""
        if (
            not self._thread or not self._thread.is_alive()
        ):  # Check if thread is alive too
            self.logger.log("啟動高頻率即時硬體監控...", "info")
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._monitor, daemon=True)
            self._thread.start()
        else:
            self.logger.log("硬體監控已經在運行中。", "info")

    def stop(self):
        """停止背景硬體監控執行緒。

        如果監控執行緒正在執行，此方法會設定停止事件，並等待執行緒結束。
        執行緒結束後，會清除控制台的最後一行狀態輸出，並使用 Logger 記錄停止訊息。
        """
        """Stops the hardware monitoring thread."""
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join(
                timeout=self._interval + 0.5
            )  # Give a bit more time for thread to join
            # Clear the line where status was printed
            # Ensure it's long enough to cover the previous status message
            print(
                "\r" + " " * 100 + "\r", end=""
            )  # Carriage return, spaces, carriage return again
            # sys.stdout.flush() # if using sys.stdout.flush() in _monitor
            self.logger.log("停止高頻率即時硬體監控...", "info")
            self._thread = None
        else:
            self.logger.log("硬體監控尚未啟動或已停止。", "info")
