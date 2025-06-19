import logging
import sys
from datetime import datetime
import pytz

# Custom Formatter for Taipei Timezone
class TaipeiFormatter(logging.Formatter):
    '''自訂日誌格式化程序，將時間戳轉換為亞洲/台北時區'''
    def formatTime(self, record, datefmt=None):
        tz = pytz.timezone('Asia/Taipei')
        # 使用 record.created (unix timestamp) 建立 aware datetime object
        ct = datetime.fromtimestamp(record.created, tz)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            # 預設時間格式，與 logging.Formatter 相似但包含毫秒
            t = ct.strftime('%Y-%m-%d %H:%M:%S')
            s = f"{t},{int(ct.microsecond / 1000):03d}" # 格式化毫秒
        return s

class Logger:
    """日誌記錄器模組，提供統一的日誌記錄接口。

    這個類別封裝了 Python 的 logging 模組，提供了一個簡便的方式來
    初始化日誌設定並記錄不同級別的訊息，並在控制台和檔案中輸出。

    使用範例:
        >>> logger = Logger(log_file_path='my_app.log')
        >>> logger.log("這是一條資訊訊息。", level="info")
        >>> logger.log("這是一個成功操作！", level="success")
    """

    def __init__(self, log_file_path, level='INFO'):
        """初始化 Logger 物件。

        這個方法會設定日誌的基本配置，包括日誌級別、格式、日期格式，
        以及日誌處理器 (同時輸出到檔案和控制台)。
        它會先清除所有現有的根日誌處理器，以避免重複記錄。

        :param log_file_path: 日誌檔案的完整路徑。
        :type log_file_path: str
        :param level: 控制台輸出的日誌級別，預設為 'INFO'。
        :type level: str, optional
        """
        # 1. 獲取並清理根 Logger
        logger = logging.getLogger()
        logger.handlers = []  # 清空所有已存在的 handlers
        logger.setLevel(logging.DEBUG)  # 將 logger 的基礎級別設定為 logging.DEBUG

        # 2. 配置「檔案處理器」(FileHandler)
        file_handler = logging.FileHandler(log_file_path, mode="w", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)  # FileHandler 的級別硬式編碼為 logging.DEBUG
        file_formatter = TaipeiFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # 3. 配置「控制台處理器」(StreamHandler)
        console_handler = logging.StreamHandler(sys.stdout)
        # 根據傳入的 level 參數動態設定 StreamHandler 的級別
        if level.upper() == 'DEBUG':
            console_handler.setLevel(logging.DEBUG)
        elif level.upper() == 'INFO':
            console_handler.setLevel(logging.INFO)
        else:
            # 預設或錯誤處理，這裡預設為 INFO
            console_handler.setLevel(logging.INFO)
        console_formatter = TaipeiFormatter('%(asctime)s - %(levelname)-8s - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # 4. 儲存 Logger 實例
        self.logger = logger

    def log(self, message, level="info"):
        """記錄一條日誌訊息。

        根據指定的級別，在訊息前添加對應的符號，並使用 logging 模組記錄。
        'step' 級別的訊息會有特殊的格式，上下帶有分隔線。

        :param message: 要記錄的日誌訊息內容。
        :type message: str
        :param level: 日誌級別，預設為 "info"。
                      可選值: "success", "warning", "error", "step", "substep", "info"。
        :type level: str, optional
        """
        symbol_map = {
            "success": "✅",
            "warning": "⚠️",
            "error": "❌",
            "step": "🚚",
            "substep": "📊",
            "info": "⚪",
        }
        symbol = symbol_map.get(level, "⚪")

        if level == "step":
            self.logger.info(f"\n{'='*80}\n{symbol} {message}\n{'='*80}")
        elif level == "error":
            self.logger.error(f"{symbol} {message}")
        elif level == "warning":
            self.logger.warning(f"{symbol} {message}")
        elif level == "debug": # Assuming you might want a debug level in your map
            self.logger.debug(f"{symbol} {message}")
        else: # Default to info for 'info', 'success', 'substep' or unknown
            self.logger.info(f"{symbol} {message}")
