import logging
import sys


class Logger:
    """日誌記錄器模組，提供統一的日誌記錄接口。

    這個類別封裝了 Python 的 logging 模組，提供了一個簡便的方式來
    初始化日誌設定並記錄不同級別的訊息，並在控制台和檔案中輸出。

    使用範例:
        >>> logger = Logger(log_file_path='my_app.log')
        >>> logger.log("這是一條資訊訊息。", level="info")
        >>> logger.log("這是一個成功操作！", level="success")
    """

    def __init__(self, log_file_path):
        """初始化 Logger 物件。

        這個方法會設定日誌的基本配置，包括日誌級別、格式、日期格式，
        以及日誌處理器 (同時輸出到檔案和控制台)。
        它會先清除所有現有的根日誌處理器，以避免重複記錄。

        :param log_file_path: 日誌檔案的完整路徑。
        :type log_file_path: str
        """
        self.log_file_path = log_file_path
        # 清除已存在的處理器
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s.%(msecs)03d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[
                logging.FileHandler(self.log_file_path, mode="w", encoding="utf-8"),
                logging.StreamHandler(sys.stdout),
            ],
        )

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
            logging.info(f"\n{'='*80}\n{symbol} {message}\n{'='*80}")
        else:
            logging.info(f"{symbol} {message}")
