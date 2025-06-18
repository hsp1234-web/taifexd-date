import hashlib
import json
import os


class FileManifest:
    """檔案處理清單管理器。

    這個類別用於追蹤哪些檔案已經被處理過。它透過維護一個包含
    已處理檔案雜湊值 (hashes) 的 JSON 清單檔案來實現此功能。
    這樣可以避免重複處理相同的檔案，除非明確指示要重新處理。

    主要功能包括：
    - 從 JSON 檔案載入已處理的檔案雜湊值集合。
    - 將新的已處理檔案雜湊值儲存回 JSON 檔案。
    - 計算檔案的 SHA256 雜湊值。
    - 檢查特定檔案（根據其雜湊值）是否已經被處理過。

    :ivar path: 清單檔案 (manifest file) 的路徑。
    :vartype path: str
    :ivar processed_hashes: 一個包含所有已處理檔案雜湊值的集合。
    :vartype processed_hashes: set[str]
    """

    def __init__(self, manifest_path):
        """初始化 FileManifest 物件。

        在初始化時，會嘗試從指定的 `manifest_path` 載入已處理的雜<x_bin_5>。

        :param manifest_path: 清單檔案的路徑。例如 'processed_files.json'。
        :type manifest_path: str
        """
        self.path = manifest_path
        self.processed_hashes = self._load()

    def _load(self):
        """從指定的路徑載入已處理的檔案雜湊值清單。

        如果檔案不存在，或檔案內容不是有效的 JSON，或 JSON 中沒有 'hashes' 鍵，
        則會回傳一個空集合。

        :meta private:
        :return: 一個包含已處理檔案雜湊值的集合。
        :rtype: set[str]
        """
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return set(data.get("hashes", []))
        except (json.JSONDecodeError, IOError) as e:
            # Optionally, log this error if a logger is available/passed
            # print(f"Error loading manifest {self.path}: {e}")
            pass
        return set()

    def _save(self):
        """將目前已處理的檔案雜湊值集合儲存到清單檔案中。

        雜湊值會以 JSON 格式儲存，包含一個 'hashes' 鍵，其值為雜湊值列表。
        儲存前會確保目標目錄存在。為了方便閱讀和版本控制，
        JSON 檔案會以縮排格式儲存，並且雜湊值列表會先排序。

        :meta private:
        """
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(
                    {"hashes": sorted(list(self.processed_hashes))},
                    f,
                    indent=2,
                    ensure_ascii=False,
                )  # Added sorted for consistency
        except IOError as e:
            # Optionally, log this error
            # print(f"Error saving manifest {self.path}: {e}")
            pass

    @staticmethod
    def get_file_hash(file_path):
        """計算給定檔案路徑的 SHA256 雜湊值。

        以二進位模式讀取檔案，分塊計算雜湊值以處理大檔案。

        :param file_path: 要計算雜湊值的檔案的路徑。
        :type file_path: str
        :return: 檔案的 SHA256 雜湊值（十六進位字串），如果檔案未找到或讀取錯誤則回傳 None。
        :rtype: str or None
        """
        sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    sha256.update(chunk)
            return sha256.hexdigest()
        except FileNotFoundError:
            return None
        except IOError as e:  # Handle other potential IOErrors during read
            # print(f"IOError while hashing file {file_path}: {e}")
            return None

    def has_been_processed(self, file_hash):
        """檢查指定的檔案雜湊值是否已經被處理過。

        :param file_hash: 要檢查的檔案雜湊值。
        :type file_hash: str
        :return: 如果雜湊值存在於已處理清單中，則回傳 True，否則回傳 False。
        :rtype: bool
        """
        return file_hash in self.processed_hashes

    def add_processed_hashes(self, hashes_to_add):
        """將一組新的雜湊值添加到已處理清單中並儲存。

        這個方法會更新 self.processed_hashes 集合，然後呼叫 _save() 方法
        將更新後的集合持久化到檔案中。

        :param hashes_to_add: 一個包含要添加的檔案雜湊值的字串，或可迭代物件 (例如列表或集合)。
        :type hashes_to_add: str or collections.abc.Iterable[str]
        """
        # Ensure hashes_to_add is iterable, even if it's a single hash string
        if isinstance(hashes_to_add, str):
            self.processed_hashes.add(hashes_to_add)
        else:  # Assuming it's an iterable (list, set)
            self.processed_hashes.update(hashes_to_add)
        self._save()
