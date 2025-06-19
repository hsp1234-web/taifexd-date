import hashlib
import json
import os
from typing import TYPE_CHECKING, Set, Union, Iterable # 為類型提示而添加

if TYPE_CHECKING:
    from .utils.logger import Logger # 用於 Logger 的類型提示


class ManifestManager:
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
    :ivar logger: 用於記錄日誌的 Logger 物件執行個體。
    :vartype logger: data_pipeline_v15.utils.logger.Logger
    :ivar processed_hashes: 一個包含所有已處理檔案雜湊值的集合。
    :vartype processed_hashes: set[str]
    """

    def __init__(self, manifest_path: str, logger: 'Logger'):
        """初始化 FileManifest 物件。

        在初始化時，會嘗試從指定的 `manifest_path` 載入已處理的雜湊值。

        :param manifest_path: 清單檔案的路徑。例如 'processed_files.json'。
        :type manifest_path: str
        :param logger: 用於記錄日誌訊息的 Logger 物件執行個體。
        :type logger: data_pipeline_v15.utils.logger.Logger
        """
        self.path = manifest_path
        self.logger = logger # 儲存 logger 物件
        self.processed_hashes: Set[str] = self._load()

    def load_or_create_manifest(self) -> None:
        """
        Ensures the manifest is loaded. Since loading occurs at initialization,
        this method can be a no-op or explicitly call _load if needed for re-load.
        For now, it's a no-op as __init__ handles initial load.
        """
        # self.processed_hashes = self._load() # Optionally allow re-loading
        pass

    def update_manifest(self, filename: str, status: str, message: str) -> None:
        """
        Updates the manifest based on the processing status of a file.
        If successful, the file's hash is added to the processed list.
        Logs the outcome.

        Args:
            filename (str): The name of the file being processed. Note: this is the basename.
                            The actual path for hashing might need context if not unique.
                            For now, we assume it's just recorded as is or a placeholder.
            status (str): The status of the processing (e.g., "SUCCESS", "ERROR").
            message (str): A message describing the outcome.
        """
        # In a real scenario, we'd get a unique identifier for the file,
        # typically a hash of its content or full path.
        # For the purpose of this method matching the orchestrator's call signature,
        # we'll use the filename as a proxy for the item to be recorded if successful.
        # The actual PipelineOrchestrator might need to pass a hash if that's the key.

        self.logger.log("info", f"Manifest update for '{filename}': Status - {status}, Message - {message}")

        # Assuming constants.STATUS_SUCCESS is available or comparing with string
        # from .core import constants # Would be needed if using constants.STATUS_SUCCESS
        if status == "SUCCESS": # Replace "SUCCESS" with actual constant if available
            # If filename itself is not the hash, this logic needs adjustment.
            # The current add_processed_hashes expects a hash or list of hashes.
            # This is a simplification to match the orchestrator's current call.
            # A more robust solution would involve hashing `filename` if it's a path,
            # or the orchestrator providing the hash.
            # For now, let's assume filename can be added if it's a unique ID.
            # However, add_processed_hashes expects a hash. This highlights a design mismatch.
            # Let's log and *not* add the raw filename if it's not a hash.
            # A placeholder for what should happen:
            file_identifier = self.get_file_hash(filename) # This would fail if filename is not a path
            if file_identifier: # Or if status indicates success and an identifier exists
                 self.add_processed_hashes(file_identifier)
                 self.logger.log("debug", f"File '{filename}' (hash: {file_identifier}) marked as processed.")
            elif status == "SUCCESS": # If it was success but we couldn't get an identifier
                 self.logger.log("warning", f"File '{filename}' reported success, but no hash obtained to update manifest. Manifest not updated with this item.")
        # No specific action for error/other statuses other than logging, already done.
        # The manifest primarily tracks *successfully* processed items by hash.

    def _load(self) -> Set[str]:
        """從指定的路徑載入已處理的檔案雜湊值清單。

        如果檔案不存在，則返回空集合。
        如果在讀取或解析現有檔案時發生任何錯誤，則會記錄錯誤並回傳一個空集合。

        :meta private:
        :return: 一個包含已處理檔案雜湊值的集合。
        :rtype: set[str]
        """
        if not os.path.exists(self.path):
            # 檔案不存在是預期情況（例如首次執行），返回空集合。
            # self.logger.log(f"Manifest 檔案 '{self.path}' 不存在，將開始一個新的清單。", level="info") # 可選日誌
            return set()

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # 從 'hashes' 鍵獲取列表，如果鍵不存在或其值不是列表（不太可能但防禦性），則data.get的結果可能是None或非列表
                # set() 構造函數可以處理可迭代對象，包括空列表。如果data.get返回None，set(None)會拋錯。
                # 因此，確保data.get的後備值是空列表[]
                hashes_list = data.get("hashes", [])
                if not isinstance(hashes_list, list):
                    self.logger.log(f"Manifest 檔案 '{self.path}' 中的 'hashes' 鍵並非一個列表，已將其視為空清單。", level="warning")
                    return set()
                return set(hashes_list)
        except Exception as e: # 捕捉讀取或解析現有檔案時的任何錯誤
            self.logger.log(f"載入 Manifest 檔案 '{self.path}' 時發生錯誤: {e}", level="error")
            return set() # 在發生錯誤時，保證返回空集合

    def _save(self) -> None:
        """將目前已處理的檔案雜湊值集合儲存到清單檔案中。

        雜湊值會以 JSON 格式儲存，包含一個 'hashes' 鍵，其值為雜湊值列表。
        儲存前會確保目標目錄存在。為了方便閱讀和版本控制，
        JSON 檔案會以縮排格式儲存，並且雜湊值列表會先排序。
        如果儲存過程中發生任何錯誤，錯誤將被記錄。

        :meta private:
        """
        try:
            manifest_dir = os.path.dirname(self.path)
            if not manifest_dir: # 處理 self.path 僅為檔名的情況
                manifest_dir = "."
            os.makedirs(manifest_dir, exist_ok=True)

            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(
                    {"hashes": sorted(list(self.processed_hashes))},
                    f,
                    indent=2,
                    ensure_ascii=False,
                )
        except Exception as e:
            self.logger.log(f"儲存 Manifest 檔案 '{self.path}' 時發生錯誤: {e}", level="error")
            # 錯誤已被記錄。依照作業描述，方法正常結束。

    @staticmethod
    def get_file_hash(file_path: str) -> Union[str, None]:
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
        except IOError as e:  # 處理讀取時其他潛在的 IOError
            # print(f"IOError while hashing file {file_path}: {e}")
            # 如果傳入了 logger，可以在此處使用，或引發錯誤
            return None

    def has_been_processed(self, file_hash: str) -> bool:
        """檢查指定的檔案雜湊值是否已經被處理過。

        :param file_hash: 要檢查的檔案雜湊值。
        :type file_hash: str
        :return: 如果雜湊值存在於已處理清單中，則回傳 True，否則回傳 False。
        :rtype: bool
        """
        return file_hash in self.processed_hashes

    def add_processed_hashes(self, hashes_to_add: Union[str, Iterable[str]]) -> None:
        """將一組新的雜湊值添加到已處理清單中並儲存。

        這個方法會更新 self.processed_hashes 集合，然後呼叫 _save() 方法
        將更新後的集合持久化到檔案中。

        :param hashes_to_add: 一個包含要添加的檔案雜湊值的字串，或可迭代物件 (例如列表或集合)。
        :type hashes_to_add: str or collections.abc.Iterable[str]
        """
        # 確保 hashes_to_add 是可迭代的，即使它只是一個雜湊字串
        if isinstance(hashes_to_add, str):
            self.processed_hashes.add(hashes_to_add)
        else:  # 假設它是一個可迭代物件 (例如列表、集合)
            self.processed_hashes.update(hashes_to_add)
        self._save()
