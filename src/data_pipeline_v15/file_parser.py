# -*- coding: utf-8 -*-

import os
import zipfile
import hashlib
from io import BytesIO
import pandas as pd
from .core import constants # 修改：導入常數


class FileParser:
    def __init__(self, manifest_manager, logger):
        self.manifest_manager = manifest_manager
        self.logger = logger

    def parse_file(self, file_path: str, staging_dir: str, schemas_config: dict) -> dict:
        """處理單一來源檔案（CSV 或 ZIP），將其解析並轉換為 Parquet 格式。
        (docstring 已省略)
        """
        original_filename = os.path.basename(file_path)

        if zipfile.is_zipfile(file_path):
            try:
                with zipfile.ZipFile(file_path, "r") as z:
                    csv_files = [
                        f
                        for f in z.namelist()
                        if f.lower().endswith(".csv") and not f.startswith("__MACOSX")
                    ]
                    if not csv_files:
                        return {
                            constants.KEY_STATUS: constants.STATUS_ERROR,
                            constants.KEY_FILE: original_filename,
                            constants.KEY_REASON: "ZIP 檔中未找到任何 CSV 檔案",
                        }
                    all_results = []
                    for csv_name in csv_files:
                        display_name_in_zip = f"{original_filename}/{csv_name}"
                        try:
                            with z.open(csv_name) as csv_file_in_zip:
                                csv_content = BytesIO(csv_file_in_zip.read())
                                result = self._parse_content( # Renamed
                                    csv_content,
                                    staging_dir,
                                    schemas_config,
                                    display_name_in_zip,
                                )
                                all_results.append(result)
                        except Exception as e_zip_read:
                            all_results.append(
                                {
                                    constants.KEY_STATUS: constants.STATUS_ERROR,
                                    constants.KEY_FILE: display_name_in_zip,
                                    constants.KEY_REASON: f"讀取 ZIP 中 CSV 時發生錯誤: {e_zip_read}",
                                }
                            )
                    return {
                        constants.KEY_STATUS: constants.STATUS_GROUP_RESULT,
                        constants.KEY_RESULTS: all_results,
                        constants.KEY_FILE: original_filename,
                    }
            except zipfile.BadZipFile:
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: original_filename,
                    constants.KEY_REASON: "損壞的 ZIP 檔案",
                }
            except Exception as e:
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: original_filename,
                    constants.KEY_REASON: f"處理 ZIP 時發生未知錯誤: {e}",
                }
        else: # Not a ZIP file, assume CSV or other parsable content
            return self._parse_content(file_path, staging_dir, schemas_config, original_filename) # Renamed

    def _parse_content( # Renamed
        self, file_input, staging_dir: str, schemas_config: dict, display_name: str # Renamed file_or_buffer to file_input
    ) -> dict:
        """解析單個檔案內容（來自路徑或記憶體緩衝區），進行內容嗅探、綱要匹配、欄位正規化，並儲存為 Parquet 格式。
        (docstring 已省略)
        """
        try:
            encodings_to_try = ["utf-8", "big5", "ms950", "cp950"]
            head_content_bytes = b""
            matched_schema_name = None
            detected_encoding = None

            if isinstance(file_input, str): # File path
                try:
                    with open(file_input, "rb") as f:
                        head_content_bytes = f.read(4096) # Read first 4KB
                except Exception as e:
                    return {
                        constants.KEY_STATUS: constants.STATUS_ERROR,
                        constants.KEY_FILE: display_name,
                        constants.KEY_REASON: f"讀取檔案失敗: {e}",
                    }
            elif isinstance(file_input, BytesIO):
                current_pos = file_input.tell()
                head_content_bytes = file_input.read(4096)
                file_input.seek(current_pos) # Reset pointer to original position for pd.read_csv
            else:
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: "不支援的輸入類型 file_input",
                }

            if not head_content_bytes:
                 return {
                    constants.KEY_STATUS: constants.STATUS_SKIPPED,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: "檔案為空或無法讀取頭部內容。",
                }

            # Content-based schema detection
            for schema_name, schema_def in schemas_config.items():
                keywords = schema_def.get("keywords", [])
                if not keywords:
                    continue
                for enc in encodings_to_try:
                    try:
                        head_content_str = head_content_bytes.decode(enc)
                        if any(k.lower() in head_content_str.lower() for k in keywords):
                            matched_schema_name = schema_name
                            detected_encoding = enc
                            self.logger.info(f"檔案 '{display_name}' 透過關鍵字在內容中匹配到 schema: '{matched_schema_name}' 使用編碼 '{detected_encoding}'")
                            break
                    except UnicodeDecodeError:
                        continue # Try next encoding
                if matched_schema_name:
                    break

            if not matched_schema_name and "default_daily" in schemas_config: # Fallback to default if specified
                self.logger.warning(f"檔案 '{display_name}' 未透過內容關鍵字匹配到特定 schema，嘗試使用 'default_daily'。")
                matched_schema_name = "default_daily"
                # detected_encoding remains None, pd.read_csv will try its list

            if not matched_schema_name:
                self.logger.warning(f"檔案 '{display_name}' 無法根據內容關鍵字匹配到任何 schema，且無 default_daily 後備。")
                return {
                    constants.KEY_STATUS: constants.STATUS_SKIPPED,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: "無法根據內容關鍵字識別檔案類型或匹配任何已知的 schema。",
                }

            # Proceed with parsing using pandas
            df = None
            error_messages = []

            # Use detected_encoding first if available, otherwise try all
            pandas_encodings_to_try = [detected_encoding] + encodings_to_try if detected_encoding else encodings_to_try

            for enc in pandas_encodings_to_try:
                if enc is None: continue # Skip if detected_encoding was None and is first in list
                try:
                    if isinstance(file_input, BytesIO):
                        file_input.seek(0) # Reset for each read attempt
                    # Pass file_input (path or BytesIO) directly to pandas
                    df = pd.read_csv(file_input, low_memory=False, encoding=enc, on_bad_lines="skip")
                    if not df.empty:
                        self.logger.info(f"成功使用編碼 '{enc}' 讀取檔案 '{display_name}'。")
                        break
                    else:
                        error_messages.append(f"使用 {enc} 編碼讀取後檔案為空")
                except UnicodeDecodeError:
                    error_messages.append(f"使用 {enc} 編碼解碼失敗")
                except pd.errors.EmptyDataError:
                    error_messages.append(f"使用 {enc} 編碼時檔案為空 (EmptyDataError)")
                except Exception as read_e:
                    error_messages.append(f"使用 {enc} 編碼讀取時發生非預期錯誤: {read_e}")

            if df is None or df.empty:
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: f"無法使用支援的編碼成功讀取檔案內容，或檔案為空. Errors: {'; '.join(error_messages)}",
                }

            schema = schemas_config.get(matched_schema_name)
            if not schema or not schema.get("columns_map"):
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: f"Schema '{matched_schema_name}' 未定義或其 column_map 為空", # This error message is fine
                }
            df.rename(columns=lambda c: str(c).strip().lower(), inplace=True)
            column_map = {
                alias.lower(): target_col_name
                for target_col_name, details in schema["columns_map"].items()
                for alias in details.get("aliases", [])
            }
            df.rename(columns=column_map, inplace=True)
            target_columns = list(schema["columns_map"].keys())

            # Check if any of the target columns are present after renaming
            # This is important to ensure the schema matching was meaningful
            if not any(col in df.columns for col in target_columns):
                # Log current columns for debugging
                self.logger.warning(f"檔案 '{display_name}' 匹配到 schema '{matched_schema_name}'，但重命名後找不到任何目標欄位。可用欄位: {df.columns.tolist()}")
                # Try to find at least one common column based on original names (before rename) to be more robust
                # This part can be enhanced, but for now, we stick to target_columns
                return {
                    constants.KEY_STATUS: constants.STATUS_ERROR,
                    constants.KEY_FILE: display_name,
                    constants.KEY_REASON: f"欄位重命名後，檔案 '{display_name}' 內容與 schema '{matched_schema_name}' 的目標欄位不符。檢查 schema 關鍵字或檔案內容。",
                }

            df = df.reindex(columns=target_columns) # Reindex to ensure consistent column order and add missing columns as NaN

            output_hash = hashlib.sha256(display_name.encode("utf-8")).hexdigest()
            output_dir = os.path.join(staging_dir, matched_schema_name)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{output_hash}.parquet")
            df.to_parquet(output_path, engine="pyarrow", index=False)
            return {
                constants.KEY_STATUS: constants.STATUS_SUCCESS,
                constants.KEY_FILE: display_name,
                constants.KEY_TABLE: matched_schema_name,
                constants.KEY_COUNT: len(df),
                constants.KEY_PATH: output_path,
            }
        except Exception as e:
            self.logger.error(f"處理檔案 '{display_name}' 時發生未知錯誤: {e}", exc_info=True)
            return {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: display_name,
                constants.KEY_REASON: f"解析檔案時發生未知錯誤: {e}",
            }
