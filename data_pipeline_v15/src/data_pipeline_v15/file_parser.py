# -*- coding: utf-8 -*-

import os
import zipfile
import hashlib
from io import BytesIO
import pandas as pd

# Consider adding 'typing' for type hints if used more formally later, e.g. Any, Dict, List


def worker_process_file(file_path: str, staging_dir: str, schemas_config: dict) -> dict:
    """處理單一來源檔案（CSV 或 ZIP），將其解析並轉換為 Parquet 格式。

    此函式作為檔案處理的入口點。如果檔案是 ZIP 壓縮檔，它會遍歷 ZIP 中的所有 CSV 檔案，
    並對每個 CSV 檔案呼叫 `` `_parse_single_csv` `` 進行處理。如果是單獨的 CSV 檔案，
    則直接呼叫 `` `_parse_single_csv` ``。

    :param file_path: 來源檔案的完整路徑。
    :type file_path: str
    :param staging_dir: 用於儲存轉換後 Parquet 檔案的暫存目錄路徑。
    :type staging_dir: str
    :param schemas_config: 包含綱要定義的字典。鍵為綱要名稱，值為綱要詳細設定。
    :type schemas_config: dict
    :return: 一個包含處理結果的字典。結構如下：
             - 若為 ZIP 檔: `{"status": "group_result", "results": list_of_individual_results, "file": original_filename}`
             - 若為單一 CSV 或 ZIP 中的 CSV: `{"status": "success"|"error"|"skipped", "file": display_name, ...}` (詳見 `` `_parse_single_csv` ``)
             常見 status 值:
               - `skipped`: 因檔案類型不支援而被跳過。`reason` 鍵會說明原因。
               - `error`: 處理過程中發生錯誤。`reason` 鍵會說明錯誤。
               - `group_result`: 表示輸入為 ZIP 檔，`results` 鍵包含每個內部 CSV 的處理結果列表。
    :rtype: dict
    """
    original_filename = os.path.basename(file_path)
    if not (
        original_filename.lower().endswith(".csv")
        or original_filename.lower().endswith(".zip")
    ):
        return {
            "status": "skipped",
            "file": original_filename,
            "reason": "不支援的檔案類型",
        }

    if original_filename.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(file_path, "r") as z:
                # Filter out macOS resource fork files and ensure proper CSVs
                csv_files = [
                    f
                    for f in z.namelist()
                    if f.lower().endswith(".csv") and not f.startswith("__MACOSX")
                ]
                if not csv_files:
                    return {
                        "status": "error",
                        "file": original_filename,
                        "reason": "ZIP 檔中未找到任何 CSV 檔案",
                    }

                all_results = []
                for csv_name in csv_files:
                    # Use a display name that includes the ZIP file and the CSV file name
                    # This helps in identifying the source of data/errors.
                    display_name_in_zip = f"{original_filename}/{csv_name}"
                    try:
                        with z.open(csv_name) as csv_file_in_zip:
                            # Pass BytesIO buffer to _parse_single_csv
                            csv_content = BytesIO(csv_file_in_zip.read())
                            result = _parse_single_csv(
                                csv_content,
                                staging_dir,
                                schemas_config,
                                display_name_in_zip,
                            )
                            all_results.append(result)
                    except Exception as e_zip_read:
                        all_results.append(
                            {
                                "status": "error",
                                "file": display_name_in_zip,
                                "reason": f"讀取 ZIP 中 CSV 時發生錯誤: {e_zip_read}",
                            }
                        )

                # Check if any of the results were successful for a more nuanced group status
                # For now, just returning a list of results.
                return {
                    "status": "group_result",
                    "results": all_results,
                    "file": original_filename,
                }
        except zipfile.BadZipFile:
            return {
                "status": "error",
                "file": original_filename,
                "reason": "損壞的 ZIP 檔案",
            }
        except Exception as e:
            # Log traceback here if logger is available
            return {
                "status": "error",
                "file": original_filename,
                "reason": f"處理 ZIP 時發生未知錯誤: {e}",
            }

    # Handle single CSV file
    return _parse_single_csv(file_path, staging_dir, schemas_config, original_filename)


def _parse_single_csv(
    file_or_buffer, staging_dir: str, schemas_config: dict, display_name: str
) -> dict:
    """解析單個 CSV 檔案（或記憶體中的緩衝區），進行欄位正規化、綱要匹配與補完，並儲存為 Parquet 格式。

    此函式是核心的 CSV 解析與轉換邏輯。它會：
    1. 嘗試使用多種編碼（utf-8, big5, ms950, cp950）讀取 CSV。
    2. 根據檔名關鍵字和 `` `schemas_config` `` 中的定義，匹配最適合的資料綱要。
    3. 根據選定的綱要，對欄位名稱進行正規化（轉小寫、移除空白、別名對映）。
    4. 使用 `` `reindex` `` 將 DataFrame 擴展至目標綱要的完整欄位結構，缺失值以 NaN 填充。
    5. 將處理後的 DataFrame 儲存到指定暫存目錄下的 Parquet 檔案，檔名基於 `` `display_name` `` 的雜湊值。

    :param file_or_buffer: CSV 檔案的路徑 (str) 或包含 CSV 數據的 BytesIO 緩衝區。
    :type file_or_buffer: str or io.BytesIO
    :param staging_dir: 用於儲存轉換後 Parquet 檔案的暫存目錄路徑。
    :type staging_dir: str
    :param schemas_config: 包含綱要定義的字典。
    :type schemas_config: dict
    :param display_name: 用於日誌記錄和 Parquet 檔名生成的檔案顯示名稱（例如 'original.zip/inner.csv' 或 'data.csv'）。
    :type display_name: str
    :return: 一個包含處理結果的字典，結構如下：
             `{"status": status_code, "file": display_name, "reason": optional_error_msg, "table": matched_schema_name, "count": num_rows, "path": output_parquet_path}`
             Status codes:
               - `success`: 成功處理。`table` 為匹配的綱要名，`count` 為處理的行數，`path` 為輸出的 Parquet 路徑。
               - `error`: 處理失敗。`reason` 包含錯誤描述。
    :rtype: dict
    """
    try:
        encodings_to_try = [
            "utf-8",
            "big5",
            "ms950",
            "cp950",
        ]  # Added cp950 as it's common for Taiwan Big5
        df = None
        error_messages = []

        for enc in encodings_to_try:
            try:
                if isinstance(file_or_buffer, BytesIO):
                    file_or_buffer.seek(
                        0
                    )  # Reset buffer position for each read attempt

                # Using chunksize to potentially handle very large files better, though not fully implemented here
                # For now, read_csv without chunking
                df = pd.read_csv(
                    file_or_buffer, low_memory=False, encoding=enc, on_bad_lines="skip"
                )

                if not df.empty:
                    break  # Successfully read and parsed
                else:  # File read but was empty
                    error_messages.append(f"使用 {enc} 編碼讀取後檔案為空")

            except UnicodeDecodeError:
                error_messages.append(f"使用 {enc} 編碼解碼失敗")
                continue  # Try next encoding
            except (
                pd.errors.EmptyDataError
            ):  # Pandas specific error for empty file/buffer
                error_messages.append(f"使用 {enc} 編碼時檔案為空 (EmptyDataError)")
                continue
            except Exception as read_e:  # Catch other potential read errors
                error_messages.append(f"使用 {enc} 編碼讀取時發生非預期錯誤: {read_e}")
                continue

        if df is None or df.empty:
            return {
                "status": "error",
                "file": display_name,
                "reason": f"無法使用支援的編碼解碼，或檔案為空. Errors: {'; '.join(error_messages)}",
            }

        # Schema matching logic (prioritize more specific schemas)
        # This assumes schema_config is a dict where keys are schema names.
        matched_schema_name = None
        available_schema_names = list(
            schemas_config.keys()
        )  # e.g., ['weekly_report', 'default_daily'] - order might matter

        # Simple keyword matching in display_name. Could be made more sophisticated.
        for (
            name
        ) in available_schema_names:  # Ensure a defined order if priority is fixed
            schema_def = schemas_config.get(name, {})  # Default to empty dict
            keywords = schema_def.get("keywords", [])
            if any(k.lower() in display_name.lower() for k in keywords):
                matched_schema_name = name
                break

        if not matched_schema_name and "default_daily" in schemas_config:  # Fallback
            matched_schema_name = "default_daily"
        elif not matched_schema_name:  # No fallback and no match
            return {
                "status": "error",
                "file": display_name,
                "reason": "找不到任何匹配的 schema (無 default_daily 後備)",
            }

        schema = schemas_config.get(matched_schema_name)
        if not schema or not schema.get(
            "columns_map"
        ):  # Check if schema itself or its column_map is missing
            return {
                "status": "error",
                "file": display_name,
                "reason": f"Schema '{matched_schema_name}' 未定義或其 column_map 為空",
            }

        # Normalize DataFrame column names (strip spaces, lower case)
        df.rename(columns=lambda c: str(c).strip().lower(), inplace=True)

        # Create column mapping from schema aliases
        column_map = {
            alias.lower(): target_col_name
            for target_col_name, details in schema["columns_map"].items()
            for alias in details.get("aliases", [])
        }
        df.rename(columns=column_map, inplace=True)

        # Select and reorder columns according to the target schema
        target_columns = list(schema["columns_map"].keys())

        # Check if any of the target columns are now in the DataFrame after renaming
        # This is crucial to ensure the schema mapping was somewhat successful.
        if not any(col in df.columns for col in target_columns):
            return {
                "status": "error",
                "file": display_name,
                "reason": "欄位重命名後，檔案內容與所有已知綱要的目標欄位完全不符",
            }

        # Reindex to ensure all target columns are present, filling missing ones with NaN (which Parquet handles as null)
        df = df.reindex(columns=target_columns)

        # Create a unique hash for the output file based on the display name to avoid collisions
        # Using SHA256 for better uniqueness than SHA1
        output_hash = hashlib.sha256(display_name.encode("utf-8")).hexdigest()
        output_dir = os.path.join(staging_dir, matched_schema_name)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{output_hash}.parquet")

        df.to_parquet(
            output_path, engine="pyarrow", index=False
        )  # Added index=False, common for data interchange

        return {
            "status": "success",
            "file": display_name,
            "table": matched_schema_name,
            "count": len(df),
            "path": output_path,
        }

    except Exception as e:
        # import traceback # For detailed error logging
        # error_details = traceback.format_exc()
        return {
            "status": "error",
            "file": display_name,
            "reason": f"解析 CSV 時發生未知錯誤: {e}",
        }  # Consider adding error_details
