# -*- coding: utf-8 -*-

import os
import zipfile
import hashlib
from io import BytesIO
import pandas as pd
from .core import constants # 修改：導入常數


def worker_process_file(file_path: str, staging_dir: str, schemas_config: dict) -> dict:
    """處理單一來源檔案（CSV 或 ZIP），將其解析並轉換為 Parquet 格式。
    (docstring 已省略)
    """
    original_filename = os.path.basename(file_path)
    if not (
        original_filename.lower().endswith(".csv")
        or original_filename.lower().endswith(".zip")
    ):
        return {
            constants.KEY_STATUS: constants.STATUS_SKIPPED,
            constants.KEY_FILE: original_filename,
            constants.KEY_REASON: "不支援的檔案類型",
        }

    if original_filename.lower().endswith(".zip"):
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
    return _parse_single_csv(file_path, staging_dir, schemas_config, original_filename)


def _parse_single_csv(
    file_or_buffer, staging_dir: str, schemas_config: dict, display_name: str
) -> dict:
    """解析單個 CSV 檔案（或記憶體中的緩衝區），進行欄位正規化、綱要匹配與補完，並儲存為 Parquet 格式。
    (docstring 已省略)
    """
    try:
        encodings_to_try = [
            "utf-8",
            "big5",
            "ms950",
            "cp950",
        ]
        df = None
        error_messages = []
        for enc in encodings_to_try:
            try:
                if isinstance(file_or_buffer, BytesIO):
                    file_or_buffer.seek(0)
                df = pd.read_csv(
                    file_or_buffer, low_memory=False, encoding=enc, on_bad_lines="skip"
                )
                if not df.empty:
                    break
                else:
                    error_messages.append(f"使用 {enc} 編碼讀取後檔案為空")
            except UnicodeDecodeError:
                error_messages.append(f"使用 {enc} 編碼解碼失敗")
                continue
            except pd.errors.EmptyDataError:
                error_messages.append(f"使用 {enc} 編碼時檔案為空 (EmptyDataError)")
                continue
            except Exception as read_e:
                error_messages.append(f"使用 {enc} 編碼讀取時發生非預期錯誤: {read_e}")
                continue

        if df is None or df.empty:
            return {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: display_name,
                constants.KEY_REASON: f"無法使用支援的編碼解碼，或檔案為空. Errors: {'; '.join(error_messages)}",
            }

        matched_schema_name = None
        available_schema_names = list(schemas_config.keys())
        for name in available_schema_names:
            schema_def = schemas_config.get(name, {})
            keywords = schema_def.get("keywords", [])
            if any(k.lower() in display_name.lower() for k in keywords):
                matched_schema_name = name
                break
        if not matched_schema_name and "default_daily" in schemas_config:
            matched_schema_name = "default_daily"
        elif not matched_schema_name:
            return {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: display_name,
                constants.KEY_REASON: "找不到任何匹配的 schema (無 default_daily 後備)",
            }

        schema = schemas_config.get(matched_schema_name)
        if not schema or not schema.get("columns_map"):
            return {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: display_name,
                constants.KEY_REASON: f"Schema '{matched_schema_name}' 未定義或其 column_map 為空",
            }
        df.rename(columns=lambda c: str(c).strip().lower(), inplace=True)
        column_map = {
            alias.lower(): target_col_name
            for target_col_name, details in schema["columns_map"].items()
            for alias in details.get("aliases", [])
        }
        df.rename(columns=column_map, inplace=True)
        target_columns = list(schema["columns_map"].keys())
        if not any(col in df.columns for col in target_columns):
            return {
                constants.KEY_STATUS: constants.STATUS_ERROR,
                constants.KEY_FILE: display_name,
                constants.KEY_REASON: "欄位重命名後，檔案內容與所有已知綱要的目標欄位完全不符",
            }
        df = df.reindex(columns=target_columns)
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
        return {
            constants.KEY_STATUS: constants.STATUS_ERROR,
            constants.KEY_FILE: display_name,
            constants.KEY_REASON: f"解析 CSV 時發生未知錯誤: {e}",
        }
