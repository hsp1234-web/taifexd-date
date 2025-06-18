# 數據整合平台 v15 (data_pipeline_v15)

本專案為「數據整合平台 v15」的核心處理引擎，使用 Python 進行開發，並透過 Poetry 進行依賴管理與封裝，使用 Pytest 進行單元測試。

## 專案設定

### 必要條件

*   Python (建議版本 3.8 或更高)
*   Poetry (請參考 [Poetry 官方文件](https://python-poetry.org/docs/#installation)進行安裝)

### 安裝步驟

1.  **複製專案庫 (Clone Repository)**：
    ```bash
    git clone <repository-url>
    cd data_pipeline_v15
    ```

2.  **安裝專案依賴**：
    使用 Poetry 安裝所有必要的依賴項。這會建立一個虛擬環境 (virtual environment) 並安裝 `pyproject.toml` 中定義的套件。
    ```bash
    poetry install
    ```

## 執行測試

本專案使用 Pytest 進行測試。

1.  **啟動 Poetry 虛擬環境** (如果尚未啟動)：
    ```bash
    poetry shell
    ```
    或者，您可以透過在每個指令前加上 `poetry run` 來執行，例如 `poetry run pytest`。

2.  **執行所有測試**：
    在專案根目錄下執行：
    ```bash
    pytest
    ```
    或者，如果您不在虛擬環境中：
    ```bash
    poetry run pytest
    ```

Pytest 會自動發現並執行 `tests` 資料夾中的所有測試案例。

## 專案結構

```
data_pipeline_v15/
├── src/                    # 主要的應用程式原始碼 (採用 src layout)
│   └── data_pipeline_v15/
│       └── __init__.py
├── tests/                  # 測試案例
│   ├── __init__.py
│   └── test_example.py     # 範例測試檔案
├── pyproject.toml          # Poetry 設定檔，包含依賴項與專案元數據
├── poetry.lock             # 精確的依賴版本鎖定檔案
└── README.md               # 本說明文件
```

## (可選) 其他指令

*   **新增依賴**：
    ```bash
    poetry add <package_name>
    ```
*   **新增開發依賴**：
    ```bash
    poetry add <package_name> --group dev
    ```
*   **更新依賴**：
    ```bash
    poetry update
    ```

---
此 README.md 文件旨在協助開發者快速上手「數據整合平台 v15」的開發環境。
