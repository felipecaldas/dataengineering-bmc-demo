from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
    azure_connection_string: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    azure_account: str = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    azure_key: str = os.getenv("AZURE_STORAGE_KEY", "")
    azure_container: str = os.getenv("AZURE_STORAGE_CONTAINER", "kmart-demo")
    azure_prefix: str = os.getenv("AZURE_STORAGE_PREFIX", "retail-data-demo").strip("/")
    databricks_storage_base_path: str = os.getenv(
        "DATABRICKS_STORAGE_BASE_PATH", ""
    ).rstrip("/")
    runtime_root: Path = Path(os.getenv("DEMO_RUNTIME_ROOT", "/workspace/runtime"))
    trading_date: date = date.fromisoformat(
        os.getenv("DEMO_TRADING_DATE", "2026-08-14")
    )
    store_count: int = int(os.getenv("STORE_COUNT", "325"))
    sku_count: int = int(os.getenv("SKU_COUNT", "2000"))
    txn_per_store: int = int(os.getenv("TXN_PER_STORE", "200"))
    demo_seed: int = int(os.getenv("DEMO_SEED", "20260814"))
    wms_host: str = os.getenv("WMS_HOST", "localhost")
    wms_port: int = int(os.getenv("WMS_PORT", "2222"))
    wms_user: str = os.getenv("WMS_USER", "demo")
    wms_password: str = os.getenv("WMS_PASSWORD", "demo")


settings = Settings()
