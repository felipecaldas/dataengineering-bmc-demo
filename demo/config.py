from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


AZURITE_DEFAULT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://retail:retail@localhost:5432/retail"
    )
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
    azure_connection_string: str = os.getenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey={AZURITE_DEFAULT_KEY};"
        "BlobEndpoint=http://localhost:10000/devstoreaccount1;",
    )
    azure_container: str = os.getenv("AZURE_STORAGE_CONTAINER", "kmart-demo")
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

