from __future__ import annotations

import csv
import io
from datetime import date
from pathlib import Path

from demo import blob
from demo.db import stage_run
from demo.gates import order_name


ORDER_HEADER = [
    "order_id",
    "trading_date",
    "store_id",
    "product_sku",
    "replenishment_units",
]


def import_databricks_order(trading_date: date) -> dict:
    filename = order_name(trading_date).rsplit("/", 1)[-1]
    source = Path("/workspace/runtime/outbound") / filename
    if not source.is_file():
        raise FileNotFoundError(f"Azure Databricks order export does not exist: {source}")
    content = source.read_bytes()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""))
    if reader.fieldnames != ORDER_HEADER:
        raise ValueError(
            f"Azure Databricks order header mismatch: expected={ORDER_HEADER}, "
            f"actual={reader.fieldnames}"
        )
    rows = list(reader)
    for index, row in enumerate(rows, start=1):
        expected_id = f"RPL-{trading_date:%Y%m%d}-{index:06d}"
        if row["order_id"] != expected_id:
            raise ValueError(f"Unexpected order ID at row {index}: {row['order_id']}")
        if row["trading_date"] != trading_date.isoformat():
            raise ValueError(f"Unexpected trading date at row {index}: {row['trading_date']}")
        if int(row["replenishment_units"]) <= 0:
            raise ValueError(f"Non-positive replenishment quantity at row {index}")

    target = order_name(trading_date)
    with stage_run("azure_replenishment_export", trading_date) as run:
        blob.upload_bytes(target, content, "text/csv")
        run["row_count"] = len(rows)
        run["message"] = target
    return {"blob": target, "order_lines": len(rows), "source": str(source)}
