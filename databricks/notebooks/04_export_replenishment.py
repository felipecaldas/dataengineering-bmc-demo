# Databricks notebook source
# MAGIC %md
# MAGIC # Export tested replenishment orders
# MAGIC Read the dbt-built gold result and create the deterministic WMS CSV.

# COMMAND ----------

dbutils.widgets.text("trading_date", "2026-08-14")
dbutils.widgets.text("outbound_path", "")
trading_date = dbutils.widgets.get("trading_date")
outbound_path = dbutils.widgets.get("outbound_path").rstrip("/")
if not outbound_path:
    raise ValueError("outbound_path is required")

# COMMAND ----------

import csv
import io
import json
import hashlib
from datetime import date

parsed_date = date.fromisoformat(trading_date)
date_key = parsed_date.strftime("%Y%m%d")
destination = f"{outbound_path}/REPLEN_ORDER_{date_key}.csv"

rows = (
    spark.table("gold.fct_replenishment_need")
    .where("replenishment_units > 0")
    .select("store_id", "product_sku", "replenishment_units")
    .orderBy("store_id", "product_sku")
    .collect()
)

stream = io.StringIO(newline="")
writer = csv.writer(stream)
writer.writerow(
    ["order_id", "trading_date", "store_id", "product_sku", "replenishment_units"]
)
for index, row in enumerate(rows, start=1):
    writer.writerow(
        [
            f"RPL-{date_key}-{index:06d}",
            trading_date,
            row["store_id"],
            row["product_sku"],
            row["replenishment_units"],
        ]
    )

content = stream.getvalue()
dbutils.fs.put(destination, content, overwrite=True)
dbutils.notebook.exit(
    json.dumps(
        {
            "trading_date": trading_date,
            "order_lines": len(rows),
            "destination": destination,
            "sha256": hashlib.sha256(content.encode()).hexdigest(),
        }
    )
)
