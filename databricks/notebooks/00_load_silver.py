# Databricks notebook source
# MAGIC %md
# MAGIC # Load validated silver inputs
# MAGIC
# MAGIC This optional real-Azure adapter loads the date-scoped CSV snapshot produced
# MAGIC only after the self-contained profile has passed its silver ASN contract.
# MAGIC Writes use deterministic Delta replacement rules and are safe to repeat.

# COMMAND ----------

import json
from datetime import date, timedelta

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

dbutils.widgets.text("trading_date", "2026-08-14")
dbutils.widgets.text("base_path", "dbfs:/tmp/retail-data-demo/20260814")
trading_date = date.fromisoformat(dbutils.widgets.get("trading_date"))
base_path = dbutils.widgets.get("base_path").rstrip("/")


def schema(*fields):
    return StructType([StructField(name, data_type, False) for name, data_type in fields])


TABLES = {
    "product_master": {
        "schema": schema(
            ("product_sku", StringType()),
            ("product_name", StringType()),
            ("category", StringType()),
            ("unit_cost", DecimalType(12, 2)),
            ("retail_price", DecimalType(12, 2)),
            ("lead_time_days", IntegerType()),
            ("review_period_days", IntegerType()),
            ("safety_stock_units", IntegerType()),
            ("is_active_line", BooleanType()),
        ),
        "date_column": None,
        "predicate": None,
    },
    "pos_transactions": {
        "schema": schema(
            ("transaction_id", StringType()),
            ("trading_date", DateType()),
            ("store_id", IntegerType()),
            ("till_id", IntegerType()),
            ("product_sku", StringType()),
            ("qty", IntegerType()),
            ("unit_price_ex_gst", DecimalType(12, 2)),
            ("transaction_ts_local", TimestampType()),
            ("transaction_ts_utc", TimestampType()),
            ("loaded_at", TimestampType()),
        ),
        "date_column": "trading_date",
        "predicate": f"trading_date = DATE '{trading_date.isoformat()}'",
    },
    "store_eod": {
        "schema": schema(
            ("store_id", IntegerType()),
            ("trading_date", DateType()),
            ("transaction_count", IntegerType()),
            ("total_ex_gst", DecimalType(14, 2)),
            ("eod_ts_local", TimestampType()),
            ("eod_ts_utc", TimestampType()),
            ("loaded_at", TimestampType()),
        ),
        "date_column": "trading_date",
        "predicate": f"trading_date = DATE '{trading_date.isoformat()}'",
    },
    "asn_inbound": {
        "schema": schema(
            ("asn_id", StringType()),
            ("trading_date", DateType()),
            ("product_sku", StringType()),
            ("expected_units", IntegerType()),
            ("expected_arrival_date", DateType()),
            ("supplier_id", StringType()),
        ),
        "date_column": "trading_date",
        "predicate": f"trading_date = DATE '{trading_date.isoformat()}'",
    },
    "stock_on_hand": {
        "schema": schema(
            ("store_id", IntegerType()),
            ("product_sku", StringType()),
            ("on_hand_units", IntegerType()),
            ("on_order_units", IntegerType()),
            ("snapshot_date", DateType()),
        ),
        "date_column": "snapshot_date",
        "predicate": f"snapshot_date = DATE '{trading_date.isoformat()}'",
    },
    "sales_history": {
        "schema": schema(
            ("sale_date", DateType()),
            ("store_id", IntegerType()),
            ("product_sku", StringType()),
            ("units_sold", IntegerType()),
            ("sales_ex_gst", DecimalType(14, 2)),
        ),
        "date_column": "sale_date",
        "predicate": (
            f"sale_date >= DATE '{(trading_date - timedelta(days=28)).isoformat()}' "
            f"AND sale_date < DATE '{trading_date.isoformat()}'"
        ),
    },
}

# COMMAND ----------

manifest = json.loads(dbutils.fs.head(f"{base_path}/manifest.json", 1024 * 1024))
if manifest.get("format_version") != 1:
    raise ValueError("Unsupported Databricks silver export manifest version")
if manifest.get("trading_date") != trading_date.isoformat():
    raise ValueError("Manifest trading date does not match the notebook parameter")
if set(manifest.get("tables", {})) != set(TABLES):
    raise ValueError("Manifest table set does not match the six-table silver contract")

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
results = {}

for table_name, definition in TABLES.items():
    frame = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(definition["schema"])
        .csv(f"{base_path}/{table_name}.csv")
    )
    actual_count = frame.count()
    expected_count = int(manifest["tables"][table_name])
    if actual_count != expected_count:
        raise ValueError(
            f"silver.{table_name} row count mismatch: "
            f"expected={expected_count}, actual={actual_count}"
        )

    predicate = definition["predicate"]
    if predicate and frame.filter(f"NOT ({predicate})").limit(1).count():
        raise ValueError(f"silver.{table_name} contains rows outside its replacement window")

    full_name = f"silver.{table_name}"
    writer = frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if predicate and spark.catalog.tableExists(full_name):
        writer.option("replaceWhere", predicate).saveAsTable(full_name)
    elif predicate:
        writer.partitionBy(definition["date_column"]).saveAsTable(full_name)
    else:
        writer.saveAsTable(full_name)

    loaded = spark.table(full_name)
    destination_count = loaded.filter(predicate).count() if predicate else loaded.count()
    if destination_count != expected_count:
        raise ValueError(
            f"silver.{table_name} Delta verification failed: "
            f"expected={expected_count}, actual={destination_count}"
        )
    results[table_name] = destination_count

dbutils.notebook.exit(json.dumps({"trading_date": trading_date.isoformat(), "tables": results}))
