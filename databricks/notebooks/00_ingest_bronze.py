# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Bronze source contract
# MAGIC
# MAGIC Validate one complete Azure landing manifest before changing any target,
# MAGIC then replace the requested Delta windows idempotently.

# COMMAND ----------

import csv
import json
import time
from datetime import date, timedelta

from pyspark.sql import functions as F
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
dbutils.widgets.text("landing_path", "")
trading_date = date.fromisoformat(dbutils.widgets.get("trading_date"))
landing_path = dbutils.widgets.get("landing_path").rstrip("/")
if not landing_path:
    raise ValueError("landing_path is required")


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
        "key": ["product_sku"],
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
        ),
        "key": ["transaction_id"],
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
        ),
        "key": ["store_id", "trading_date"],
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
        "key": ["asn_id", "product_sku"],
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
        "key": ["store_id", "product_sku", "snapshot_date"],
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
        "key": ["sale_date", "store_id", "product_sku"],
        "date_column": "sale_date",
        "predicate": (
            f"sale_date >= DATE '{(trading_date - timedelta(days=28)).isoformat()}' "
            f"AND sale_date < DATE '{trading_date.isoformat()}'"
        ),
    },
}

# COMMAND ----------

manifest = json.loads(dbutils.fs.head(f"{landing_path}/manifest.json", 1024 * 1024))
if manifest.get("format_version") != 2:
    raise ValueError("Unsupported Azure landing manifest version")
if manifest.get("trading_date") != trading_date.isoformat():
    raise ValueError("Manifest trading date does not match the notebook parameter")
if not manifest.get("simulation_id"):
    raise ValueError("Manifest simulation_id is required")
if not str(manifest.get("eod_decision", "")).startswith("PROCEED"):
    raise ValueError("Manifest does not record a proceeding EOD decision")
if set(manifest.get("tables", {})) != set(TABLES):
    raise ValueError("Manifest table set does not match the six-table Bronze contract")

frames = {}
counts = {}
for table_name, definition in TABLES.items():
    table_manifest = manifest["tables"][table_name]
    if table_manifest.get("file") != f"{table_name}.csv":
        raise ValueError(f"{table_name} manifest filename is not canonical")
    path = f"{landing_path}/{table_manifest['file']}"
    first_line = dbutils.fs.head(path, 64 * 1024).splitlines()[0]
    actual_header = next(csv.reader([first_line]))
    expected_header = definition["schema"].fieldNames()
    if actual_header != expected_header:
        raise ValueError(
            f"{table_name} header mismatch: expected={expected_header}, actual={actual_header}"
        )

    actual_sha = (
        spark.read.format("binaryFile")
        .load(path)
        .select(F.lower(F.sha2("content", 256)).alias("sha256"))
        .first()["sha256"]
    )
    if actual_sha != str(table_manifest["sha256"]).lower():
        raise ValueError(f"{table_name} checksum does not match the landing manifest")

    frame = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(definition["schema"])
        .csv(path)
        .cache()
    )
    actual_count = frame.count()
    expected_count = int(table_manifest["rows"])
    if actual_count != expected_count:
        raise ValueError(
            f"{table_name} row count mismatch: expected={expected_count}, actual={actual_count}"
        )
    null_predicate = " OR ".join(f"`{column}` IS NULL" for column in expected_header)
    if frame.filter(null_predicate).limit(1).count():
        raise ValueError(f"{table_name} contains null or unparseable required values")
    if (
        frame.groupBy(*definition["key"])
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
    ):
        raise ValueError(f"{table_name} contains duplicate natural keys")
    predicate = definition["predicate"]
    if predicate and frame.filter(f"NOT ({predicate})").limit(1).count():
        raise ValueError(f"{table_name} contains rows outside its replacement window")
    frames[table_name] = frame
    counts[table_name] = actual_count

delay = int(manifest.get("ingest_delay_seconds", 0))
if delay < 0 or delay > 300:
    raise ValueError("ingest_delay_seconds must be between zero and 300")
if delay:
    time.sleep(delay)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
results = {}
for table_name, definition in TABLES.items():
    frame = frames[table_name]
    predicate = definition["predicate"]
    full_name = f"bronze.{table_name}"
    writer = frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if predicate and spark.catalog.tableExists(full_name):
        writer.option("replaceWhere", predicate).saveAsTable(full_name)
    elif predicate:
        writer.partitionBy(definition["date_column"]).saveAsTable(full_name)
    else:
        writer.saveAsTable(full_name)

    loaded = spark.table(full_name)
    destination_count = loaded.filter(predicate).count() if predicate else loaded.count()
    if destination_count != counts[table_name]:
        raise ValueError(
            f"bronze.{table_name} Delta verification failed: "
            f"expected={counts[table_name]}, actual={destination_count}"
        )
    results[table_name] = destination_count
    frame.unpersist()

dbutils.notebook.exit(
    json.dumps(
        {
            "trading_date": trading_date.isoformat(),
            "simulation_id": manifest["simulation_id"],
            "tables": results,
        }
    )
)
