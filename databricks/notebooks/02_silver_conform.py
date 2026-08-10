# Databricks notebook source
# MAGIC %md
# MAGIC # Silver conform
# MAGIC Dedupe, enforce the ASN schema contract, and retain both local and UTC POS timestamps.

# COMMAND ----------

dbutils.widgets.text("trading_date", "2026-08-14")
trading_date = dbutils.widgets.get("trading_date")

# COMMAND ----------

from datetime import date
from demo.stages import silver_conform

display(silver_conform(date.fromisoformat(trading_date)))

