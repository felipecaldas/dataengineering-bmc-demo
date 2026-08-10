# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest
# MAGIC Pure and idempotent for a supplied `trading_date`. In the self-contained
# MAGIC profile, the Jobs API-compatible service invokes the same implementation.

# COMMAND ----------

dbutils.widgets.text("trading_date", "2026-08-14")
trading_date = dbutils.widgets.get("trading_date")

# COMMAND ----------

# Real-Azure deployment adapter. Package `demo` as a workspace wheel before using
# this notebook; cloud credentials are supplied by the job environment.
from datetime import date
from demo.stages import bronze_ingest

display(bronze_ingest(date.fromisoformat(trading_date)))

