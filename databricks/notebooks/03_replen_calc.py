# Databricks notebook source
# MAGIC %md
# MAGIC # Replenishment calculation
# MAGIC Export the tested gold result to the outbound Blob path.

# COMMAND ----------

dbutils.widgets.text("trading_date", "2026-08-14")
trading_date = dbutils.widgets.get("trading_date")

# COMMAND ----------

from datetime import date
from demo.stages import replenishment_calc

display(replenishment_calc(date.fromisoformat(trading_date)))

