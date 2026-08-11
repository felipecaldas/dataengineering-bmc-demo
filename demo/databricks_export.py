from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from demo.db import connect


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
EXPORT_ROOT = REPOSITORY_ROOT / "runtime" / "databricks" / "export"


@dataclass(frozen=True)
class ExportSpec:
    table: str
    columns: tuple[str, ...]
    predicate: str | None = None


EXPORT_SPECS = (
    ExportSpec(
        "product_master",
        (
            "product_sku",
            "product_name",
            "category",
            "unit_cost",
            "retail_price",
            "lead_time_days",
            "review_period_days",
            "safety_stock_units",
            "is_active_line::text AS is_active_line",
        ),
    ),
    ExportSpec(
        "pos_transactions",
        (
            "transaction_id",
            "trading_date",
            "store_id",
            "till_id",
            "product_sku",
            "qty",
            "unit_price_ex_gst",
            "transaction_ts_local",
            "transaction_ts_utc",
            "loaded_at",
        ),
        "trading_date = %s",
    ),
    ExportSpec(
        "store_eod",
        (
            "store_id",
            "trading_date",
            "transaction_count",
            "total_ex_gst",
            "eod_ts_local",
            "eod_ts_utc",
            "loaded_at",
        ),
        "trading_date = %s",
    ),
    ExportSpec(
        "asn_inbound",
        (
            "asn_id",
            "trading_date",
            "product_sku",
            "expected_units",
            "expected_arrival_date",
            "supplier_id",
        ),
        "trading_date = %s",
    ),
    ExportSpec(
        "stock_on_hand",
        (
            "store_id",
            "product_sku",
            "on_hand_units",
            "on_order_units",
            "snapshot_date",
        ),
        "snapshot_date = %s",
    ),
    ExportSpec(
        "sales_history",
        (
            "sale_date",
            "store_id",
            "product_sku",
            "units_sold",
            "sales_ex_gst",
        ),
        "sale_date >= %s AND sale_date < %s",
    ),
)


def export_directory(trading_date: date) -> Path:
    return EXPORT_ROOT / trading_date.strftime("%Y%m%d")


def _parameters(spec: ExportSpec, trading_date: date) -> tuple[Any, ...]:
    if spec.table == "sales_history":
        return trading_date - timedelta(days=28), trading_date
    return (trading_date,) if spec.predicate else ()


def _select_sql(spec: ExportSpec) -> str:
    where = f" WHERE {spec.predicate}" if spec.predicate else ""
    return f"SELECT {', '.join(spec.columns)} FROM silver.{spec.table}{where}"


def export_silver(trading_date: date) -> dict[str, Any]:
    """Export one consistent, validated silver snapshot for Azure Databricks."""
    target = export_directory(trading_date)
    target.mkdir(parents=True, exist_ok=True)
    manifest_path = target / "manifest.json"
    manifest_path.unlink(missing_ok=True)
    temporary_paths: list[Path] = []
    counts: dict[str, int] = {}

    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            for spec in EXPORT_SPECS:
                parameters = _parameters(spec, trading_date)
                select_sql = _select_sql(spec)
                cur.execute(f"SELECT count(*) AS count FROM ({select_sql}) AS exported", parameters)
                count = int(cur.fetchone()["count"])
                if count == 0:
                    raise RuntimeError(
                        f"silver.{spec.table} has no rows for the {trading_date} export window"
                    )

                temporary_path = target / f".{spec.table}.{os.getpid()}.tmp"
                temporary_paths.append(temporary_path)
                copy_sql = f"COPY ({select_sql}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
                with temporary_path.open("wb") as output, cur.copy(
                    copy_sql, parameters
                ) as copy:
                    while data := copy.read():
                        output.write(data)
                counts[spec.table] = count

        for spec in EXPORT_SPECS:
            temporary_path = target / f".{spec.table}.{os.getpid()}.tmp"
            temporary_path.replace(target / f"{spec.table}.csv")

        manifest = {
            "format_version": 1,
            "trading_date": trading_date.isoformat(),
            "history_start_date": (trading_date - timedelta(days=28)).isoformat(),
            "tables": counts,
        }
        temporary_manifest = target / f".manifest.{os.getpid()}.tmp"
        temporary_manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        temporary_manifest.replace(manifest_path)
        return manifest
    finally:
        for path in temporary_paths:
            path.unlink(missing_ok=True)
