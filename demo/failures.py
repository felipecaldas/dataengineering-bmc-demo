from __future__ import annotations

import time
from datetime import date
from pathlib import Path

from demo import blob
from demo.db import connect, set_config
from demo.gates import asn_name, eod_status
from demo.simulate import generate_asn, release_eod


def late_stores(trading_date: date, count: int) -> dict:
    if count < 1:
        raise ValueError("Store count must be at least 1")
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.store_id
            FROM silver.dim_store s
            JOIN silver.trading_calendar c
              ON c.state_code=s.state_code AND c.calendar_date=%s
            WHERE s.status='TRADING' AND c.is_trading_day=true
            ORDER BY s.store_id DESC LIMIT %s
            """,
            (trading_date, count),
        )
        store_ids = [row["store_id"] for row in cur.fetchall()]
        cur.execute(
            """
            DELETE FROM ingress.kafka_events
            WHERE topic='pos.store-eod.v1' AND payload->>'trading_date'=%s
              AND (payload->>'store_id')::int = ANY(%s)
            """,
            (trading_date.isoformat(), store_ids),
        )
        cur.execute(
            "DELETE FROM bronze.store_eod WHERE trading_date=%s AND store_id=ANY(%s)",
            (trading_date, store_ids),
        )
        cur.execute(
            "DELETE FROM silver.store_eod WHERE trading_date=%s AND store_id=ANY(%s)",
            (trading_date, store_ids),
        )
    set_config("withhold_eod_count", str(count))
    return {"failure": "late_store", "withheld_store_ids": sorted(store_ids)}


def no_asn(trading_date: date) -> dict:
    set_config("asn_enabled", "false")
    deleted = blob.delete(asn_name(trading_date))
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM bronze.asn_raw WHERE trading_date=%s", (trading_date,))
    host_visible = Path("/workspace/runtime/asn") / f"ASN_{trading_date:%Y%m%d}.csv"
    if host_visible.exists():
        host_visible.unlink()
    return {"failure": "no_asn", "blob_deleted": deleted}


def schema_drift(trading_date: date) -> dict:
    set_config("asn_enabled", "true")
    set_config("asn_schema_variant", "drift")
    result = generate_asn(trading_date, force=True)
    return {"failure": "schema_drift", **result}


def phantom_stock(trading_date: date, count: int = 400) -> dict:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS silver.stock_on_hand_failure_snapshot")
        cur.execute(
            """
            CREATE TABLE silver.stock_on_hand_failure_snapshot AS
            SELECT * FROM silver.stock_on_hand
            WHERE snapshot_date=%s
            ORDER BY store_id, product_sku
            LIMIT %s
            """,
            (trading_date, count),
        )
        cur.execute(
            """
            UPDATE silver.stock_on_hand target SET on_hand_units=-12
            FROM silver.stock_on_hand_failure_snapshot snap
            WHERE target.store_id=snap.store_id
              AND target.product_sku=snap.product_sku
              AND target.snapshot_date=snap.snapshot_date
            """
        )
        affected = cur.rowcount
    return {"failure": "phantom_stock", "affected_rows": affected}


def slow_cluster(seconds: int = 45) -> dict:
    set_config("silver_delay_seconds", str(seconds))
    return {"failure": "slow_cluster", "silver_delay_seconds": seconds}


def reset(trading_date: date) -> dict:
    # Quiesce the WMS watcher before touching its file and delivery state.
    # The worker re-checks both mode and state after any configured delay.
    set_config("wms_mode", "never_ack")
    for key, value in {
        "withhold_eod_count": "0",
        "asn_enabled": "true",
        "asn_schema_variant": "standard",
        "silver_delay_seconds": "0",
        "wms_ack_delay_seconds": "2",
    }.items():
        set_config(key, value)
    restored = 0
    removed_files = []
    inbound = Path("/wms/inbound/replen") / f"REPLEN_ORDER_{trading_date:%Y%m%d}.csv"
    if inbound.exists():
        inbound.unlink()
        removed_files.append(str(inbound))
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('silver.stock_on_hand_failure_snapshot') AS table_name")
        if cur.fetchone()["table_name"]:
            cur.execute(
                """
                UPDATE silver.stock_on_hand target SET
                  on_hand_units=snap.on_hand_units,
                  on_order_units=snap.on_order_units
                FROM silver.stock_on_hand_failure_snapshot snap
                WHERE target.store_id=snap.store_id
                  AND target.product_sku=snap.product_sku
                  AND target.snapshot_date=snap.snapshot_date
                """
            )
            restored = cur.rowcount
            # dbt retry starts at the failed test and its skipped descendants;
            # it deliberately does not rebuild this already-successful parent.
            # Repair its materialised rows from the same lossless snapshot so
            # the retry can resume precisely from the quality gate.
            cur.execute("SELECT to_regclass('intermediate.int_stock_on_hand') AS table_name")
            if cur.fetchone()["table_name"]:
                cur.execute(
                    """
                    UPDATE intermediate.int_stock_on_hand target SET
                      on_hand_units=snap.on_hand_units,
                      on_order_units=snap.on_order_units
                    FROM silver.stock_on_hand_failure_snapshot snap
                    WHERE target.store_id=snap.store_id
                      AND target.product_sku=snap.product_sku
                      AND target.snapshot_date=snap.snapshot_date
                    """
                )
            cur.execute("DROP TABLE silver.stock_on_hand_failure_snapshot")
        cur.execute(
            "DELETE FROM meta.wms_deliveries WHERE filename=%s",
            (f"REPLEN_ORDER_{trading_date:%Y%m%d}.csv",),
        )
    generated = generate_asn(trading_date, force=True)
    try:
        released = release_eod(trading_date)
        deadline = time.monotonic() + 15
        while not eod_status(trading_date).ready and time.monotonic() < deadline:
            time.sleep(0.5)
    except Exception as exc:
        released = {"warning": str(exc)}
    # A late-store rehearsal removes bronze/silver EOD rows, while schema drift
    # persists its unexpected header in bronze. Refresh both layers only after
    # the released EOD messages have reached ingress so reset is truly green.
    from demo.stages import bronze_ingest, silver_conform

    bronze = bronze_ingest(trading_date)
    silver = silver_conform(trading_date)
    for directory in (Path("/wms/ack"), Path("/wms/reject")):
        if directory.exists():
            for path in directory.glob(f"*_{trading_date:%Y%m%d}.txt"):
                path.unlink()
                removed_files.append(str(path))
    for directory in (
        Path("/workspace/runtime/wms/ack"),
        Path("/workspace/runtime/wms/reject"),
    ):
        if directory.exists():
            for path in directory.glob(f"*_{trading_date:%Y%m%d}.txt"):
                path.unlink()
                removed_files.append(str(path))
    set_config("wms_mode", "ack")
    return {
        "status": "GREEN",
        "stock_rows_restored": restored,
        "asn": generated,
        "bronze": bronze,
        "silver": silver,
        "eod": released,
        "wms_files_removed": removed_files,
    }
