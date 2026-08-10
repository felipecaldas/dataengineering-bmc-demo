from __future__ import annotations

import csv
import io
import json
import time
from datetime import date
from pathlib import Path

from psycopg.types.json import Jsonb

from demo import blob
from demo.db import connect, get_config, stage_run
from demo.gates import asn_name, order_name
from demo.simulate import ASN_HEADER


def bronze_ingest(trading_date: date) -> dict:
    with stage_run("bronze_ingest", trading_date) as run:
        if not blob.exists(asn_name(trading_date)):
            raise FileNotFoundError(f"Required ASN blob is absent: {asn_name(trading_date)}")
        raw_content = blob.download_bytes(asn_name(trading_date)).decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(raw_content))
        header = reader.fieldnames or []
        asn_rows = list(reader)
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bronze.pos_transactions
                  (transaction_id, trading_date, store_id, till_id, product_sku, qty,
                   unit_price_ex_gst, transaction_ts_local, transaction_ts_utc, loaded_at)
                SELECT payload->>'transaction_id', (payload->>'trading_date')::date,
                       (payload->>'store_id')::int, (payload->>'till_id')::int,
                       payload->>'product_sku', (payload->>'qty')::int,
                       (payload->>'unit_price_ex_gst')::numeric,
                       (payload->>'transaction_ts_local')::timestamptz,
                       (payload->>'transaction_ts_utc')::timestamptz, now()
                FROM ingress.kafka_events
                WHERE topic='pos.transactions.v1' AND payload->>'trading_date'=%s
                ON CONFLICT (transaction_id) DO UPDATE SET
                  qty=excluded.qty, unit_price_ex_gst=excluded.unit_price_ex_gst,
                  loaded_at=excluded.loaded_at
                """,
                (trading_date.isoformat(),),
            )
            cur.execute(
                """
                INSERT INTO bronze.store_eod
                  (store_id, trading_date, transaction_count, total_ex_gst,
                   eod_ts_local, eod_ts_utc, loaded_at)
                SELECT (payload->>'store_id')::int, (payload->>'trading_date')::date,
                       (payload->>'transaction_count')::int,
                       (payload->>'total_ex_gst')::numeric,
                       (payload->>'eod_ts_local')::timestamptz,
                       (payload->>'eod_ts_utc')::timestamptz, now()
                FROM ingress.kafka_events
                WHERE topic='pos.store-eod.v1' AND payload->>'trading_date'=%s
                ON CONFLICT (store_id, trading_date) DO UPDATE SET
                  transaction_count=excluded.transaction_count,
                  total_ex_gst=excluded.total_ex_gst,
                  eod_ts_local=excluded.eod_ts_local,
                  eod_ts_utc=excluded.eod_ts_utc,
                  loaded_at=excluded.loaded_at
                """,
                (trading_date.isoformat(),),
            )
            cur.execute(
                """
                INSERT INTO bronze.asn_raw(trading_date, blob_name, header, rows, loaded_at)
                VALUES (%s,%s,%s,%s,now())
                ON CONFLICT (trading_date) DO UPDATE SET
                  blob_name=excluded.blob_name, header=excluded.header,
                  rows=excluded.rows, loaded_at=excluded.loaded_at
                """,
                (
                    trading_date,
                    asn_name(trading_date),
                    Jsonb(header),
                    Jsonb(asn_rows),
                ),
            )
            cur.execute(
                "SELECT count(*) AS count FROM bronze.pos_transactions WHERE trading_date=%s",
                (trading_date,),
            )
            txn_count = cur.fetchone()["count"]
            cur.execute(
                "SELECT count(*) AS count FROM bronze.store_eod WHERE trading_date=%s",
                (trading_date,),
            )
            eod_count = cur.fetchone()["count"]
        run["row_count"] = txn_count + eod_count + len(asn_rows)
        run["message"] = json.dumps(
            {"transactions": txn_count, "eod": eod_count, "asn": len(asn_rows)}
        )
        return {"run_id": run["run_id"], **json.loads(run["message"])}


def silver_conform(trading_date: date) -> dict:
    with stage_run("silver_conform", trading_date) as run:
        delay = int(get_config("silver_delay_seconds", "0") or 0)
        if delay:
            time.sleep(delay)
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT header, rows FROM bronze.asn_raw WHERE trading_date=%s",
                (trading_date,),
            )
            asn = cur.fetchone()
            if not asn:
                raise RuntimeError(f"No bronze ASN data for {trading_date}")
            actual_header = list(asn["header"])
            if actual_header != ASN_HEADER:
                added = [column for column in actual_header if column not in ASN_HEADER]
                missing = [column for column in ASN_HEADER if column not in actual_header]
                raise ValueError(
                    "ASN schema contract failed before silver load: "
                    f"added={added or '[]'}, missing={missing or '[]'}"
                )
            cur.execute(
                """
                INSERT INTO silver.pos_transactions
                SELECT * FROM bronze.pos_transactions WHERE trading_date=%s
                ON CONFLICT (transaction_id) DO UPDATE SET
                  trading_date=excluded.trading_date, store_id=excluded.store_id,
                  till_id=excluded.till_id, product_sku=excluded.product_sku,
                  qty=excluded.qty, unit_price_ex_gst=excluded.unit_price_ex_gst,
                  transaction_ts_local=excluded.transaction_ts_local,
                  transaction_ts_utc=excluded.transaction_ts_utc,
                  loaded_at=excluded.loaded_at
                """,
                (trading_date,),
            )
            cur.execute(
                """
                INSERT INTO silver.store_eod
                SELECT * FROM bronze.store_eod WHERE trading_date=%s
                ON CONFLICT (store_id, trading_date) DO UPDATE SET
                  transaction_count=excluded.transaction_count,
                  total_ex_gst=excluded.total_ex_gst,
                  eod_ts_local=excluded.eod_ts_local,
                  eod_ts_utc=excluded.eod_ts_utc,
                  loaded_at=excluded.loaded_at
                """,
                (trading_date,),
            )
            cur.execute("DELETE FROM silver.asn_inbound WHERE trading_date=%s", (trading_date,))
            rows = asn["rows"]
            cur.executemany(
                """
                INSERT INTO silver.asn_inbound
                  (asn_id, trading_date, product_sku, expected_units,
                   expected_arrival_date, supplier_id)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (asn_id, product_sku) DO UPDATE SET
                  expected_units=excluded.expected_units,
                  expected_arrival_date=excluded.expected_arrival_date,
                  supplier_id=excluded.supplier_id
                """,
                [
                    (
                        row["asn_id"],
                        row["trading_date"],
                        row["product_sku"],
                        int(row["expected_units"]),
                        row["expected_arrival_date"],
                        row["supplier_id"],
                    )
                    for row in rows
                ],
            )
            cur.execute(
                "SELECT count(*) AS count FROM silver.pos_transactions WHERE trading_date=%s",
                (trading_date,),
            )
            txn_count = cur.fetchone()["count"]
            cur.execute(
                "SELECT count(*) AS count FROM silver.store_eod WHERE trading_date=%s",
                (trading_date,),
            )
            eod_count = cur.fetchone()["count"]
        run["row_count"] = txn_count + eod_count + len(rows)
        run["message"] = json.dumps(
            {"transactions": txn_count, "eod": eod_count, "asn": len(rows), "delay": delay}
        )
        return {"run_id": run["run_id"], **json.loads(run["message"])}


def replenishment_calc(trading_date: date) -> dict:
    with stage_run("replenishment_calc", trading_date) as run:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT store_id, product_sku, replenishment_units
                FROM gold.fct_replenishment_need
                WHERE replenishment_units > 0
                ORDER BY store_id, product_sku
                """
            )
            rows = cur.fetchall()
        stream = io.StringIO(newline="")
        writer = csv.writer(stream)
        writer.writerow(
            ["order_id", "trading_date", "store_id", "product_sku", "replenishment_units"]
        )
        for index, row in enumerate(rows, start=1):
            writer.writerow(
                [
                    f"RPL-{trading_date:%Y%m%d}-{index:06d}",
                    trading_date.isoformat(),
                    row["store_id"],
                    row["product_sku"],
                    row["replenishment_units"],
                ]
            )
        content = stream.getvalue().encode()
        target = order_name(trading_date)
        blob.upload_bytes(target, content, "text/csv")
        host_visible = Path("/workspace/runtime/outbound") / target.rsplit("/", 1)[-1]
        host_visible.parent.mkdir(parents=True, exist_ok=True)
        host_visible.write_bytes(content)
        run["row_count"] = len(rows)
        run["message"] = target
        return {"run_id": run["run_id"], "blob": target, "order_lines": len(rows)}
