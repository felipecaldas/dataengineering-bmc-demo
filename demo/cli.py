from __future__ import annotations

import argparse
import json
import socket
import sys
import time
from datetime import date
from typing import Any, Callable

import httpx
from confluent_kafka.admin import AdminClient

from demo import blob
from demo.config import settings
from demo.db import connect, set_config
from demo.failures import late_stores, no_asn, phantom_stock, reset, schema_drift, slow_cluster
from demo.gates import ack_name, asn_ready, eod_status
from demo.seed import seed_history, seed_reference
from demo.simulate import generate_asn, release_eod, simulate_day, wait_for_ingest
from demo.stages import bronze_ingest, replenishment_calc, silver_conform
from demo.wms import ack_exists, deliver_to_wms


def emit(value: Any) -> None:
    print(json.dumps(value, indent=2, default=str, sort_keys=True))


def parsed_date(value: str | None) -> date:
    return date.fromisoformat(value) if value else settings.trading_date


def cmd_health(_: argparse.Namespace) -> None:
    result: dict[str, dict[str, Any]] = {}
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            result["postgres"] = {"healthy": cur.fetchone()["ok"] == 1}
    except Exception as exc:
        result["postgres"] = {"healthy": False, "message": str(exc)}
    try:
        metadata = AdminClient({"bootstrap.servers": settings.kafka_bootstrap}).list_topics(timeout=5)
        required = {"pos.transactions.v1", "pos.store-eod.v1"}
        result["kafka"] = {
            "healthy": required.issubset(metadata.topics),
            "topics": sorted(required.intersection(metadata.topics)),
        }
    except Exception as exc:
        result["kafka"] = {"healthy": False, "message": str(exc)}
    try:
        blob.init_container()
        result["azurite"] = {"healthy": True, "container": settings.azure_container}
    except Exception as exc:
        result["azurite"] = {"healthy": False, "message": str(exc)}
    try:
        response = httpx.get("http://databricks-local:8000/health", timeout=5)
        response.raise_for_status()
        result["databricks_local"] = {"healthy": True, **response.json()}
    except Exception as exc:
        result["databricks_local"] = {"healthy": False, "message": str(exc)}
    try:
        with socket.create_connection((settings.wms_host, settings.wms_port), timeout=5):
            result["wms_sftp"] = {"healthy": True}
    except Exception as exc:
        result["wms_sftp"] = {"healthy": False, "message": str(exc)}
    result["all_healthy"] = all(item.get("healthy", False) for item in result.values())
    emit(result)
    if not result["all_healthy"]:
        raise SystemExit(1)


def cmd_databricks_run(args: argparse.Namespace) -> None:
    trading_date = parsed_date(args.date)
    response = httpx.post(
        "http://databricks-local:8000/api/2.1/jobs/run-now",
        json={
            "job_id": args.job_id,
            "job_parameters": {"trading_date": trading_date.isoformat()},
            "idempotency_token": f"{args.job_id}-{trading_date}-{time.time_ns()}",
        },
        timeout=10,
    )
    response.raise_for_status()
    run_id = response.json()["run_id"]
    while True:
        run_response = httpx.get(
            "http://databricks-local:8000/api/2.1/jobs/runs/get",
            params={"run_id": run_id},
            timeout=10,
        )
        run_response.raise_for_status()
        run = run_response.json()
        if run["state"]["life_cycle_state"] == "TERMINATED":
            emit(run)
            if run["state"].get("result_state") != "SUCCESS":
                raise SystemExit(1)
            return
        time.sleep(1)


def cmd_gate_eod(args: argparse.Namespace) -> None:
    trading_date = parsed_date(args.date)
    deadline = time.monotonic() + args.timeout
    while True:
        status = eod_status(trading_date)
        if status.ready or not args.wait or time.monotonic() >= deadline:
            emit(status.as_dict())
            if not status.ready:
                raise SystemExit(2)
            return
        time.sleep(args.interval)


def cmd_gate_asn(args: argparse.Namespace) -> None:
    trading_date = parsed_date(args.date)
    deadline = time.monotonic() + args.timeout
    while True:
        ready = asn_ready(trading_date)
        if ready or not args.wait or time.monotonic() >= deadline:
            emit({"trading_date": trading_date, "ready": ready})
            if not ready:
                raise SystemExit(2)
            return
        time.sleep(args.interval)


def cmd_gate_ack(args: argparse.Namespace) -> None:
    trading_date = parsed_date(args.date)
    deadline = time.monotonic() + args.timeout
    while True:
        ready = ack_exists(trading_date)
        if ready or not args.wait or time.monotonic() >= deadline:
            emit({"trading_date": trading_date, "ack": ack_name(trading_date), "ready": ready})
            if not ready:
                raise SystemExit(2)
            return
        time.sleep(args.interval)


def add_date(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--date", help="Trading date (YYYY-MM-DD)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Retail DataOps demo operator CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-blob")
    sub.add_parser("health")

    seed_parser = sub.add_parser("seed")
    seed_parser.add_argument("--history-days", type=int, default=28)

    simulate_parser = sub.add_parser("simulate")
    add_date(simulate_parser)
    simulate_parser.add_argument("--withhold", type=int)
    simulate_parser.add_argument("--wait-ingest", action="store_true")

    release_parser = sub.add_parser("release-eod")
    add_date(release_parser)

    asn_parser = sub.add_parser("generate-asn")
    add_date(asn_parser)
    asn_parser.add_argument("--lines", type=int, default=5000)
    asn_parser.add_argument("--force", action="store_true")

    for name in ("bronze", "silver", "replen", "deliver"):
        item = sub.add_parser(name)
        add_date(item)

    dbx = sub.add_parser("databricks-run")
    dbx.add_argument("job_id", type=int, choices=[440, 441, 447])
    add_date(dbx)

    for name in ("gate-eod", "gate-asn", "gate-ack"):
        gate = sub.add_parser(name)
        add_date(gate)
        gate.add_argument("--wait", action="store_true")
        gate.add_argument("--timeout", type=int, default=7200)
        gate.add_argument("--interval", type=int, default=5)

    failure = sub.add_parser("failure")
    failure_sub = failure.add_subparsers(dest="failure", required=True)
    late = failure_sub.add_parser("late-store")
    add_date(late)
    late.add_argument("--stores", type=int, default=1)
    absent = failure_sub.add_parser("no-asn")
    add_date(absent)
    drift = failure_sub.add_parser("schema-drift")
    add_date(drift)
    phantom = failure_sub.add_parser("phantom-stock")
    add_date(phantom)
    phantom.add_argument("--rows", type=int, default=400)
    slow = failure_sub.add_parser("slow-cluster")
    slow.add_argument("--seconds", type=int, default=45)

    reset_parser = sub.add_parser("reset")
    add_date(reset_parser)

    wms_mode = sub.add_parser("wms-mode")
    wms_mode.add_argument("mode", choices=["ack", "never_ack", "late", "reject"])
    wms_mode.add_argument("--delay", type=int)

    args = parser.parse_args()
    if args.command == "init-blob":
        blob.init_container()
        emit({"container": settings.azure_container, "status": "READY"})
    elif args.command == "health":
        cmd_health(args)
    elif args.command == "seed":
        emit({"reference": seed_reference(), "history_rows": seed_history(args.history_days)})
    elif args.command == "simulate":
        trading_date = parsed_date(args.date)
        result = simulate_day(trading_date, args.withhold)
        if args.wait_ingest:
            result["ingress_transactions"] = wait_for_ingest(
                trading_date, result["transactions"]
            )
        emit(result)
    elif args.command == "release-eod":
        emit(release_eod(parsed_date(args.date)))
    elif args.command == "generate-asn":
        emit(generate_asn(parsed_date(args.date), args.lines, args.force))
    elif args.command == "bronze":
        emit(bronze_ingest(parsed_date(args.date)))
    elif args.command == "silver":
        emit(silver_conform(parsed_date(args.date)))
    elif args.command == "replen":
        emit(replenishment_calc(parsed_date(args.date)))
    elif args.command == "deliver":
        emit(deliver_to_wms(parsed_date(args.date)))
    elif args.command == "databricks-run":
        cmd_databricks_run(args)
    elif args.command == "gate-eod":
        cmd_gate_eod(args)
    elif args.command == "gate-asn":
        cmd_gate_asn(args)
    elif args.command == "gate-ack":
        cmd_gate_ack(args)
    elif args.command == "failure":
        trading_date = parsed_date(getattr(args, "date", None))
        if args.failure == "late-store":
            emit(late_stores(trading_date, args.stores))
        elif args.failure == "no-asn":
            emit(no_asn(trading_date))
        elif args.failure == "schema-drift":
            emit(schema_drift(trading_date))
        elif args.failure == "phantom-stock":
            emit(phantom_stock(trading_date, args.rows))
        elif args.failure == "slow-cluster":
            emit(slow_cluster(args.seconds))
    elif args.command == "reset":
        emit(reset(parsed_date(args.date)))
    elif args.command == "wms-mode":
        set_config("wms_mode", args.mode)
        if args.delay is not None:
            set_config("wms_ack_delay_seconds", str(args.delay))
        emit({"wms_mode": args.mode, "delay_seconds": args.delay})


if __name__ == "__main__":
    main()
