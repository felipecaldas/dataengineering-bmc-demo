from __future__ import annotations

import argparse
import json
import socket
import time
from datetime import date
from typing import Any

from confluent_kafka.admin import AdminClient

from demo import blob
from demo.config import settings
from demo.failures import late_stores, no_asn, phantom_stock, reset, schema_drift, slow_cluster
from demo.gates import ack_name, asn_ready, eod_status
from demo.landing import stage_inputs
from demo.seed import seed_history, seed_reference
from demo.simulate import generate_asn, release_eod, simulate_day
from demo.state import set_config
from demo.wms import ack_exists, deliver_to_wms


def emit(value: Any) -> None:
    print(json.dumps(value, indent=2, default=str, sort_keys=True))


def parsed_date(value: str | None) -> date:
    return date.fromisoformat(value) if value else settings.trading_date


def cmd_health(_: argparse.Namespace) -> None:
    result: dict[str, dict[str, Any]] = {}
    try:
        metadata = AdminClient({"bootstrap.servers": settings.kafka_bootstrap}).list_topics(timeout=5)
        required = {
            "pos.transactions.v1",
            "pos.store-eod.v1",
            "retail.store-eod-readiness.v1",
        }
        result["kafka"] = {
            "healthy": required.issubset(metadata.topics),
            "topics": sorted(required.intersection(metadata.topics)),
        }
    except Exception as exc:
        result["kafka"] = {"healthy": False, "message": str(exc)}
    try:
        blob.init_container()
        result["azure_storage"] = {
            "healthy": True,
            "container": settings.azure_container,
            "prefix": settings.azure_prefix,
        }
    except Exception as exc:
        result["azure_storage"] = {"healthy": False, "message": str(exc)}
    try:
        with socket.create_connection((settings.wms_host, settings.wms_port), timeout=5):
            result["wms_sftp"] = {"healthy": True}
    except Exception as exc:
        result["wms_sftp"] = {"healthy": False, "message": str(exc)}
    result["all_healthy"] = all(item.get("healthy", False) for item in result.values())
    emit(result)
    if not result["all_healthy"]:
        raise SystemExit(1)


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
    sub.add_parser("health")

    seed_parser = sub.add_parser("seed")
    add_date(seed_parser)
    seed_parser.add_argument("--history-days", type=int, default=28)

    simulate_parser = sub.add_parser("simulate")
    add_date(simulate_parser)
    simulate_parser.add_argument("--withhold", type=int)

    release_parser = sub.add_parser("release-eod")
    add_date(release_parser)

    asn_parser = sub.add_parser("generate-asn")
    add_date(asn_parser)
    asn_parser.add_argument("--lines", type=int, default=5000)
    asn_parser.add_argument("--force", action="store_true")

    stage_parser = sub.add_parser("stage-inputs")
    add_date(stage_parser)
    deliver_parser = sub.add_parser("deliver")
    add_date(deliver_parser)

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
    if args.command == "health":
        cmd_health(args)
    elif args.command == "seed":
        trading_date = parsed_date(args.date)
        emit(
            {
                "reference": seed_reference(trading_date),
                "history_rows": seed_history(args.history_days, trading_date),
            }
        )
    elif args.command == "simulate":
        emit(simulate_day(parsed_date(args.date), args.withhold))
    elif args.command == "release-eod":
        emit(release_eod(parsed_date(args.date)))
    elif args.command == "generate-asn":
        emit(generate_asn(parsed_date(args.date), args.lines, args.force))
    elif args.command == "stage-inputs":
        emit(stage_inputs(parsed_date(args.date)))
    elif args.command == "deliver":
        emit(deliver_to_wms(parsed_date(args.date)))
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
