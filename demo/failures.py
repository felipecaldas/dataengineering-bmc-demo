from __future__ import annotations

import csv
import io
from datetime import date
from pathlib import Path

from demo import blob
from demo.config import settings
from demo.gates import asn_name, order_name
from demo.seed import landing_name, landing_prefix, trading_stores
from demo.simulate import generate_asn, release_eod
from demo.state import reset_config, set_config, stock_snapshot_path


def late_stores(trading_date: date, count: int) -> dict:
    stores = trading_stores(trading_date)
    if count < 1 or count >= len(stores):
        raise ValueError("Store count must be between one and the trading-store count minus one")
    store_ids = sorted(int(row["store_id"]) for row in stores[-count:])
    set_config("withhold_eod_count", str(count))
    return {
        "failure": "late_store",
        "withheld_store_ids": store_ids,
        "next_step": f"arm and simulate {trading_date} to publish the failure generation",
    }


def no_asn(trading_date: date) -> dict:
    set_config("asn_enabled", "false")
    deleted = blob.delete(asn_name(trading_date))
    marker = settings.runtime_root / "asn" / f"ASN_{trading_date:%Y%m%d}.csv"
    marker.unlink(missing_ok=True)
    blob.delete(f"{landing_prefix(trading_date)}/manifest.json")
    return {"failure": "no_asn", "azure_object_deleted": deleted}


def schema_drift(trading_date: date) -> dict:
    set_config("asn_enabled", "true")
    set_config("asn_schema_variant", "drift")
    result = generate_asn(trading_date, force=True)
    blob.delete(f"{landing_prefix(trading_date)}/manifest.json")
    return {"failure": "schema_drift", **result}


def phantom_stock(trading_date: date, count: int = 400) -> dict:
    if count < 1:
        raise ValueError("Row count must be at least one")
    name = landing_name(trading_date, "stock_on_hand")
    content = blob.download_bytes(name)
    snapshot = stock_snapshot_path(trading_date)
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    if not snapshot.exists():
        snapshot.write_bytes(content)

    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""))
    if reader.fieldnames is None:
        raise ValueError("stock_on_hand CSV has no header")
    rows = list(reader)
    affected = 0
    for row in rows:
        if row["snapshot_date"] == trading_date.isoformat() and affected < count:
            row["on_hand_units"] = "-12"
            affected += 1
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=reader.fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    blob.upload_bytes(name, stream.getvalue().encode(), "text/csv")
    blob.delete(f"{landing_prefix(trading_date)}/manifest.json")
    return {"failure": "phantom_stock", "affected_rows": affected}


def slow_cluster(seconds: int = 45) -> dict:
    if seconds < 0:
        raise ValueError("Delay must not be negative")
    set_config("ingest_delay_seconds", str(seconds))
    return {"failure": "slow_cluster", "ingest_delay_seconds": seconds}


def _remove_date_files(trading_date: date) -> list[str]:
    date_key = trading_date.strftime("%Y%m%d")
    removed: list[str] = []
    candidates = [
        Path("/wms/inbound/replen") / f"REPLEN_ORDER_{date_key}.csv",
        Path("/wms/ack") / f"REPLEN_ACK_{date_key}.txt",
        Path("/wms/reject") / f"REPLEN_REJECT_{date_key}.txt",
        Path("/wms/state") / f"REPLEN_ORDER_{date_key}.csv.json",
        settings.runtime_root / "outbound" / f"REPLEN_ORDER_{date_key}.csv",
        settings.runtime_root / "wms" / "ack" / f"REPLEN_ACK_{date_key}.txt",
        settings.runtime_root / "wms" / "reject" / f"REPLEN_REJECT_{date_key}.txt",
    ]
    for path in candidates:
        if path.exists():
            path.unlink()
            removed.append(str(path))
    return removed


def reset(trading_date: date) -> dict:
    reset_config()
    set_config("wms_mode", "never_ack")
    removed_files = _remove_date_files(trading_date)
    restored = 0
    snapshot = stock_snapshot_path(trading_date)
    if snapshot.is_file():
        content = snapshot.read_bytes()
        blob.upload_bytes(landing_name(trading_date, "stock_on_hand"), content, "text/csv")
        restored = max(0, content.count(b"\n") - 1)
        snapshot.unlink()

    blob.delete(f"{landing_prefix(trading_date)}/manifest.json")
    blob.delete(order_name(trading_date))
    generated = generate_asn(trading_date, force=True)
    try:
        released = release_eod(trading_date)
    except RuntimeError as exc:
        released = {"warning": str(exc)}
    set_config("wms_mode", "ack")
    return {
        "status": "GREEN_SOURCE_STATE",
        "stock_rows_restored": restored,
        "asn": generated,
        "eod": released,
        "wms_files_removed": removed_files,
        "next_step": "stage inputs and rerun the failed remote job",
    }
