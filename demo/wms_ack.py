from __future__ import annotations

import re
import signal
import time
from pathlib import Path

from demo.db import connect, get_config


running = True
ORDER_PATTERN = re.compile(r"REPLEN_ORDER_(\d{8})\.csv$")


def _stop(*_: object) -> None:
    global running
    running = False


def _process(path: Path) -> None:
    match = ORDER_PATTERN.match(path.name)
    if not match:
        return
    date_key = match.group(1)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.wms_deliveries(filename, status)
            VALUES (%s, 'RECEIVED') ON CONFLICT (filename) DO NOTHING
            """,
            (path.name,),
        )
        cur.execute("SELECT status FROM meta.wms_deliveries WHERE filename=%s", (path.name,))
        status = cur.fetchone()["status"]
    mode = get_config("wms_mode", "ack") or "ack"
    if status != "RECEIVED":
        return
    if mode == "never_ack":
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE meta.wms_deliveries SET status='NO_ACK' "
                "WHERE filename=%s AND status='RECEIVED'",
                (path.name,),
            )
        return
    # Claim the file before sleeping. This prevents the polling loop from
    # starting multiple acknowledgements for the same delivery.
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE meta.wms_deliveries SET status='PROCESSING'
            WHERE filename=%s AND status='RECEIVED'
            RETURNING filename
            """,
            (path.name,),
        )
        if not cur.fetchone():
            return
    delay = int(get_config("wms_ack_delay_seconds", "2") or 2)
    if mode == "late":
        delay = max(delay, 30)
    time.sleep(delay)
    # Reset deletes this row and temporarily switches to never_ack. Re-check
    # after the delay so a cancelled rehearsal cannot emit a stale ACK/REJECT.
    current_mode = get_config("wms_mode", "ack") or "ack"
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM meta.wms_deliveries WHERE filename=%s",
            (path.name,),
        )
        delivery = cur.fetchone()
    if not delivery or delivery["status"] != "PROCESSING" or current_mode != mode:
        return
    if mode == "reject":
        destination = Path("/wms/reject") / f"REPLEN_REJECT_{date_key}.txt"
        destination.write_text(f"REJECTED {path.name}: demo failure mode\n")
        runtime_destination = Path("/workspace/runtime/wms/reject") / destination.name
        new_status = "REJECTED"
    else:
        destination = Path("/wms/ack") / f"REPLEN_ACK_{date_key}.txt"
        destination.write_text(f"ACCEPTED {path.name}\n")
        runtime_destination = Path("/workspace/runtime/wms/ack") / destination.name
        new_status = "ACKNOWLEDGED"
    runtime_destination.parent.mkdir(parents=True, exist_ok=True)
    runtime_destination.write_text(destination.read_text())
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE meta.wms_deliveries
            SET status=%s, acknowledged_at=now()
            WHERE filename=%s AND status='PROCESSING'
            """,
            (new_status, path.name),
        )


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    inbound = Path("/wms/inbound/replen")
    for directory in (inbound, Path("/wms/ack"), Path("/wms/reject")):
        directory.mkdir(parents=True, exist_ok=True)
    while running:
        for path in sorted(inbound.glob("REPLEN_ORDER_*.csv")):
            _process(path)
        time.sleep(1)


if __name__ == "__main__":
    main()
