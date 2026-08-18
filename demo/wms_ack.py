from __future__ import annotations

import os
import re
import signal
import time
from pathlib import Path

from demo.config import settings
from demo.state import get_config, write_json


running = True
ORDER_PATTERN = re.compile(r"REPLEN_ORDER_(\d{8})\.csv$")


def _stop(*_: object) -> None:
    global running
    running = False


def _claim(path: Path) -> Path | None:
    claims = Path("/wms/state")
    claims.mkdir(parents=True, exist_ok=True)
    claim = claims / f"{path.name}.json"
    try:
        descriptor = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return None
    os.close(descriptor)
    write_json(claim, {"filename": path.name, "status": "RECEIVED"})
    return claim


def _process(path: Path) -> None:
    match = ORDER_PATTERN.match(path.name)
    if not match:
        return
    claim = _claim(path)
    if claim is None:
        return
    date_key = match.group(1)
    mode = get_config("wms_mode", "ack") or "ack"
    if mode == "never_ack":
        write_json(claim, {"filename": path.name, "status": "NO_ACK"})
        return

    write_json(claim, {"filename": path.name, "status": "PROCESSING", "mode": mode})
    delay = int(get_config("wms_ack_delay_seconds", "2") or 2)
    if mode == "late":
        delay = max(delay, 30)
    time.sleep(delay)
    if not claim.exists() or (get_config("wms_mode", "ack") or "ack") != mode:
        return

    if mode == "reject":
        destination = Path("/wms/reject") / f"REPLEN_REJECT_{date_key}.txt"
        destination.write_text(f"REJECTED {path.name}: demo failure mode\n")
        runtime_destination = settings.runtime_root / "wms" / "reject" / destination.name
        status = "REJECTED"
    else:
        destination = Path("/wms/ack") / f"REPLEN_ACK_{date_key}.txt"
        destination.write_text(f"ACCEPTED {path.name}\n")
        runtime_destination = settings.runtime_root / "wms" / "ack" / destination.name
        status = "ACKNOWLEDGED"
    runtime_destination.parent.mkdir(parents=True, exist_ok=True)
    runtime_destination.write_text(destination.read_text())
    write_json(claim, {"filename": path.name, "status": status, "mode": mode})


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    inbound = Path("/wms/inbound/replen")
    for directory in (inbound, Path("/wms/ack"), Path("/wms/reject"), Path("/wms/state")):
        directory.mkdir(parents=True, exist_ok=True)
    while running:
        for path in sorted(inbound.glob("REPLEN_ORDER_*.csv")):
            _process(path)
        time.sleep(1)


if __name__ == "__main__":
    main()
