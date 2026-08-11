#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
from datetime import date
from pathlib import Path

from sync_silver import REPOSITORY_ROOT, STATE_PATH, _load_json, _run


NOTEBOOK_SOURCE = REPOSITORY_ROOT / "databricks" / "notebooks" / "04_export_replenishment.py"
WORKSPACE_NOTEBOOK = "/Shared/retail-data-demo/04_export_replenishment"
OUTBOUND_ROOT = REPOSITORY_ROOT / "runtime" / "outbound"


def export_replenishment(trading_date: date) -> Path:
    state = _load_json(STATE_PATH)
    profile = str(state.get("profile", ""))
    cluster_id = str(state.get("cluster_id", ""))
    if not profile or not cluster_id:
        raise ValueError("Generated Azure state is missing its profile or cluster identifier")

    _run(profile, "current-user", "me")
    _run(profile, "workspace", "mkdirs", "/Shared/retail-data-demo")
    _run(
        profile,
        "workspace",
        "import",
        WORKSPACE_NOTEBOOK,
        "--file",
        str(NOTEBOOK_SOURCE),
        "--format",
        "SOURCE",
        "--language",
        "PYTHON",
        "--overwrite",
    )

    request = {
        "run_name": f"retail-demo-export-replenishment-{trading_date:%Y%m%d}",
        "tasks": [
            {
                "task_key": "export_replenishment",
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": WORKSPACE_NOTEBOOK,
                    "base_parameters": {"trading_date": trading_date.isoformat()},
                },
                "timeout_seconds": 3600,
            }
        ],
    }
    _run(
        profile,
        "jobs",
        "submit",
        "--json",
        json.dumps(request, separators=(",", ":")),
        "--timeout",
        "60m",
    )

    date_key = trading_date.strftime("%Y%m%d")
    destination = OUTBOUND_ROOT / f"REPLEN_ORDER_{date_key}.csv"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{destination.name}.", suffix=".download", dir=destination.parent,
        delete=False,
    ) as handle:
        download = Path(handle.name)
    try:
        _run(
            profile,
            "fs",
            "cp",
            f"dbfs:/tmp/retail-data-demo/{date_key}/{destination.name}",
            str(download),
            "--overwrite",
        )
        if download.stat().st_size == 0:
            raise RuntimeError("Databricks did not produce the expected replenishment CSV")
        download.replace(destination)
    finally:
        download.unlink(missing_ok=True)
    print(f"Azure Databricks gold order downloaded to {destination.relative_to(REPOSITORY_ROOT)}.")
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export the tested Azure Databricks gold order for WMS delivery"
    )
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    export_replenishment(parser.parse_args().date)


if __name__ == "__main__":
    main()
