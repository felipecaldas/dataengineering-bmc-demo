#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import date
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = REPOSITORY_ROOT / "runtime" / "databricks" / "azure.json"
EXPORT_ROOT = REPOSITORY_ROOT / "runtime" / "databricks" / "export"
NOTEBOOK_SOURCE = REPOSITORY_ROOT / "databricks" / "notebooks" / "00_load_silver.py"
WORKSPACE_NOTEBOOK = "/Shared/retail-data-demo/00_load_silver"
EXPECTED_TABLES = {
    "product_master",
    "pos_transactions",
    "store_eod",
    "asn_inbound",
    "stock_on_hand",
    "sales_history",
}


def _redact(message: str) -> str:
    message = re.sub(r"https?://[^\s]+", "[url-redacted]", message)
    message = re.sub(r"[\w.+-]+@[\w.-]+", "[identity-redacted]", message)
    return re.sub(r"\b[A-Za-z0-9_-]{24,}\b", "[identifier-redacted]", message)


def _run(profile: str, *arguments: str) -> str:
    command = ["databricks", *arguments, "--profile", profile, "--output", "json"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        detail = _redact((result.stderr or result.stdout).strip())
        raise RuntimeError(f"Databricks CLI command failed: {detail[:1000]}")
    return result.stdout


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"Required generated state is missing: {path.relative_to(REPOSITORY_ROOT)}")
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path.relative_to(REPOSITORY_ROOT)}")
    return value


def sync_silver(trading_date: date) -> None:
    state = _load_json(STATE_PATH)
    profile = str(state.get("profile", ""))
    cluster_id = str(state.get("cluster_id", ""))
    if not profile or not cluster_id:
        raise ValueError("Generated Azure state is missing its profile or cluster identifier")

    export_directory = EXPORT_ROOT / trading_date.strftime("%Y%m%d")
    manifest = _load_json(export_directory / "manifest.json")
    if manifest.get("trading_date") != trading_date.isoformat():
        raise ValueError("Export manifest trading date does not match the requested sync date")
    if set(manifest.get("tables", {})) != EXPECTED_TABLES:
        raise ValueError("Export manifest does not contain the six required silver tables")

    required_files = [export_directory / f"{table}.csv" for table in sorted(EXPECTED_TABLES)]
    required_files.append(export_directory / "manifest.json")
    missing = [path.name for path in required_files if not path.is_file()]
    if missing:
        raise RuntimeError(f"Databricks export is incomplete; missing files: {missing}")

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

    dbfs_base = f"dbfs:/tmp/retail-data-demo/{trading_date:%Y%m%d}"
    _run(profile, "fs", "mkdir", dbfs_base)
    for path in required_files:
        _run(
            profile,
            "fs",
            "cp",
            str(path),
            f"{dbfs_base}/{path.name}",
            "--overwrite",
        )

    request = {
        "run_name": f"retail-demo-load-silver-{trading_date:%Y%m%d}",
        "tasks": [
            {
                "task_key": "load_silver",
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": WORKSPACE_NOTEBOOK,
                    "base_parameters": {
                        "trading_date": trading_date.isoformat(),
                        "base_path": dbfs_base,
                    },
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
    print(
        f"Validated silver inputs for {trading_date} were loaded into Azure "
        "Databricks Delta tables."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync validated silver inputs to Azure Databricks")
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    sync_silver(args.date)


if __name__ == "__main__":
    main()
