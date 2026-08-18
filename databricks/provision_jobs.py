#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import provision_cluster as base


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = REPOSITORY_ROOT / "runtime" / "databricks" / "azure.json"
WORKSPACE_ROOT = "/Shared/retail-data-demo"
JOBS = {
    "ingest": {
        "name": "Retail Demo Ingest Bronze",
        "task_key": "ingest_bronze",
        "source": REPOSITORY_ROOT / "databricks" / "notebooks" / "00_ingest_bronze.py",
        "workspace_path": f"{WORKSPACE_ROOT}/00_ingest_bronze",
        "parameters": ["trading_date", "landing_path"],
    },
    "export": {
        "name": "Retail Demo Export Replenishment",
        "task_key": "export_replenishment",
        "source": REPOSITORY_ROOT / "databricks" / "notebooks" / "04_export_replenishment.py",
        "workspace_path": f"{WORKSPACE_ROOT}/04_export_replenishment",
        "parameters": ["trading_date", "outbound_path"],
    },
}


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.is_file():
        raise RuntimeError("Azure Databricks state is missing; provision the cluster first")
    state = json.loads(STATE_PATH.read_text())
    if not isinstance(state, dict) or not state.get("cluster_id"):
        raise ValueError("Azure Databricks state does not contain a cluster identifier")
    return state


def _import_notebook(definition: dict[str, Any]) -> None:
    base._run_cli("workspace", "mkdirs", WORKSPACE_ROOT)
    base._run_cli(
        "workspace",
        "import",
        str(definition["workspace_path"]),
        "--file",
        str(definition["source"]),
        "--format",
        "SOURCE",
        "--language",
        "PYTHON",
        "--overwrite",
    )


def _settings(definition: dict[str, Any], cluster_id: str) -> dict[str, Any]:
    return {
        "name": definition["name"],
        "max_concurrent_runs": 1,
        "parameters": [
            {"name": name, "default": ""} for name in definition["parameters"]
        ],
        "tasks": [
            {
                "task_key": definition["task_key"],
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": definition["workspace_path"],
                    "base_parameters": {
                        name: f"{{{{job.parameters.{name}}}}}"
                        for name in definition["parameters"]
                    },
                },
                "timeout_seconds": 3600,
            }
        ],
    }


def _existing_jobs() -> list[dict[str, Any]]:
    payload = base._run_cli("jobs", "list", "--limit", "100")
    return base._items(payload, "jobs")


def main() -> None:
    state = _load_state()
    base._run_cli("current-user", "me")
    jobs = _existing_jobs()
    job_ids: dict[str, int] = {}
    actions: dict[str, str] = {}
    for key, definition in JOBS.items():
        _import_notebook(definition)
        matches = [job for job in jobs if job.get("settings", {}).get("name") == definition["name"]]
        if len(matches) > 1:
            raise RuntimeError(f"Multiple Databricks jobs are named {definition['name']!r}")
        settings = _settings(definition, str(state["cluster_id"]))
        if matches:
            job_id = int(matches[0]["job_id"])
            base._run_cli(
                "jobs", "reset", "--json", json.dumps({"job_id": job_id, "new_settings": settings})
            )
            actions[key] = "updated"
        else:
            created = base._run_cli("jobs", "create", "--json", json.dumps(settings))
            job_id = int(created["job_id"])
            actions[key] = "created"
        job_ids[key] = job_id

    state["job_ids"] = job_ids
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    STATE_PATH.chmod(0o644)
    print(f"Azure Databricks jobs ready: {actions}")


if __name__ == "__main__":
    main()
