#!/usr/bin/env python3
from __future__ import annotations

import configparser
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any


PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "retail-demo-azure")
CLUSTER_NAME = os.environ.get("DATABRICKS_CLUSTER_NAME", "retail-demo-dbt")
SPARK_VERSION = os.environ.get("DATABRICKS_SPARK_VERSION", "16.4.x-scala2.12")
NODE_TYPE_ID = os.environ.get("DATABRICKS_NODE_TYPE_ID", "Standard_D4as_v5")
AUTOTERMINATION_MINUTES = int(
    os.environ.get("DATABRICKS_AUTOTERMINATION_MINUTES", "20")
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = REPOSITORY_ROOT / "runtime" / "databricks" / "azure.json"


def _redact(message: str) -> str:
    message = re.sub(r"https?://[^\s]+", "[url-redacted]", message)
    message = re.sub(r"[\w.+-]+@[\w.-]+", "[identity-redacted]", message)
    return re.sub(r"\b[A-Za-z0-9_-]{24,}\b", "[identifier-redacted]", message)


def _run_cli(*arguments: str, expect_json: bool = True) -> Any:
    command = [
        "databricks",
        *arguments,
        "--profile",
        PROFILE,
        "--output",
        "json",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        detail = _redact((result.stderr or result.stdout).strip())
        raise RuntimeError(f"Databricks CLI command failed: {detail[:1000]}")
    if not expect_json or not result.stdout.strip():
        return None
    return json.loads(result.stdout)


def _items(payload: Any, key: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get(key, [])
    return []


def _profile_host(profile: str = PROFILE) -> str:
    config = configparser.ConfigParser()
    config.read(Path.home() / ".databrickscfg")
    if profile not in config:
        raise RuntimeError(
            f"Databricks profile {profile!r} is absent; run databricks auth login first."
        )
    host = config[profile].get("host", "").rstrip("/")
    if not host:
        raise RuntimeError(f"Databricks profile {profile!r} does not define a host.")
    return host


def _workspace_id(host: str) -> str:
    match = re.fullmatch(r"https://adb-(\d+)\.\d+\.azuredatabricks\.net", host)
    if not match:
        raise ValueError("The configured host is not an Azure Databricks workspace URL.")
    return match.group(1)


def _http_path(host: str, cluster_id: str) -> str:
    return f"/sql/protocolv1/o/{_workspace_id(host)}/{cluster_id}"


def _desired_configuration() -> dict[str, Any]:
    if AUTOTERMINATION_MINUTES < 10:
        raise ValueError("DATABRICKS_AUTOTERMINATION_MINUTES must be at least 10.")
    return {
        "cluster_name": CLUSTER_NAME,
        "spark_version": SPARK_VERSION,
        "node_type_id": NODE_TYPE_ID,
        "num_workers": 0,
        "autotermination_minutes": AUTOTERMINATION_MINUTES,
        "data_security_mode": "NONE",
        "runtime_engine": "STANDARD",
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        "custom_tags": {
            "Project": "retail-data-demo",
            "ResourceClass": "SingleNode",
        },
    }


def _configuration_drift(cluster: dict[str, Any]) -> bool:
    desired = _desired_configuration()
    scalar_keys = (
        "cluster_name",
        "node_type_id",
        "num_workers",
        "autotermination_minutes",
        "data_security_mode",
        "runtime_engine",
    )
    if cluster.get("spark_version") != SPARK_VERSION:
        return True
    if any(cluster.get(key) != desired[key] for key in scalar_keys):
        return True
    for key, value in desired["spark_conf"].items():
        if cluster.get("spark_conf", {}).get(key) != value:
            return True
    for key, value in desired["custom_tags"].items():
        if cluster.get("custom_tags", {}).get(key) != value:
            return True
    return False


def _find_cluster() -> dict[str, Any] | None:
    clusters = _items(_run_cli("clusters", "list"), "clusters")
    matches = [cluster for cluster in clusters if cluster.get("cluster_name") == CLUSTER_NAME]
    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple clusters are named {CLUSTER_NAME!r}; resolve the duplicate before provisioning."
        )
    return matches[0] if matches else None


def _create_cluster() -> dict[str, Any]:
    _run_cli(
        "clusters",
        "create",
        "--json",
        json.dumps(_desired_configuration(), separators=(",", ":")),
        "--timeout",
        "30m",
    )
    cluster = _find_cluster()
    if cluster is None:
        raise RuntimeError("Databricks reported success but the cluster was not found.")
    return cluster


def _update_cluster(cluster: dict[str, Any]) -> dict[str, Any]:
    cluster_id = str(cluster["cluster_id"])
    if _configuration_drift(cluster):
        configuration = {**_desired_configuration(), "cluster_id": cluster_id}
        _run_cli(
            "clusters",
            "edit",
            "--json",
            json.dumps(configuration, separators=(",", ":")),
            "--timeout",
            "30m",
        )
    current = _run_cli("clusters", "get", cluster_id)
    state = current.get("state")
    if state == "TERMINATED":
        _run_cli("clusters", "start", cluster_id, "--timeout", "30m")
        current = _run_cli("clusters", "get", cluster_id)
    elif state != "RUNNING":
        raise RuntimeError(f"Cluster is in unsupported provisioning state {state!r}.")
    return current


def _write_state(cluster: dict[str, Any]) -> None:
    host = _profile_host()
    state = {
        "profile": PROFILE,
        "cluster_id": cluster["cluster_id"],
        "http_path": _http_path(host, str(cluster["cluster_id"])),
        "metastore": "hive_metastore",
        "spark_version": cluster["spark_version"],
        "node_type_id": cluster["node_type_id"],
        "autotermination_minutes": cluster["autotermination_minutes"],
    }
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    STATE_PATH.chmod(0o600)


def main() -> None:
    _run_cli("current-user", "me")
    cluster = _find_cluster()
    cluster = _create_cluster() if cluster is None else _update_cluster(cluster)
    cluster = _run_cli("clusters", "get", str(cluster["cluster_id"]))
    if cluster.get("state") != "RUNNING":
        raise RuntimeError(f"Cluster did not reach RUNNING: {cluster.get('state')!r}.")
    _write_state(cluster)
    print(
        "Azure Databricks cluster is RUNNING; connection metadata was written "
        "to ignored runtime state."
    )


if __name__ == "__main__":
    main()
