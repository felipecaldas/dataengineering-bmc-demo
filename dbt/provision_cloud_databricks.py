#!/usr/bin/env python3
from __future__ import annotations

import configparser
import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
AZURE_STATE_PATH = REPOSITORY_ROOT / "runtime" / "databricks" / "azure.json"
CLOUD_STATE_PATH = REPOSITORY_ROOT / "runtime" / "dbt_cloud" / "azure.json"
CONNECTION_NAME = os.environ.get(
    "DBT_CLOUD_DATABRICKS_CONNECTION_NAME", "Retail demo Azure Databricks"
)
ENVIRONMENT_NAME = os.environ.get(
    "DBT_CLOUD_DATABRICKS_ENVIRONMENT_NAME", "Azure Databricks Development"
)
DBT_VERSION = os.environ.get("DBT_CLOUD_DBT_VERSION", "latest")


def _redact(message: str) -> str:
    message = re.sub(r"dbtu_[A-Za-z0-9_-]+", "[dbt-token-redacted]", message)
    message = re.sub(r"https?://[^\s]+", "[url-redacted]", message)
    return re.sub(r"\b[A-Za-z0-9_-]{24,}\b", "[identifier-redacted]", message)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"Required generated state is missing: {path.relative_to(REPOSITORY_ROOT)}")
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path.relative_to(REPOSITORY_ROOT)}")
    return value


def _dotenv_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _cloud_configuration() -> dict[str, Any]:
    dotenv = _dotenv_values(REPOSITORY_ROOT / ".env")
    project_config = yaml.safe_load(
        (REPOSITORY_ROOT / "dbt" / "kmart_retail" / "dbt_project.yml").read_text()
    )
    configured_project_id = str(
        project_config.get("dbt-cloud", {}).get("project-id", "")
    )
    dotenv_project_id = os.environ.get("DBT_CLOUD_PROJECT_ID") or dotenv.get(
        "DBT_CLOUD_PROJECT_ID", ""
    )
    service_project_id = (
        dotenv_project_id if re.fullmatch(r"\d+", dotenv_project_id) else configured_project_id
    )
    service_fields = {
        "project-id": service_project_id,
        "account-id": os.environ.get("DBT_CLOUD_ACCOUNT_ID")
        or dotenv.get("DBT_CLOUD_ACCOUNT_ID"),
        "account-host": os.environ.get("DBT_CLOUD_HOST")
        or dotenv.get("DBT_CLOUD_HOST"),
        "token-value": os.environ.get("DBT_CLOUD_SERVICE_TOKEN")
        or dotenv.get("DBT_CLOUD_SERVICE_TOKEN"),
    }
    service_values_are_valid = (
        bool(re.fullmatch(r"\d+", str(service_fields["project-id"] or "")))
        and bool(re.fullmatch(r"\d+", str(service_fields["account-id"] or "")))
        and bool(service_fields["account-host"])
        and "<" not in str(service_fields["account-host"])
        and bool(service_fields["token-value"])
        and "<" not in str(service_fields["token-value"])
    )
    if service_values_are_valid:
        return {**service_fields, "token-source": "service"}

    candidates = (
        Path.home() / ".dbt" / "dbt_cloud.yml",
        REPOSITORY_ROOT / "dbt" / "dbt_cloud.yml",
    )
    config_path = next((path for path in candidates if path.is_file()), None)
    if config_path is None:
        raise RuntimeError("dbt Cloud CLI configuration was not found; authenticate dbt first")
    config = yaml.safe_load(config_path.read_text()) or {}
    active_project = str(config.get("context", {}).get("active-project", ""))
    projects = config.get("projects", [])
    project = next(
        (item for item in projects if str(item.get("project-id", "")) == active_project),
        None,
    )
    if not project:
        raise RuntimeError("The active dbt Cloud project is absent from the CLI configuration")
    required = ("project-id", "account-id", "account-host", "token-value")
    missing = [key for key in required if not project.get(key)]
    if missing:
        raise RuntimeError(f"dbt Cloud CLI configuration is missing required fields: {missing}")
    return {**project, "token-source": "personal-cli"}


def _databricks_host(profile: str) -> str:
    config = configparser.ConfigParser()
    config.read(Path.home() / ".databrickscfg")
    if profile not in config:
        raise RuntimeError(f"Databricks authentication profile {profile!r} is missing")
    host = config[profile].get("host", "").rstrip("/")
    if not host:
        raise RuntimeError(f"Databricks authentication profile {profile!r} has no host")
    return host.removeprefix("https://").removeprefix("http://")


class CloudClient:
    def __init__(self, host: str, account_id: int, token: str) -> None:
        normalized_host = host.strip().rstrip("/")
        normalized_host = normalized_host.removeprefix("https://").removeprefix("http://")
        self.base_url = f"https://{normalized_host}"
        self.account_id = account_id
        self.token = token

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={
                "Authorization": f"Token {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "retail-data-demo/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                value = json.loads(response.read())
        except urllib.error.HTTPError as exc:
            detail = _redact(exc.read().decode(errors="replace"))
            raise RuntimeError(
                f"dbt Cloud API {method} failed with HTTP {exc.code}: {detail[:1000]}"
            ) from exc
        if not isinstance(value, dict):
            raise RuntimeError("dbt Cloud API returned an unexpected response")
        return value


def _data(response: dict[str, Any]) -> Any:
    if "data" not in response:
        raise RuntimeError("dbt Cloud API response did not contain data")
    return response["data"]


def _ensure_connection(
    client: CloudClient, databricks_host: str, http_path: str
) -> tuple[int, str]:
    account_id = client.account_id
    response = client.request("GET", f"/api/v3/accounts/{account_id}/connections/")
    connections = _data(response)
    if not isinstance(connections, list):
        raise RuntimeError("dbt Cloud connection listing was not a list")
    matches = [connection for connection in connections if connection.get("name") == CONNECTION_NAME]
    if len(matches) > 1:
        raise RuntimeError(f"Multiple dbt Cloud connections are named {CONNECTION_NAME!r}")

    desired_config = {"host": databricks_host, "http_path": http_path}
    if not matches:
        payload = {
            "account_id": account_id,
            "name": CONNECTION_NAME,
            "adapter_version": "databricks_v0",
            "config": desired_config,
        }
        created = _data(
            client.request("POST", f"/api/v3/accounts/{account_id}/connections/", payload)
        )
        return int(created["id"]), "created"

    connection_id = int(matches[0]["id"])
    detail = _data(
        client.request("GET", f"/api/v3/accounts/{account_id}/connections/{connection_id}/")
    )
    if detail.get("adapter_version") != "databricks_v0":
        raise RuntimeError(f"dbt Cloud connection {CONNECTION_NAME!r} is not Databricks")
    current_config = detail.get("config", {})
    if any(current_config.get(key) != value for key, value in desired_config.items()):
        payload = {
            "account_id": account_id,
            "name": CONNECTION_NAME,
            "config": desired_config,
        }
        client.request(
            "PATCH", f"/api/v3/accounts/{account_id}/connections/{connection_id}/", payload
        )
        return connection_id, "updated"
    return connection_id, "unchanged"


def _ensure_environment(
    client: CloudClient, project_id: int, connection_id: int
) -> tuple[int, str]:
    account_id = client.account_id
    base_path = f"/api/v3/accounts/{account_id}/projects/{project_id}/environments/"
    response = client.request("GET", f"{base_path}?limit=100")
    environments = _data(response)
    if not isinstance(environments, list):
        raise RuntimeError("dbt Cloud environment listing was not a list")
    matches = [environment for environment in environments if environment.get("name") == ENVIRONMENT_NAME]
    if len(matches) > 1:
        raise RuntimeError(f"Multiple dbt Cloud environments are named {ENVIRONMENT_NAME!r}")

    desired = {
        "state": 1,
        "account_id": account_id,
        "project_id": project_id,
        "name": ENVIRONMENT_NAME,
        "dbt_version": DBT_VERSION,
        "type": "development",
        "use_custom_branch": False,
        "connection_id": connection_id,
    }
    if not matches:
        created = _data(client.request("POST", base_path, desired))
        return int(created["id"]), "created"

    environment_id = int(matches[0]["id"])
    detail = _data(client.request("GET", f"{base_path}{environment_id}/"))
    if detail.get("type") != "development":
        raise RuntimeError(f"dbt Cloud environment {ENVIRONMENT_NAME!r} is not development")
    drift_keys = ("connection_id", "dbt_version", "state")
    if any(detail.get(key) != desired[key] for key in drift_keys):
        client.request("PATCH", f"{base_path}{environment_id}/", desired)
        return environment_id, "updated"
    return environment_id, "unchanged"


def main() -> None:
    azure_state = _load_json(AZURE_STATE_PATH)
    cloud = _cloud_configuration()
    project_id = int(cloud["project-id"])
    account_id = int(cloud["account-id"])
    client = CloudClient(str(cloud["account-host"]), account_id, str(cloud["token-value"]))
    host = _databricks_host(str(azure_state["profile"]))

    connection_id, connection_action = _ensure_connection(
        client, host, str(azure_state["http_path"])
    )
    environment_id, environment_action = _ensure_environment(
        client, project_id, connection_id
    )

    state = {
        "connection_id": connection_id,
        "connection_name": CONNECTION_NAME,
        "environment_id": environment_id,
        "environment_name": ENVIRONMENT_NAME,
        "project_id": project_id,
    }
    CLOUD_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CLOUD_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    CLOUD_STATE_PATH.chmod(0o644)
    print(
        f"dbt Cloud Databricks connection {connection_action}; "
        f"development environment {environment_action}; "
        f"authenticated with {cloud['token-source']} token."
    )


if __name__ == "__main__":
    main()
