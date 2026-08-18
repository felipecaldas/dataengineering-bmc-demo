#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import provision_cloud_databricks as base


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CLOUD_STATE_PATH = REPOSITORY_ROOT / "runtime" / "dbt_cloud" / "azure.json"
PROJECT_SUBDIRECTORY = "dbt/kmart_retail"
DOTENV = base._dotenv_values(REPOSITORY_ROOT / ".env")


def _setting(name: str, default: str) -> str:
    return os.environ.get(name) or DOTENV.get(name, default)


DEPLOYMENT_ENVIRONMENT_NAME = os.environ.get(
    "DBT_CLOUD_DEPLOYMENT_ENVIRONMENT_NAME"
) or DOTENV.get(
    "DBT_CLOUD_DEPLOYMENT_ENVIRONMENT_NAME", "Azure Databricks Shared"
)
DEPLOYMENT_BRANCH = _setting("DBT_CLOUD_DEPLOYMENT_BRANCH", "demo/dbt-cloud-databricks")
CONTROL_M_CONNECTION_PROFILE = _setting("CTM_DBT_CONNECTION_PROFILE", "FMO_AZURE_DBT")
DEFAULT_TRADING_DATE = _setting("DEMO_TRADING_DATE", "2026-08-14")
JOB_DEFINITIONS = {
    "stage": {
        "name": "Retail Demo Stage",
        "description": "Build and test the dbt staging models in Azure Databricks Silver.",
        "selector": "tag:stage",
        "generate_docs": False,
    },
    "intermediate": {
        "name": "Retail Demo Intermediate",
        "description": "Build and test the dbt intermediate models in Azure Databricks Silver.",
        "selector": "tag:intermediate",
        "generate_docs": False,
    },
    "gold": {
        "name": "Retail Demo Gold",
        "description": "Build and test the dbt marts layer on Azure Databricks.",
        "selector": "tag:gold",
        "generate_docs": True,
    },
}


def _dotenv() -> dict[str, str]:
    return base._dotenv_values(REPOSITORY_ROOT / ".env")


def _required_databricks_token() -> str:
    token = os.environ.get("DATABRICKS_TOKEN") or _dotenv().get("DATABRICKS_TOKEN", "")
    if not token or "<" in token:
        raise RuntimeError(
            "DATABRICKS_TOKEN must contain a Databricks PAT for the dbt Cloud "
            "deployment credential"
        )
    return token


def _credential_details(token: str) -> dict[str, Any]:
    def field(
        label: str,
        value: str,
        *,
        encrypted: bool = False,
        required: bool = False,
        description: str = "",
        depends_on: dict[str, list[str]] | None = None,
        options: list[dict[str, str]] | None = None,
        field_type: str = "text",
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "label": label,
            "description": description,
            "field_type": field_type,
            "encrypt": encrypted,
            "overrideable": False,
            "validation": {"required": required},
        }
        if depends_on:
            metadata["depends_on"] = depends_on
        if options:
            metadata["options"] = options
            metadata["is_searchable"] = False
        return {"metadata": metadata, "value": value}

    return {
        "fields": {
            "auth_type": field(
                "Auth method",
                "token",
                required=True,
                field_type="select",
                options=[
                    {"label": "Token", "value": "token"},
                    {"label": "OAuth", "value": "oauth"},
                ],
            ),
            "token": field(
                "Token",
                token,
                encrypted=True,
                required=True,
                description="Personalized user token.",
                depends_on={"auth_type": ["token"]},
            ),
            "schema": field("Schema", "gold", required=True, description="User schema."),
            "target_name": field("Target Name", "default"),
            "catalog": field(
                "Catalog",
                "",
                description="Catalog name when Unity Catalog is enabled.",
            ),
        },
        "field_order": [],
    }


def _ensure_project_subdirectory(
    client: base.CloudClient, project_id: int
) -> str:
    path = f"/api/v3/accounts/{client.account_id}/projects/{project_id}/"
    project = base._data(client.request("GET", path))
    if project.get("dbt_project_subdirectory") == PROJECT_SUBDIRECTORY:
        return "unchanged"
    payload = {
        key: project.get(key)
        for key in (
            "id",
            "name",
            "description",
            "type",
            "state",
            "account_id",
            "connection_id",
            "repository_id",
        )
    }
    payload["dbt_project_subdirectory"] = PROJECT_SUBDIRECTORY
    client.request("POST", path, payload)
    return "updated"


def _credential_id_from_state(client: base.CloudClient, project_id: int) -> int | None:
    if not CLOUD_STATE_PATH.is_file():
        return None
    state = json.loads(CLOUD_STATE_PATH.read_text())
    value = state.get("deployment_credential_id")
    if not value:
        return None
    path = (
        f"/api/v3/accounts/{client.account_id}/projects/{project_id}/"
        f"credentials/{int(value)}/"
    )
    try:
        credential = base._data(client.request("GET", path))
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return None
        raise
    if credential.get("adapter_version") != "databricks_v0":
        raise RuntimeError("Stored dbt deployment credential is not Databricks")
    return int(value)


def _ensure_deployment_credential(
    client: base.CloudClient, project_id: int, token: str
) -> tuple[int, str]:
    credential_id = _credential_id_from_state(client, project_id)
    details = _credential_details(token)
    base_path = (
        f"/api/v3/accounts/{client.account_id}/projects/{project_id}/credentials/"
    )
    if credential_id is None:
        credentials = base._data(client.request("GET", base_path))
        matches = [
            item
            for item in credentials
            if item.get("adapter_version") == "databricks_v0"
            and item.get("unencrypted_credential_details", {}).get("schema") == "gold"
        ]
        if len(matches) > 1:
            raise RuntimeError("Multiple Databricks deployment credentials use schema 'gold'")
        if matches:
            credential_id = int(matches[0]["id"])

    if credential_id is None:
        payload = {
            "account_id": client.account_id,
            "project_id": project_id,
            "type": "adapter",
            "adapter_version": "databricks_v0",
            "state": 1,
            "threads": 4,
            "credential_details": details,
        }
        created = base._data(client.request("POST", base_path, payload))
        return int(created["id"]), "created"

    client.request(
        "PATCH",
        f"{base_path}{credential_id}/",
        {"id": credential_id, "credential_details": details},
    )
    return credential_id, "updated"


def _ensure_deployment_environment(
    client: base.CloudClient,
    project_id: int,
    connection_id: int,
    credential_id: int,
) -> tuple[int, str]:
    base_path = (
        f"/api/v3/accounts/{client.account_id}/projects/{project_id}/environments/"
    )
    environments = base._data(client.request("GET", f"{base_path}?limit=100"))
    matches = [
        environment
        for environment in environments
        if environment.get("name") == DEPLOYMENT_ENVIRONMENT_NAME
    ]
    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple dbt environments are named {DEPLOYMENT_ENVIRONMENT_NAME!r}"
        )
    desired = {
        "state": 1,
        "account_id": client.account_id,
        "project_id": project_id,
        "name": DEPLOYMENT_ENVIRONMENT_NAME,
        "dbt_version": base.DBT_VERSION,
        "type": "deployment",
        "deployment_type": "staging",
        "use_custom_branch": True,
        "custom_branch": DEPLOYMENT_BRANCH,
        "credentials_id": credential_id,
        "connection_id": connection_id,
    }
    if not matches:
        created = base._data(client.request("POST", base_path, desired))
        return int(created["id"]), "created"

    environment_id = int(matches[0]["id"])
    detail = base._data(client.request("GET", f"{base_path}{environment_id}/"))
    if detail.get("type") != "deployment":
        raise RuntimeError(
            f"dbt environment {DEPLOYMENT_ENVIRONMENT_NAME!r} is not deployment"
        )
    drift_keys = (
        "state",
        "dbt_version",
        "deployment_type",
        "use_custom_branch",
        "custom_branch",
        "credentials_id",
        "connection_id",
    )
    if any(detail.get(key) != desired[key] for key in drift_keys):
        client.request("PATCH", f"{base_path}{environment_id}/", desired)
        return environment_id, "updated"
    return environment_id, "unchanged"


def _job_command(selector: str, trading_date: str = DEFAULT_TRADING_DATE) -> str:
    return (
        f'dbt build --select {selector} '
        f'--vars "{{trading_date: \'{trading_date}\'}}"'
    )


def _job_payload(
    client: base.CloudClient,
    project_id: int,
    environment_id: int,
    definition: dict[str, Any],
) -> dict[str, Any]:
    return {
        "account_id": client.account_id,
        "project_id": project_id,
        "environment_id": environment_id,
        "name": definition["name"],
        "description": definition["description"],
        "execute_steps": [_job_command(str(definition["selector"]))],
        "state": 1,
        "triggers": {
            "github_webhook": False,
            "git_provider_webhook": False,
            "schedule": False,
            "on_merge": False,
        },
        "settings": {"threads": 4, "target_name": "default"},
        "schedule": {
            "date": {"type": "days_of_week", "days": [0, 1, 2, 3, 4, 5, 6]},
            "time": {"type": "every_hour", "interval": 1},
        },
        "generate_docs": bool(definition["generate_docs"]),
        "run_generate_sources": False,
        "execution": {"timeout_seconds": 3600},
    }


def _ensure_jobs(
    client: base.CloudClient, project_id: int, environment_id: int
) -> tuple[dict[str, int], dict[str, str]]:
    path = f"/api/v2/accounts/{client.account_id}/jobs/"
    jobs = base._data(client.request("GET", f"{path}?project_id={project_id}&limit=100"))
    ids: dict[str, int] = {}
    actions: dict[str, str] = {}
    for layer, definition in JOB_DEFINITIONS.items():
        matches = [job for job in jobs if job.get("name") == definition["name"]]
        if len(matches) > 1:
            raise RuntimeError(f"Multiple dbt jobs are named {definition['name']!r}")
        payload = _job_payload(client, project_id, environment_id, definition)
        if not matches:
            created = base._data(client.request("POST", path, payload))
            ids[layer] = int(created["id"])
            actions[layer] = "created"
            continue
        job_id = int(matches[0]["id"])
        detail = base._data(client.request("GET", f"{path}{job_id}/"))
        drift_keys = (
            "environment_id",
            "description",
            "execute_steps",
            "state",
            "triggers",
            "settings",
            "generate_docs",
            "run_generate_sources",
            "execution",
        )
        if any(detail.get(key) != payload[key] for key in drift_keys):
            payload["id"] = job_id
            base._data(client.request("POST", f"{path}{job_id}/", payload))
            actions[layer] = "updated"
        else:
            actions[layer] = "unchanged"
        ids[layer] = job_id
    return ids, actions


def main() -> None:
    base.main()
    state = json.loads(CLOUD_STATE_PATH.read_text())
    cloud = base._cloud_configuration()
    project_id = int(state["project_id"])
    client = base.CloudClient(
        str(cloud["account-host"]),
        int(cloud["account-id"]),
        str(cloud["token-value"]),
    )
    project_action = _ensure_project_subdirectory(client, project_id)
    credential_id, credential_action = _ensure_deployment_credential(
        client, project_id, _required_databricks_token()
    )
    environment_id, environment_action = _ensure_deployment_environment(
        client,
        project_id,
        int(state["connection_id"]),
        credential_id,
    )
    job_ids, job_actions = _ensure_jobs(client, project_id, environment_id)

    state.update(
        {
            "controlm_connection_profile": CONTROL_M_CONNECTION_PROFILE,
            "deployment_branch": DEPLOYMENT_BRANCH,
            "deployment_credential_id": credential_id,
            "deployment_environment_id": environment_id,
            "deployment_environment_name": DEPLOYMENT_ENVIRONMENT_NAME,
            "job_ids": job_ids,
        }
    )
    CLOUD_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    CLOUD_STATE_PATH.chmod(0o644)
    print(
        "Shared dbt Cloud resources ready; "
        f"project subdirectory {project_action}; credential {credential_action}; "
        f"deployment environment {environment_action}; jobs {job_actions}."
    )


if __name__ == "__main__":
    main()
