#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CONNECTION_PROFILE = os.environ.get("CTM_DBT_CONNECTION_PROFILE", "FMO_AZURE_DBT")
CONTROL_M_SERVER = os.environ.get("CTM_SERVER", "IN01")
CONTROL_M_AGENT = os.environ.get("CTM_AGENT_HOST", "fmo-azureuser")
CONTROL_M_ENVIRONMENT = os.environ.get("CTM_ENV", "se-dev")


def _dotenv() -> dict[str, str]:
    values: dict[str, str] = {}
    path = REPOSITORY_ROOT / ".env"
    if not path.is_file():
        return values
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def _required(name: str, values: dict[str, str]) -> str:
    value = os.environ.get(name) or values.get(name, "")
    if not value or "<" in value:
        raise RuntimeError(f"{name} is required to provision the Control-M dbt profile")
    return value


def _run(*arguments: str) -> str:
    result = subprocess.run(arguments, capture_output=True, text=True)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        detail = re.sub(r"dbtc_[A-Za-z0-9_-]+", "[dbt-token-redacted]", detail)
        detail = re.sub(r"dbtu_[A-Za-z0-9_-]+", "[dbt-token-redacted]", detail)
        raise RuntimeError(f"Control-M command failed: {detail[:1000]}")
    return result.stdout.strip()


def main() -> None:
    values = _dotenv()
    host = _required("DBT_CLOUD_HOST", values).rstrip("/")
    if not host.startswith(("https://", "http://")):
        host = f"https://{host}"
    definition = {
        CONNECTION_PROFILE: {
            "Type": "ConnectionProfile:DBT",
            "DBT URL": host,
            "DBT Token": _required("DBT_CLOUD_SERVICE_TOKEN", values),
            "Account ID": _required("DBT_CLOUD_ACCOUNT_ID", values),
            "Connection Timeout": "60",
            "Description": "Retail demo dbt Cloud service-token profile",
            "Centralized": True,
        }
    }
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="retail-dbt-profile-", delete=False
    ) as handle:
        json.dump(definition, handle)
        handle.write("\n")
        path = Path(handle.name)
    path.chmod(0o600)
    try:
        _run("ctm", "deploy", str(path), "-e", CONTROL_M_ENVIRONMENT)
    finally:
        path.unlink(missing_ok=True)

    try:
        _run(
            "ctm",
            "deploy",
            "connectionprofile:centralized::test",
            "DBT",
            CONNECTION_PROFILE,
            CONTROL_M_SERVER,
            CONTROL_M_AGENT,
            "-e",
            CONTROL_M_ENVIRONMENT,
        )
        test_result = "passed its agent connection test"
    except RuntimeError:
        log_root = Path.home() / "ctmag" / "ctm" / "proclog"
        unsupported = any(
            "cant find any operation for Connection profile test"
            in path.read_text(errors="replace")
            for path in log_root.glob("AI_Ctmcm_validate_account_request*.log")
        )
        if not unsupported:
            raise
        test_result = (
            "was synchronized; installed dbt plug-in 1.0.01 does not implement "
            "the connection-profile test operation, so the first job run is the "
            "connection test"
        )
    print(f"Control-M dbt connection profile {CONNECTION_PROFILE} {test_result}.")


if __name__ == "__main__":
    main()
