#!/usr/bin/env bash
set -euo pipefail

demo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

read_setting() {
  local name=${1:?setting name is required}
  python3 - "$demo_root/.env" "$name" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
name = sys.argv[2]
if not path.is_file():
    raise SystemExit(0)
for raw_line in path.read_text().splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    if key.strip() != name:
        continue
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    print(value)
    break
PY
}

databricks_host=${DATABRICKS_HOST:-$(read_setting DATABRICKS_HOST)}
databricks_profile=${DATABRICKS_CONFIG_PROFILE:-$(read_setting DATABRICKS_CONFIG_PROFILE)}
databricks_profile=${databricks_profile:-retail-demo-azure}

if [[ -z "$databricks_host" || "$databricks_host" == *'<'* ]]; then
  echo "DATABRICKS_HOST must be populated in the ignored .env before authentication." >&2
  exit 2
fi
if [[ ! "$databricks_host" =~ ^https://[^[:space:]]+$ ]]; then
  echo "DATABRICKS_HOST must be an https:// Azure Databricks workspace URL." >&2
  exit 2
fi

databricks_cli=$(command -v databricks || true)
if [[ -z "$databricks_cli" && -x "${HOME}/.local/bin/databricks" ]]; then
  databricks_cli="${HOME}/.local/bin/databricks"
fi
if [[ -z "$databricks_cli" ]]; then
  echo "Databricks CLI is not installed; run make install-databricks-cli first." >&2
  exit 2
fi

exec "$databricks_cli" auth login \
  --host "$databricks_host" \
  --profile "$databricks_profile"
