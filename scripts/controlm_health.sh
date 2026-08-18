#!/usr/bin/env bash
set -euo pipefail

pgrep -u azureuser -x p_ctmag >/dev/null 2>&1 || {
  echo "Control-M Agent is not running for azureuser" >&2
  exit 1
}

echo "Control-M Agent: running"
