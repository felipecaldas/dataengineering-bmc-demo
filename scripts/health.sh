#!/usr/bin/env bash
set -euo pipefail

cd /home/azureuser/retail-data-demo

docker compose ps
docker compose run --rm toolbox python -m demo.cli health

airflow_status=$(docker compose ps --format json airflow-api-server | python3 -c 'import json,sys; rows=[json.loads(line) for line in sys.stdin if line.strip()]; print(rows[0].get("Health", "") if rows else "missing")')
if [[ "$airflow_status" != "healthy" ]]; then
  echo "Airflow API is not healthy: $airflow_status" >&2
  exit 1
fi

pgrep -u azureuser -x p_ctmag >/dev/null 2>&1 || {
  echo "Control-M agent is not responding" >&2
  exit 1
}

echo "Airflow API: healthy"
echo "Control-M agent: healthy"
