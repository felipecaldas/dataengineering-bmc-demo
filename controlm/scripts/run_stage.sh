#!/usr/bin/env bash
set -euo pipefail

DEMO_ROOT=/home/azureuser/retail-data-demo
STAGE=${1:?stage is required}
RAW_DATE=${2:-20260814}
export PATH="/home/azureuser/.local/bin:/usr/local/bin:/usr/bin:/bin"

if [[ "$RAW_DATE" =~ ^[0-9]{8}$ ]]; then
  TRADING_DATE="${RAW_DATE:0:4}-${RAW_DATE:4:2}-${RAW_DATE:6:2}"
else
  TRADING_DATE="$RAW_DATE"
fi

if [[ ! "$TRADING_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Invalid trading date: $TRADING_DATE" >&2
  exit 64
fi

cd "$DEMO_ROOT"

# The long-running Agent may predate Docker's installation and therefore not
# carry its supplementary group. Open a short-lived docker-group shell for the
# stage without requiring a restart of the enrolled SaaS Agent.
run_compose() {
  local command
  printf -v command '%q ' docker compose "$@"
  sg docker -c "$command"
}

case "$STAGE" in
  gate-eod)
    run_compose run --rm toolbox python -m demo.cli gate-eod --date "$TRADING_DATE" --wait --timeout 120 --interval 5
    ;;
  stage-inputs)
    run_compose run --rm toolbox python -m demo.cli stage-inputs --date "$TRADING_DATE"
    ;;
  ingest)
    python3 databricks/run_job.py ingest --date "$TRADING_DATE"
    ;;
  export)
    python3 databricks/run_job.py export --date "$TRADING_DATE"
    ;;
  deliver)
    run_compose run --rm toolbox python -m demo.cli deliver --date "$TRADING_DATE"
    ;;
  confirm-ack)
    run_compose run --rm toolbox python -m demo.cli gate-ack --date "$TRADING_DATE" --wait --timeout 120 --interval 5
    ;;
  *)
    echo "Unknown stage: $STAGE" >&2
    exit 64
    ;;
esac
