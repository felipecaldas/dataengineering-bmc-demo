#!/usr/bin/env bash
set -euo pipefail

DEMO_ROOT=/home/azureuser/retail-data-demo
UNIT_SOURCE="$DEMO_ROOT/controlm/systemd/controlm-agent.service"
UNIT_TARGET=/etc/systemd/system/controlm-agent.service

if [[ ! -x /home/azureuser/ctmag/ctm/scripts/start-ag ]]; then
  echo "Control-M Agent start script is missing" >&2
  exit 1
fi

sudo install -o root -g root -m 0644 "$UNIT_SOURCE" "$UNIT_TARGET"
sudo systemctl daemon-reload

# This image previously had a combined Agent/Airflow unit with stale paths.
# Keep the host Agent independent from the containerised Airflow deployment.
if systemctl list-unit-files start-services.service --no-legend 2>/dev/null | grep -q '^start-services.service'; then
  sudo systemctl disable --now start-services.service
fi

sudo systemctl enable --now controlm-agent.service
sudo systemctl --no-pager --full status controlm-agent.service
