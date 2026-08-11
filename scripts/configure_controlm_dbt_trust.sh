#!/usr/bin/env bash
set -euo pipefail

agent_home=${CTM_AGENT_HOME:-/home/azureuser/ctmag/ctm}
truststore="$agent_home/cm/AI/data/security/apcerts"
container="$agent_home/cm/AI/exe/cm_container"
ca_file=${CTM_DBT_CA_FILE:-/etc/ssl/certs/ISRG_Root_X1.pem}
store_password=${CTM_AI_TRUSTSTORE_PASSWORD:-appass}
alias_name=isrgrootx1-dbt
backup="$truststore.pre-isrg-root-x1"
container_dir=$(dirname "$container")

for path in "$truststore" "$container" "$ca_file"; do
  if [[ ! -e "$path" ]]; then
    echo "Required Control-M trust path does not exist: $path" >&2
    exit 1
  fi
done

if keytool -list -keystore "$truststore" -storepass "$store_password" \
  -alias "$alias_name" >/dev/null 2>&1; then
  echo "Control-M Application Integrator already trusts ISRG Root X1."
  if ! "$container" status >/dev/null 2>&1; then
    (cd "$container_dir" && ./cm_container start)
  fi
  exit 0
fi

if [[ ! -e "$backup" ]]; then
  cp -p -- "$truststore" "$backup"
fi

keytool -importcert -noprompt \
  -alias "$alias_name" \
  -keystore "$truststore" \
  -storepass "$store_password" \
  -file "$ca_file"

if "$container" status >/dev/null 2>&1; then
  (cd "$container_dir" && ./cm_container stop)
fi
(cd "$container_dir" && ./cm_container start)

echo "Control-M Application Integrator now trusts the dbt Cloud certificate chain."
echo "Backup retained at $backup"
