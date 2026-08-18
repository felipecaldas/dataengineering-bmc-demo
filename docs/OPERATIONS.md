# Operations guide

## Prerequisites

- Docker Engine with Compose v2 and at least 4 GB available memory.
- Python 3.12 for host provisioning scripts.
- An ADLS Gen2 storage account and an existing container.
- An Azure Databricks workspace in which the operator may create a secret scope,
  cluster, notebooks and jobs.
- A dbt Cloud project connected to the repository.
- For the Control-M path: the host CLI, enrolled Agent, dbt plug-in profile and
  BMC Event Handler.

The standard presentation date is `2026-08-14`. Pass `DATE` explicitly in
rehearsal and presentation commands.

## Configure secrets

Create the ignored environment file and runtime directories:

```bash
make prepare
```

Populate `.env` with the values represented by the empty fields in
`.env.example`. At minimum the cloud-backed profile needs:

```text
AZURE_STORAGE_ACCOUNT
AZURE_STORAGE_CONTAINER
AZURE_STORAGE_KEY
DATABRICKS_STORAGE_BASE_PATH
DATABRICKS_HOST
DATABRICKS_TOKEN
DBT_CLOUD_HOST
DBT_CLOUD_ACCOUNT_ID
DBT_CLOUD_PROJECT_ID
DBT_CLOUD_SERVICE_TOKEN
```

`DATABRICKS_STORAGE_BASE_PATH` is the ADLS Gen2 prefix visible to Spark, for
example:

```text
abfss://kmart-demo@ACCOUNT.dfs.core.windows.net/retail-data-demo
```

The Python adapters use the same account/container/prefix through the Azure Blob
API. The container must already exist. Do not commit `.env`, show it during a
presentation, or reuse the demo WMS credentials elsewhere.

## Provision Azure Databricks

Install the pinned CLI and authenticate without placing credentials in source:

```bash
make install-databricks-cli
databricks auth login \
  --host https://WORKSPACE.azuredatabricks.net \
  --profile retail-demo-azure
```

Create or reconcile the shared compute and jobs:

```bash
make databricks-provision
```

This explicit external operation:

- stores the ADLS key in the configured Databricks secret scope;
- creates or updates the single-node, auto-terminating cluster;
- imports `00_ingest_bronze` and `04_export_replenishment`;
- creates or updates the `ingest` and `export` jobs; and
- writes non-secret IDs to ignored `runtime/databricks/azure.json`.

The default cluster is Databricks Runtime 16.4 LTS on `Standard_D4as_v5` with a
20-minute inactivity timeout. It is billable Azure infrastructure. Provisioning
is never part of lint, tests or a normal workflow run.

## Provision dbt Cloud

Publish only the dbt project to its deployment branch, then create or reconcile
the Databricks connection, deployment credential, environment and jobs:

```bash
make dbt-cloud-publish
make dbt-cloud-provision
```

The shared jobs are:

| Job | Selector | Databricks output |
|---|---|---|
| `Retail Demo Stage` | `tag:stage` | Silver staging views |
| `Retail Demo Intermediate` | `tag:intermediate` | Silver intermediate tables |
| `Retail Demo Gold` | `tag:gold` | Gold marts |

Generated IDs are stored in ignored, container-readable
`runtime/dbt_cloud/azure.json`; service and Databricks tokens are not. `make
dbt-cloud-connect` may be used when only the
development connection/environment needs reconciliation.

Airflow reads both generated state files when the DAG is parsed. If jobs were
provisioned after Airflow started, restart that service:

```bash
make airflow-stop
make airflow-start
```

## Start and inspect local services

```bash
make up
make health
make ps
```

The local topology contains Redpanda, Redpanda Console, the EOD readiness
projector, WMS SFTP/acknowledgement simulation, Airflow standalone and the
on-demand toolbox. There is no Postgres, Azurite, Kafka-to-database ingress or
local Databricks surrogate.

| Endpoint | Address |
|---|---|
| Airflow | `http://localhost:8080` |
| Redpanda Console | `http://localhost:8081` |
| WMS SFTP | `sftp://demo:demo@localhost:2222` |
| Redpanda external listener | `localhost:19092` |

`make health` checks Kafka topics, Azure storage, WMS, Airflow and the readiness
projector. Use `make controlm-health` to check the enrolled host Agent separately.

Airflow uses `LocalExecutor` with SQLite on the `airflow-state` named volume. This
database contains control-plane metadata only. `make down` retains it;
`make clean` permanently removes it and all other local named volumes.

## Prove the shared data plane manually

The following chain invokes no orchestrator:

```bash
make seed DATE=2026-08-14
make eod-readiness-arm DATE=2026-08-14
make simulate DATE=2026-08-14
make gate-eod DATE=2026-08-14
make gate-asn DATE=2026-08-14
make stage-inputs DATE=2026-08-14
make databricks-ingest DATE=2026-08-14
make dbt-stage DATE=2026-08-14
make dbt-intermediate DATE=2026-08-14
make dbt-gold DATE=2026-08-14
make databricks-export DATE=2026-08-14
make deliver DATE=2026-08-14
make gate-ack DATE=2026-08-14
```

Expected default source counts are 2,000 products, 26,000 stock positions,
364,000 history rows, 65,000 POS messages, 325 EOD markers and 5,000 ASN rows.
`stage-inputs` selects only the current simulation generation and publishes the
manifest after every object exists.

## Run Airflow

Prepare, arm and trigger before publishing the source events:

```bash
make reset DATE=2026-08-14
make seed DATE=2026-08-14
make eod-readiness-arm DATE=2026-08-14
make run-airflow DATE=2026-08-14
make simulate DATE=2026-08-14
```

The convenience target performs the same startup sequence:

```bash
make demo-airflow DATE=2026-08-14
```

The DAG waits for store readiness and the Azure ASN, stages the active Kafka
generation, runs the shared Databricks/dbt Cloud jobs, and delivers the result.
Its responsibility ends at successful SFTP delivery.

## Run Control-M

Reconcile the host dbt trust/profile once, then render and validate the workflow:

```bash
make controlm-dbt-trust
make controlm-dbt-provision
make controlm-health
make controlm-build
```

Deployment and ordering mutate the connected Control-M tenant and therefore stay
explicit:

```bash
make controlm-deploy
make reset DATE=2026-08-14
make seed DATE=2026-08-14
make eod-readiness-arm DATE=2026-08-14
make run-controlm DATE=2026-08-14
make simulate DATE=2026-08-14
```

After deployment, `make demo-controlm DATE=...` performs startup, health, seed,
arm, order and simulation. It does not provision Azure/dbt Cloud resources or
deploy the workflow behind the operator's back.

The host Agent enters the application through `controlm/scripts/run_stage.sh`.
The dbt jobs use native `Job:DBT`; the Databricks command jobs synchronously invoke
the pre-provisioned jobs with the authenticated host CLI. Control-M continues
after delivery through the ACK File Watcher and `SLA_PickWave`.

## Failure and recovery operations

```bash
make fail-1 STORES=1 DATE=2026-08-14
make fail-1 STORES=8 DATE=2026-08-14
make fail-2 DATE=2026-08-14
make fail-3 DATE=2026-08-14
make fail-4 ROWS=400 DATE=2026-08-14
make fail-5 SECONDS=45
```

- `fail-1` configures the next simulation to withhold EOD markers.
- `fail-2` removes and suppresses the ASN.
- `fail-3` writes the unexpected `carton_id` ASN column.
- `fail-4` snapshots and injects negative stock rows.
- `fail-5` records an ingest delay in the next landing manifest.

WMS outcomes are independent:

```bash
make wms-ack
make wms-never-ack
make wms-late
make wms-reject
```

Always recover the date after an exercise:

```bash
make reset DATE=2026-08-14
```

Reset restores the source snapshot and modes, removes the date's manifest,
outbound object, WMS delivery/acknowledgement files and claims, recreates the
standard ASN, and releases withheld EOD markers for the active generation. It
does not rerun or delete remote Databricks/dbt Cloud jobs.

## Validation and shutdown

```bash
make lint
make test
make logs
make down
```

`make lint` is local and non-mutating. `make test` rebuilds the toolbox if needed
and runs the contract suite. Neither provisions cloud resources, deploys
Control-M, orders work, or deletes volumes.
