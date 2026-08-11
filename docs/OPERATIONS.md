# Demo operations and connection guide

This guide covers starting the complete demonstration after an Azure VM boot,
checking it, connecting to its services, and stopping it safely.

## What starts automatically with the VM

The enrolled Control-M Agent is the one intentional host component. Its systemd
service is enabled and starts after the network is online:

```bash
systemctl status controlm-agent
```

The service uses the vendor `start-ag` command at startup and `shut-ag` during VM
shutdown. The logical Agent name is `fmo-azureuser`.

Docker containers are managed by the repository's single master
`docker-compose.yml`. Start them explicitly after logging into the VM so the
operator can see whether preparation succeeded.

## One-command cold start

From a new login shell:

```bash
cd /home/azureuser/retail-data-demo
make demo-ready DATE=2026-08-14
```

`demo-ready` performs these operations in order:

1. Uses the repository's master `docker-compose.yml` file to build the images and
   start all demo containers as one application.
2. Checks Kafka, Azurite, Postgres, the local Jobs API surrogate, WMS, Airflow and
   the Control-M Agent.
3. Seeds 325 stores, the Australian trading calendar, 2,000 products and 28 days
   of sales history.
4. Generates 65,000 POS events, store EOD markers and the supplier ASN.
5. Resets all failure modes and leaves the standard trading date ready to run.

The first image build can take several minutes. Seeding and simulation are
idempotent, so the command is safe to repeat for the same date.

## Faster subsequent start

Named volumes retain the seeded data after `make down`. For a normal repeat session:

```bash
cd /home/azureuser/retail-data-demo
make up
make health
make reset DATE=2026-08-14
```

Inspect containers or logs with:

```bash
make ps
make logs
```

## Airflow access

Airflow is published on TCP 8080.

From the VM itself:

```text
http://localhost:8080
```

From the current Azure virtual network, the VM's present private address is:

```text
http://10.0.0.4:8080
```

The private address may change if Azure networking is configured dynamically.
Prefer the VM's private DNS name where one exists.

The Airflow username is `admin`. Retrieve the generated demo password on the VM:

```bash
cat airflow/config/simple_auth_manager_passwords.json.generated
```

For workstation access without opening an Azure NSG port, create an SSH tunnel:

```bash
ssh -L 8080:localhost:8080 azureuser@VM_PUBLIC_IP
```

Then open `http://localhost:8080` on the workstation.

Trigger the standard Airflow run with:

```bash
make run-airflow DATE=2026-08-14
```

## Kafka/Redpanda connection parameters

Kafka uses a binary TCP protocol; it does not have a browser URL.

### Client running directly on this VM

```text
bootstrap.servers=localhost:19092
security.protocol=PLAINTEXT
SASL authentication=none
TLS=disabled
```

### Client running in the Compose network

```text
bootstrap.servers=redpanda:9092
security.protocol=PLAINTEXT
SASL authentication=none
TLS=disabled
```

The configured topics are:

| Topic | Partitions | Purpose |
|---|---:|---|
| `pos.transactions.v1` | 6 | POS transaction events |
| `pos.store-eod.v1` | 3 | Per-store trading-day completion markers |
| `retail.store-eod-readiness-command.v1` | 1 | Explicit date/generation arm commands |
| `retail.store-eod-readiness-state.v1` | 1 | Private compacted idempotency state |
| `retail.store-eod-readiness.v1` | 1 | Committed business events consumed by BMC |

### Control-M Event Handler

The BMC Event Handler runs as one replica in the local kind cluster under the
`event-handler` namespace. Its machine-local configuration is under
`/home/azureuser/controlm-event-driven`; the AAPI token remains only in the
Kubernetes `ctm-credentials` Secret.

```bash
/home/azureuser/controlm-event-driven/scripts/install-handler.sh
/home/azureuser/controlm-event-driven/scripts/status.sh
source /home/azureuser/controlm-event-driven/env.sh
kubectl logs deployment/retail-event-handler -n event-handler --tail=100
```

The queue consumes `retail.store-eod-readiness.v1` with
`isolation.level=read_committed` and maps `message.event_name` to `setevent` on
`IN01`. If the VM private address changes, update both `KAFKA_ADVERTISED_HOST` in
ignored `.env` and `bootstrap.servers` in the handler's `config/queues.yml`, then
recreate Redpanda and rerun `install-handler.sh`.

List them from the VM with:

```bash
make kafka-topics
```

That Make target wraps
`docker compose exec redpanda rpk topic list --brokers redpanda:9092`. Routine
operator actions are exposed through `make`; the underlying command is documented
only to make the wrapper transparent.

The broker listener configuration and topic creation are in `docker-compose.yml`.
Application containers receive `KAFKA_BOOTSTRAP=redpanda:9092`; host-side demo code
defaults to `localhost:19092` in `demo/config.py`.

### Remote Kafka clients

The checked-in `.env.example` value is:

```bash
KAFKA_ADVERTISED_HOST=localhost
```

This machine's ignored `.env` uses its private address because the kind pod is a
Kafka client outside the Compose network. Any external client must receive a broker
address it can resolve and reach. On a trusted Azure virtual network, set a stable
private DNS name or IP in `.env`, for example:

```bash
KAFKA_ADVERTISED_HOST=fmo.internal.example
```

Then recreate Redpanda so the advertised broker metadata changes:

```bash
docker compose up -d --force-recreate redpanda
```

Do not expose this unauthenticated PLAINTEXT listener to the public internet. Use
private networking, or add TLS and authentication before allowing remote access.

## Redpanda Console

The Kafka browser UI is published on TCP 8081:

```text
http://localhost:8081
http://10.0.0.4:8081
```

Tunnel both operator UIs from a workstation with:

```bash
ssh -L 8080:localhost:8080 -L 8081:localhost:8081 azureuser@VM_PUBLIC_IP
```

## Other local endpoints

| Service | Host connection |
|---|---|
| Local Jobs API surrogate (not Azure Databricks) | `http://localhost:8090/docs` |
| Azurite Blob endpoint | `http://localhost:10000` |
| PostgreSQL | `postgresql://retail:retail@localhost:5432/retail` |
| WMS SFTP | `sftp://demo:demo@localhost:2222` |
| Redpanda admin API | `http://localhost:9644` |

These credentials are intentionally local-demo values and must not be reused in a
production or publicly accessible environment.

## Databricks integration boundary

The self-contained profile does not connect to Azure Databricks. In
`docker-compose.yml`, Airflow's `databricks_default` connection is explicitly:

```text
host=databricks-local
port=8000
scheme=http
token=demo-token
```

That connection lets the actual Airflow `DatabricksRunNowOperator` exercise the
Jobs API interaction against the local surrogate. It does not provide Spark,
Delta Lake, clusters, Azure authentication or Azure runtime performance.

The original `demo_design.md` specified external Azure Databricks. The local
surrogate is the implementation choice made to satisfy the later requirement that
the demo run entirely from the master Compose application. No connections or
credentials were taken from the host Airflow installation. The Azure host, token
and HTTP-path placeholders in `.env.example` remain empty and are not wired into
the default profile.

### Optional host Databricks CLI

The optional real-Azure profile uses the current Databricks CLI rather than the
legacy Python CLI. Install the repository-pinned Linux x86-64 release for the
current user with:

```bash
make install-databricks-cli
databricks version
```

The installer verifies the release archive against its pinned SHA-256 digest and
places the binary at `~/.local/bin/databricks`; it does not use sudo, change an
existing Databricks authentication profile, or activate the real-Azure profile.
It exits without changing anything when the pinned version is already installed.
Use `DATABRICKS_CLI_INSTALL_DIR` to select a different user-writable directory.

Authentication is a separate, explicit operation. Once the actual workspace host
is known, create a named OAuth profile without putting a token in source or shell
history:

```bash
databricks auth login \
  --host https://WORKSPACE.azuredatabricks.net \
  --profile retail-demo-azure
```

Complete the browser authorization as the intended Azure Databricks identity. Do
not overwrite the default local profile or commit `~/.databrickscfg`.

The currently connected Azure workspace is Standard-tier and has no Unity Catalog
metastore. It therefore cannot provide a Databricks SQL warehouse or Unity Catalog
volume. The optional profile uses a dedicated single-node all-purpose cluster and
the legacy Hive Metastore instead. Provision or restart that cluster explicitly:

```bash
make databricks-azure-provision
```

This is an external, billable operation. The cluster uses Databricks Runtime 16.4
LTS, `Standard_D4as_v5`, and a 20-minute inactivity timeout by default. Override
those non-secret settings with the `DATABRICKS_*` variables documented in
`.env.example`. Provisioning is idempotent by cluster name and writes the generated
cluster ID and HTTP path to ignored `runtime/databricks/azure.json`. It does not
put OAuth credentials or workspace values into the repository.

After the normal local bronze and silver stages succeed, copy that validated
date into Azure Databricks with:

```bash
make databricks-azure-sync DATE=2026-08-14
```

This target is also an explicit external, billable operation. It performs three
separate adapter actions:

1. `demo.cli export-databricks-silver` reads all six dbt sources from one
   repeatable-read PostgreSQL snapshot and writes deterministic CSV files plus a
   row-count manifest under ignored `runtime/databricks/export/YYYYMMDD/`.
2. `databricks/sync_silver.py` uploads the files to
   `dbfs:/tmp/retail-data-demo/YYYYMMDD/` and imports the checked-in
   `00_load_silver` notebook under `/Shared/retail-data-demo/`.
3. A one-time Databricks run validates the manifest and atomically replaces the
   matching date windows in six `silver` Delta tables in the legacy Hive
   Metastore. Repeating the same date replaces the same partitions.

Run `make silver DATE=...` first. The Azure adapter deliberately does not
reimplement or weaken the pre-silver ASN header contract: a failed local silver
stage prevents a complete export manifest from being created. The reference
product table is replaced as a whole; POS, EOD, ASN and stock are replaced for
the requested date; sales history is replaced for the preceding 28-day window.

The dbt models use portable casts plus adapter-dispatched date arithmetic, so the
same ten-model graph remains valid for local Postgres and dbt Cloud on
Databricks. The connection and environment in dbt Cloud are account metadata and
are configured separately from this data synchronization step.

With both the Databricks and dbt Cloud CLIs already authenticated, create or update
the account-level connection and development environment explicitly:

```bash
make dbt-cloud-databricks-provision
```

This mutates the connected dbt Cloud account. It is idempotent by the names
`Retail demo Azure Databricks` and `Azure Databricks Development`, preserves the
existing Postgres connection, and records only generated object IDs in ignored
`runtime/dbt_cloud/azure.json`. The script reads the Databricks hostname and HTTP
path from generated state. It prefers `DBT_CLOUD_HOST`, `DBT_CLOUD_ACCOUNT_ID`,
`DBT_CLOUD_PROJECT_ID`, and `DBT_CLOUD_SERVICE_TOKEN` from the ignored root `.env`;
if the project ID is blank or still a placeholder, it uses the `dbt-cloud` project
ID in `dbt_project.yml`. When the remaining service-token fields are incomplete,
it falls back to the personal dbt Cloud CLI configuration. It never writes either
credential into source. `make prepare` restricts `.env` permissions to the current
user.

The connected dbt tenant currently exposes the connection as `databricks_v0` and
rejects that adapter when an environment requests `latest-fusion`. The script
therefore defaults this environment to the Core `latest` track. Set
`DBT_CLOUD_DBT_VERSION=latest-fusion` only after the account API reports that its
Databricks connection is Fusion compatible; the generic product capability label
does not override the tenant's live validation result.

Development credentials remain personal to each dbt user and are deliberately not
copied from the Databricks CLI's cached OAuth session. In dbt Cloud, open the new
environment's personal credentials, authenticate to Databricks using an approved
OAuth application or a separately governed token, set the development schema, and
run **Test connection**. The administrative API and Terraform provider do not make
personal development credentials actionable on an environment resource, so this is
the one identity-bound interactive step rather than a repository secret.

Upgrading the Azure workspace to Premium and assigning a Unity Catalog metastore
would allow the preferred SQL Warehouse and Unity Catalog design. Do not present
those features as active while the Standard-tier adapter is in use.

## Control-M operations

Validate the workflow without changing the tenant:

```bash
make controlm-build
```

The integrated profile needs three generated dbt Cloud job IDs and a centralized
Control-M connection profile. Prepare those resources before validation:

```bash
make dbt-cloud-publish-controlm
make dbt-cloud-controlm-provision
make controlm-dbt-trust
make controlm-dbt-provision
```

`controlm-dbt-trust` is an idempotent host operation. It adds the public ISRG Root
X1 certificate to the Application Integrator trust store, retaining a backup, and
restarts only that plug-in container. This is required on an older Agent trust
store when dbt Cloud reports `PKIX path building failed`; it does not disable TLS
certificate verification. Override `CTM_AGENT_HOME`, `CTM_DBT_CA_FILE`, or
`CTM_AI_TRUSTSTORE_PASSWORD` only when the local Agent installation differs.

`controlm-dbt-provision` writes the dbt service token into centralized profile
`FMO_AZURE_DBT` through a mode-0600 temporary file and removes that file after
deployment. Plug-in version 1.0.01 does not implement the generic connection-test
operation, so its first real `Job:DBT` execution is the effective test.

The current validated result is one SMART folder, 12 jobs and a valid development
descriptor. It contains three native `Job:DBT` jobs between Azure source sync and
the Azure Gold export. Deployment and ordering are separate external changes:

```bash
make controlm-deploy
make run-controlm DATE=2026-08-14
```

`controlm-deploy` changes `se-dev`, and `run-controlm` starts workload in that
environment. Use each only when that external action is explicitly intended.

`run-controlm` orders the folder on Control-M's live business date, preserving a
meaningful 06:00 SLA clock, and passes `DEMO_DATE=20260814`,
`DEMO_ISO_DATE=2026-08-14`, and complete ASN/acknowledgement paths. The folder
defaults `DEMO_DATE` to `%%ODATE` for a production-like scheduled run. This
separation prevents a replay date from making the live service deadline historical
and prevents a period after an AutoEdit date variable from being parsed as part of
the variable name.

## Conducting the presentation

Startup and presentation are deliberately separate concerns:

- this document explains how to operate and connect to the environment;
- [`RUNSHEET.md`](RUNSHEET.md) gives the timed, command-by-command sequence for the
  customer presentation, including the real Azure Delta synchronization, dbt
  Cloud build, Postgres isolation proof, optional Airflow/Control-M comparison,
  failure choices and cleanup.

For the integrated Control-M profile, run the complete preparation and order
sequence with:

```bash
make demo-controlm-azure DATE=2026-08-14
```

This explicitly arms a new EOD generation, orders asynchronously, and only then
publishes the POS/EOD and ASN inputs. Monitor `WaitForStoreEODThreshold` in Wait
Condition, the readiness message in Redpanda Console, and all 12 jobs through Azure
Delta synchronization, the three dbt Cloud plug-in calls, Azure Gold export, WMS
delivery, acknowledgement, and SLA closure. The readiness projector itself has no
Postgres dependency. Keep Postgres running for the still-local ingress/conformance,
run-metadata, and acknowledgement components in this transitional profile.

Postgres isolation is a presentation action, not a normal stack state. Use it only
after a successful Azure sync:

```bash
make postgres-stop
# Run only the dbt Cloud/Azure section here; make health is expected to fail.
make postgres-start
make health
```

The separate Postgres-isolation proof is still available after a completed Azure
sync, but it is not part of the integrated end-to-end run. The Airflow scheduler,
local Jobs API, Kafka ingress, gates, failure tools, and WMS acknowledgement adapter
require local Postgres in the default profile.

## Safe shutdown

Stop the containerised demo while retaining all named-volume data:

```bash
make down
```

The Control-M Agent can remain running. It shuts down automatically when the Azure
VM shuts down.

To stop the Agent manually:

```bash
sudo systemctl stop controlm-agent
```

To discard all Compose data and return to a fresh database and broker, use:

```bash
make clean
```

`make clean` removes the demo's named volumes and is deliberately destructive.
