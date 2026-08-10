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

The default `.env` value is:

```bash
KAFKA_ADVERTISED_HOST=localhost
```

This is correct for clients on the VM. A client on another machine must receive a
broker address it can resolve and reach. On a trusted Azure virtual network, set a
stable private DNS name or IP in `.env`, for example:

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
credentials were taken from the host Airflow installation. The optional real-Azure
variables in `.env.example` are empty and are not wired into the default profile.

## Control-M operations

Validate the workflow without changing the tenant:

```bash
make controlm-build
```

The current validated result is one SMART folder, nine jobs and a valid development
descriptor. Deployment and ordering are separate external changes:

```bash
make controlm-deploy
make run-controlm DATE=2026-08-14
```

`controlm-deploy` changes `se-dev`, and `run-controlm` starts workload in that
environment. Use each only when that external action is explicitly intended.

`run-controlm` orders the folder on Control-M's live business date, preserving a
meaningful 06:00 SLA clock, and passes `DEMO_DATE=20260814` for the fixed data
partition. The folder defaults `DEMO_DATE` to `%%ODATE` for a production-like
scheduled run. This separation prevents a replay date from making the live service
deadline historical.

## Conducting the presentation

Startup and presentation are deliberately separate concerns:

- this document explains how to operate and connect to the environment;
- [`RUNSHEET.md`](RUNSHEET.md) gives the timed, command-by-command sequence for the
  customer presentation, including the Airflow run, independent Control-M run,
  comparison points, failure choices and cleanup.

In short: run `make demo-ready`, prove the two input gates, run Airflow, reset the
shared data plane, run Control-M for the same date, compare their service boundaries,
then demonstrate one or two reversible failures.

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
