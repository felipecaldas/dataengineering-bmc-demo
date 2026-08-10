# Trade Day Close to Store Replenishment

This repository implements the demo in `demo_design.md` as one repeatable Docker
Compose application. It presents the same idempotent retail data plane through two
independent control planes:

- **Control plane A:** Apache Airflow 3.3 with Cosmos 1.15, showing ten dbt nodes.
- **Control plane B:** the host's enrolled Control-M SaaS Agent 9.0.22, with a
  workflow-as-code definition, native file watches, downstream acknowledgement,
  and an SLA Management job.

The business premise is deliberately labelled as an industry-informed assumption,
not as Kmart's actual process. Confirm overnight versus intraday replenishment,
cut-off time, POS transport, ASN ownership, and store/state distribution with the
customer before presenting.

## What is self-contained

`docker-compose.yml` is the master file. It starts Redpanda (Kafka API), Azurite
(Azure Blob API), Postgres, a Databricks Jobs API-compatible local stage runner,
an SFTP WMS and acknowledgement writer, the data generators, and Airflow.

The Control-M Agent is the only deliberate host component. Its SaaS enrollment and
identity already belong to this machine and must not be copied into an image. Every
job it launches executes inside the Compose application through
`controlm/scripts/run_stage.sh`.

The checked-in `controlm-agent.service` only manages that existing host Agent. It
does not install, enroll, or copy Agent credentials, and it has no dependency on
Airflow.

The local Databricks service preserves the Jobs API job/run interaction used by the
Airflow provider, but it is not Apache Spark or Delta Lake and must not be presented
as a performance simulation of Azure Databricks. The checked-in Databricks notebook
and job definitions are the adapter for a later real-Azure profile. No Azure
workspace details were supplied or discovered, and no connection, credential or
token was read from or copied out of the host Airflow installation.

This is an explicit implementation change from the original `demo_design.md`,
which specified external Azure Databricks. It follows the later requirement that
the runnable demo be self-contained under one Compose file. Connecting a real
workspace remains possible as a separate profile, but requires an Azure workspace
host, authentication method, job IDs 440/441/447 (or replacements), and any Blob,
catalog and SQL-warehouse details. Those values are currently empty.

## Airflow version decision

The host's Airflow 2.10.5 is unsuitable for this August 2026 demo. The Airflow
project lists all 2.x releases as EOL from 22 April 2026, while 3.3.0 is the current
maintained release. This project therefore leaves the old host installation alone
and pins the official `apache/airflow:3.3.0-python3.12` image. Cosmos 1.15.0 is the
current stable release and supports Airflow 3; its Airflow 3 UI integration requires
Airflow 3.1 or newer.

References: [Airflow supported versions](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html),
[Airflow Docker image](https://airflow.apache.org/docs/docker-stack/), and
[Cosmos Airflow 3 compatibility](https://astronomer.github.io/astronomer-cosmos/policy/airflow3-compatibility.html).

## Quick start

The host needs Docker Engine with Compose v2 and at least 4 GB memory available.
On this machine Docker 29.1.3 and Compose 2.40.3 are installed. The one-time Agent
boot-service install requires sudo; it replaces the machine's obsolete combined
Agent/Airflow startup unit, whose paths no longer exist:

```bash
make controlm-service
```

Then start the entire containerised application from the master Compose file:

```bash
make up
make health
make seed
make simulate DATE=2026-08-14
```

For a cold VM start, the equivalent one-command preparation is:

```bash
make demo-ready DATE=2026-08-14
```

If the current terminal predates membership of the `docker` group, open a new login
shell first. Control-M stage commands handle this themselves through a short-lived
group shell.

Run every stage without an orchestrator to prove data-plane independence:

```bash
make gate-eod DATE=2026-08-14
make gate-asn DATE=2026-08-14
make bronze DATE=2026-08-14
make silver DATE=2026-08-14
make dbt DATE=2026-08-14
make replen DATE=2026-08-14
make deliver DATE=2026-08-14
make gate-ack DATE=2026-08-14
```

The operator UIs are:

| Service | URL / endpoint |
|---|---|
| Airflow | `http://HOST:8080` |
| Redpanda Console | `http://HOST:8081` |
| Local Jobs API surrogate (not Azure Databricks) | `http://HOST:8090/docs` |
| Azurite Blob | `http://HOST:10000` |
| WMS SFTP | `sftp://demo:demo@HOST:2222` |
| Postgres | `postgresql://retail:retail@HOST:5432/retail` |

The Airflow Simple Auth Manager generates the local admin password in
`airflow/config/simple_auth_manager_passwords.json.generated` on first start.

## Control plane A — Airflow

Trigger the already-unpaused DAG:

```bash
make run-airflow DATE=2026-08-14
```

The demo defaults to manual triggering so a newly created environment cannot start
a stale catch-up run before data is seeded. To demonstrate the design's 01:00
schedule, set `DEMO_AIRFLOW_SCHEDULE=0 1 * * *` in `.env` before `make up`.

The DAG uses two proper rescheduling sensors and the actual open-source Airflow
Databricks provider against the local Jobs API surrogate, plus a Cosmos
`DbtTaskGroup`. The provider code is real, but the target is not Azure Databricks.
The DAG intentionally ends when the order is written to WMS. It does not model the
WMS acknowledgement or 06:00 business service; those omissions are the comparison
in the design, not missing implementation.

## Control plane B — Control-M

The workflow is in `controlm/workflows/trade_close_to_replenishment.json`. It uses
the Agent logical host `fmo-azureuser` and scheduling server `IN01`, discovered from
the installed Agent. Validate before any external change:

```bash
make controlm-build
```

Deployment is intentionally separate because it changes the connected tenant:

```bash
make controlm-deploy
make run-controlm DATE=2026-08-14
```

The self-contained version uses `Job:Command` for the local Databricks API and dbt
Core. This is technically important: Control-M's current `Job:DBT` integration is
for a pre-existing dbt platform job, not a generic local dbt Core invocation. Native
`Job:Databricks` is appropriate when the optional Azure connection profile is
configured. Native File Watcher and SLA Management remain first-class jobs here.
The dbt command wrapper records a date-scoped failure marker, so a Control-M rerun
uses `dbt retry`; a successful run clears the marker.

The current `se-dev` API token is authenticated and authorised for configuration
reads and workflow compilation. `ctm build` validates one SMART folder, all nine
jobs, and the development deploy descriptor. The Agent is enrolled, running, and
has a successful server ping; the same stage chain has also been run with all
Airflow services stopped. No workflow has been deployed by this repository.

`.github/workflows/validate.yml` runs local contracts on a hosted runner and compiles
the development descriptor on a `self-hosted, controlm` runner. Set the repository
variable `CTM_ENV`; the runner owns its CLI authentication, so tenant credentials are
not committed. The production descriptor is deliberately excluded until its
placeholder server, Agent and run-as values are replaced with real production values.

## Failure injection

Every scenario is one reversible command:

```bash
make fail-1 STORES=1   # 324/325 = 99.692%, proceed with a named exception
make reset
make fail-1 STORES=8   # 317/325 = 97.538%, hold as an integration incident
make fail-2            # ASN absent
make fail-3            # unexpected carton_id; silver contract fails before load
make fail-4 ROWS=400   # negative stock; dbt accepted_range tests fail
make fail-5 SECONDS=45 # no failure, but a Control-M SLA forecast can go late
make reset
```

`make fail-4` snapshots every changed stock row before mutation. `make reset`
restores it, resets all modes, republishes withheld EOD markers, regenerates the
standard ASN, refreshes bronze and silver, and removes date-specific WMS
acknowledgements.

The WMS modes are also one-command and deliberately not embedded in either
orchestrator: `make wms-never-ack`, `make wms-late`, `make wms-reject`, and
`make wms-ack`. Reset cancels any in-flight response and restores normal ack mode.

## Data and business rules

- 325 stores are distributed across all eight Australian jurisdictions.
- `holidays` generates the 2026 state/territory holiday reference calendar.
- 2,000 products and 28 days of pre-aggregated velocity history are seeded.
- A normal simulated day publishes 65,000 POS messages and one EOD marker per
  expected trading store.
- EOD policy: at least 99.5% proceeds; 98.0–99.5% proceeds with a trade-ops alert;
  below 98.0% holds.
- The ASN schema is validated before silver rows are changed.
- dbt builds four staging, two intermediate, and four gold models with tests.
- All stage writes use natural keys or replace the date partition and are safe to
  rerun for the same trading date.

For the exact presentation sequence see `docs/RUNSHEET.md`. For component and
trust-boundary detail see `docs/ARCHITECTURE.md`. Startup, shutdown, UI access and
Kafka client settings are documented in `docs/OPERATIONS.md`.

## Honest boundaries

- Validate all customer-specific operating assumptions before the session.
- State public holidays are real 2026 dates, but `CLOSED_FOR_DEMO` is a demo policy;
  actual retailer trading restrictions must come from an authoritative store calendar.
- Native Control-M Data Assurance schema-drift detection is not claimed. The demo
  enforces an explicit pre-silver schema contract and uses dbt tests for row quality.
- SLA forecasting needs successful history in the target tenant. After deploying,
  run and verify 12–15 successful workflows before rehearsing Failure 5.
- The `prod` deploy descriptor contains placeholders and must never be used as-is.
