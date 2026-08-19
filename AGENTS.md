# AGENTS.md

## Scope and purpose

This guide applies to the whole repository. Use it with the focused documents in
`docs/`; update those sources together when behaviour, commands, topology or the
presentation story changes.

## Project summary

This is a fictional retail DataOps demonstration named **Trade Day Close to Store
Replenishment**. It simulates store POS/EOD events and a supplier ASN, ingests the
source contract into Azure Databricks, builds/tests Silver and Gold with dbt Cloud,
exports a replenishment order, delivers it to a WMS simulator, and observes
acceptance.

One Azure Databricks/dbt Cloud data plane is exposed through two independent
control planes:

- Airflow orchestrates readiness through WMS delivery.
- Control-M orchestrates the same stages and continues through WMS acknowledgement
  and the 06:00 service SLA.

The purpose is to show coexistence, not replacement. Do not make Airflow invoke
Control-M, make Control-M invoke Airflow, or give either path different business
transformations.

The scenario is industry-informed but fictional. Never represent its estate,
thresholds, schedule, calendar or operating model as Kmart production facts.

## Architecture rules

Azure storage and Azure Databricks are the only business-data plane. dbt Cloud
connects to Databricks. Do not reintroduce PostgreSQL, Azurite, a local Jobs API
surrogate, a local dbt profile or a bridge that copies business data between local
and Azure databases.

The local stack contains only source/downstream/control-plane support:

- Redpanda and Redpanda Console;
- the Kafka-backed EOD readiness projector;
- WMS SFTP and acknowledgement writer;
- Airflow 3.3 standalone; and
- an on-demand toolbox.

Airflow's SQLite named volume is control-plane metadata only. It must never hold
retail events, medallion data, WMS state or transformation results.

Bronze, Silver and Gold mean physical Azure Databricks schemas/objects:

- `bronze`: six Delta source tables written by `00_ingest_bronze`;
- `silver`: four dbt staging views and two dbt intermediate tables; and
- `gold`: four tested dbt marts.

## Business flow

For an explicit trading date:

1. Upload product, stock and 28-day sales-history snapshots to ADLS Gen2.
2. Publish POS and EOD events to Redpanda and the ASN to ADLS Gen2.
3. Converge the EOD completeness and ASN arrival gates.
4. Snapshot the active `simulation_id` Kafka generation and publish a complete
   six-file Azure manifest.
5. Run the shared Azure Databricks ingest job and validate the entire contract
   before writing Bronze.
6. Run the shared dbt Cloud Stage, Intermediate and Gold jobs.
7. Run the shared Azure Databricks export job.
8. Deliver `REPLEN_ORDER_YYYYMMDD.csv` to WMS SFTP.
9. In Control-M only, wait for `REPLEN_ACK_YYYYMMDD.txt` and measure the 06:00 SLA.

Important contracts:

- The canonical estate is 325 stores across all eight Australian jurisdictions.
- The default has 2,000 products, 28 days of history and 200 transactions per
  store (65,000 total on a full day).
- EOD is `PROCEED` at 100%, `PROCEED_WITH_EXCEPTIONS` at or above 99.5% with
  missing stores, `PROCEED_WITH_TRADE_OPS_ALERT` from 98.0% to below 99.5%, and
  `HOLD` below 98.0%.
- The ASN header is exact and ordered. Header, checksum, count, required-value or
  date-window failures must occur before the attempted run's first Bronze write.
- Stage writes are rerunnable for a date. Preserve deterministic whole-snapshot
  and Delta `replaceWhere` windows.
- Filenames use `YYYYMMDD`; Python/cloud job variables use `YYYY-MM-DD`.
- Failed gates intentionally return non-zero.

## Sources of truth

| Path | Responsibility |
|---|---|
| `README.md` | Overview, quick start and honest boundaries |
| `docs/ARCHITECTURE.md` | Architecture decision, as-built boundaries and rerun rules |
| `docs/OPERATIONS.md` | Provisioning, startup, commands and recovery |
| `docs/RUNSHEET.md` | Canonical comparison/presentation sequence |
| `docs/talktrack.md` | Timed 23-minute presenter script derived from the focused docs |
| `docker-compose.yml` | Local simulation/control-plane topology |
| `Makefile` | Canonical operator command surface |
| `demo/` | Generators, readiness, landing, gates, WMS and failure logic |
| `databricks/` | Cluster/job provisioning and the two notebooks |
| `dbt/kmart_retail/` | Databricks dbt project |
| `dbt/*.py` | dbt Cloud provisioning/manual run adapters |
| `airflow/dags/` | Control Plane A only |
| `controlm/` | Control Plane B workflow and host adapters |
| `tests/` | Fast local contract tests |
| `runtime/` | Ignored generated IDs, modes, snapshots and watcher files |

`demo_design.md` is historical intent and may describe superseded implementation
choices. Executable behaviour plus the three focused docs above are authoritative.

## Environment and commands

Use Docker Compose v2, Python 3.12 and at least 4 GB of Docker memory. Run
`make prepare`, then put Azure/Databricks/dbt Cloud secrets only in ignored `.env`
or the platforms' authentication stores.

Start with `make help`. Common local operations are:

```bash
make prepare
make up
make health
make controlm-health
make ps
make logs
make down
```

The shared manual data chain is:

```bash
make seed DATE=2026-08-14
make eod-readiness-arm DATE=2026-08-14
make simulate DATE=2026-08-14
make stage-inputs DATE=2026-08-14
make databricks-ingest DATE=2026-08-14
make dbt-stage DATE=2026-08-14
make dbt-intermediate DATE=2026-08-14
make dbt-gold DATE=2026-08-14
make databricks-export DATE=2026-08-14
make deliver DATE=2026-08-14
```

Run the control planes with `make demo-airflow` and `make demo-controlm`. Do not
run them concurrently for the same date.

Cloud provisioning is explicit and externally mutating:

```bash
make databricks-provision
make dbt-cloud-publish
make dbt-cloud-provision
```

Control-M deployment and ordering are also external changes:

```bash
make controlm-build
make controlm-deploy
make run-controlm DATE=2026-08-14
```

Do not perform provisioning, publishing, deployment, ordering,
`make seed-sla-history`, `make controlm-service` or destructive `make clean` as an
incidental validation step.

## Development rules

1. Preserve a single shared data plane. Put reusable logic in `demo/`, Databricks
   notebooks or dbt, and keep orchestrator files as adapters.
2. Maintain both Airflow and Control-M when a shared job name, job-state key,
   command, path, variable or contract changes.
3. Preserve rerun safety. Every write needs a whole-snapshot replacement,
   deterministic date window or stable key.
4. Thread the trading date explicitly. Do not confuse replay date with the live
   Control-M order date used by the SLA.
5. Preserve `simulation_id` filtering and bounded Kafka snapshots; old immutable
   topic records must not contaminate a replay.
6. Publish the landing manifest last and validate every input before the first
   Bronze write.
7. Keep generators and notebooks free of scheduling, downstream dependency and
   retry decisions.
8. Keep Airflow ending at delivery and Control-M continuing through ACK/SLA unless
   the comparison itself is deliberately redesigned and documented.
9. Keep shell scripts in strict mode, quote expansions and validate dates at host
   boundaries.
10. Keep JSON deterministic and valid. Workflow source belongs under
    `controlm/workflows/`; generated substitutions belong under ignored `runtime/`.
11. Use pinned dependencies and update claims when versions change.
12. Prefer existing Make targets over undocumented raw commands; update `make
    help` descriptions for new operator actions.
13. Do not hand-edit generated `runtime/`, Airflow logs/config, or dbt target/logs.
14. Preserve unrelated user changes in a dirty worktree.
15. Update all affected docs when topology, commands, behaviour, thresholds or the
    presentation story changes.

## Validation

Always run:

```bash
make lint
make test
```

Use proportionate integration checks:

- source/gate changes: extend `tests/test_demo_contracts.py`, run the affected
  local simulation and prove reset;
- Compose/image changes: run `make up` and `make health` after lint;
- landing/notebook changes: stage the same date twice, run ingest twice and verify
  deterministic counts/windows in Databricks;
- dbt changes: publish/provision only with explicit authority, then run the three
  jobs and inspect tests/artifacts;
- Airflow changes: verify DAG import and one complete manual run;
- Control-M changes: use `make controlm-build`; deployment is not validation;
- failure changes: prove the failure and a successful `make reset`.

Local lint/tests must not contact or mutate Azure Databricks, dbt Cloud or
Control-M.

## Secrets and generated state

Treat `.env`, `.databrickscfg`, Airflow credentials, dbt Cloud tokens and
Control-M configuration as secrets. Never print, copy or commit them.

`runtime/databricks/azure.json` and `runtime/dbt_cloud/azure.json` contain only
non-secret generated IDs, are container-readable and are ignored. The Databricks storage key is stored in
a Databricks secret scope and referenced from cluster Spark configuration. Do not
put it directly in notebook code or job JSON.

## Destructive and external actions

- `make clean` deletes local named volumes permanently.
- `make controlm-deploy` mutates the connected SaaS tenant.
- `make run-controlm` and `make seed-sla-history` order live work.
- `make controlm-service` changes a host systemd service with sudo.
- `make databricks-provision` creates/updates billable Azure resources and a
  secret.
- `make dbt-cloud-publish` pushes a deployment branch.
- `make dbt-cloud-provision` creates/updates dbt Cloud resources.
- `make step0` wraps authentication, cloud provisioning, dbt publication and
  Control-M deployment; never use it as a validation shortcut.
- `make step1`, `make step3`, `make step4` and `make step6` start local and/or
  external presentation runs. `step4` and `step6` order live Control-M work.
- `make reset` mutates one date's demo input/output state but is the normal,
  reversible recovery operation.

## Honest boundaries

- Redpanda sources and WMS are simulations; Databricks and dbt Cloud are real.
- Airflow standalone/SQLite is for this one-user demo, not production guidance.
- The small Databricks cluster is not a performance simulation.
- `CLOSED_FOR_DEMO` and all retail policy are fictional demonstration choices.
- Schema drift uses an explicit source contract and dbt tests; do not claim an
  unimplemented native Control-M Data Assurance feature.
- Useful SLA forecasting needs successful history in the connected tenant.
- `controlm/descriptors/prod.json` contains placeholders and must not be deployed
  as-is.

## Definition of done

A change is complete when code, notebooks, dbt, both orchestration adapters,
tests, Make commands and documentation agree; applicable local validation passes;
rerun/reset behaviour remains safe; no secret/generated state is committed; and
the demo still tells an accurate shared-data-plane/coexistence story.
