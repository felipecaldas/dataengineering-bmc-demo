# AGENTS.md

## Purpose of this file

This file is the working guide for coding agents and human contributors in this
repository. It applies to the entire repository. Use it together with the focused
operator and presentation documents under `docs/`; do not duplicate or silently
contradict those documents when changing the project.

## Project in one paragraph

This is a self-contained retail DataOps demonstration called **Trade Day Close to
Store Replenishment**. It simulates a retailer collecting point-of-sale (POS) and
store end-of-day events, waiting for a supplier advance shipping notice (ASN),
conforming and testing the data, calculating replenishment orders, delivering an
order to a warehouse management system (WMS), and observing downstream acceptance.
The same idempotent data plane is deliberately exposed through two independent
control planes: Apache Airflow and Control-M. The demo is intended to show where
the products complement one another, not to prove that one should replace the
other.

The retail scenario is industry-informed but fictional. Its thresholds, schedule,
store estate, operating model, and integration boundaries are demo assumptions;
they must never be represented as Kmart's actual production process.

## What the demo is trying to show

The central design choice is a **shared data plane with independent control
planes**:

- Airflow shows Python-native data orchestration, provider integrations, two
  rescheduling sensors, and detailed dbt/Cosmos lineage across ten models.
- Control-M shows cross-platform operational ownership, native file watches,
  retry-from-failure behaviour, WMS acknowledgement, and an end-to-end 06:00
  business-service SLA.
- Both invoke the same gates and idempotent processing stages. Neither control
  plane owns the business transformations.
- The Airflow workflow deliberately ends after the order is delivered to WMS.
  The Control-M workflow deliberately continues through WMS acknowledgement and
  SLA measurement. That difference is part of the comparison.
- The intended conclusion is coexistence: Airflow is strong inside the data
  engineering domain, while Control-M can govern the wider business service.

Do not blur this comparison by making Airflow invoke Control-M, Control-M invoke
Airflow, or by giving either path a different implementation of the data stages.

## Business flow and contracts

For a trading date, the logical flow is:

1. Seed reference data and historical sales/stock data.
2. Publish POS transaction events and per-store EOD markers to Redpanda.
3. Generate the supplier ASN in Azurite and mirror it to `runtime/asn/` for the
   Control-M File Watcher.
4. Converge the store-completeness and ASN-readiness gates.
5. Run bronze ingestion (Databricks job ID `440`).
6. Run silver conformance and enforce the ASN schema before changing silver rows
   (job ID `441`).
7. Run the ten-model dbt graph and its data tests.
8. Calculate and export replenishment needs (job ID `447`).
9. Deliver a deterministic CSV to WMS SFTP.
10. In the Control-M path, wait for WMS acknowledgement and measure the complete
    service against the 06:00 pick-wave deadline.

Important business and data contracts:

- The canonical estate is 325 stores across all eight Australian jurisdictions.
- The default seed has 2,000 products and 28 days of history.
- A normal day produces 200 POS transactions per store (65,000 total).
- Store EOD completeness is `PROCEED` at 100%,
  `PROCEED_WITH_EXCEPTIONS` at or above 99.5% with missing stores,
  `PROCEED_WITH_TRADE_OPS_ALERT` from 98.0% to below 99.5%, and `HOLD` below
  98.0%.
- The ASN header is an exact, ordered contract. Unexpected or missing columns
  must fail before the silver ASN partition is changed.
- Stage writes must remain safe to rerun for the same trading date. Preserve the
  natural keys and date-partition replacement rules documented in
  `docs/ARCHITECTURE.md`.
- Export paths and WMS filenames are deterministic and use `YYYYMMDD` in names.
- Failed gates intentionally exit non-zero. A non-zero result in a failure demo
  is often the expected result, not necessarily a defect.

## Honest boundaries

Maintain these statements in code, docs, and presentations:

- The default stack does **not** connect to Azure Databricks. The
  `databricks-local` FastAPI service implements the Jobs API interaction needed by
  the real Airflow Databricks provider, but it is not Spark, Delta Lake, a cluster,
  or a performance simulation.
- The checked-in notebooks, job definitions, and empty Azure settings are an
  adapter/handoff for a future real-Azure profile.
- Azurite, Redpanda, local Postgres, and the SFTP credentials are demo components
  and demo-only credentials.
- `CLOSED_FOR_DEMO` is a demonstration calendar policy, not an authoritative
  statement about retailer trading restrictions.
- Schema drift is enforced by an explicit pre-silver contract and dbt tests. Do
  not claim native Control-M Data Assurance behaviour that is not implemented.
- Useful Control-M SLA prediction requires successful history in the connected
  tenant; the JSON definition alone does not create a meaningful forecast.
- `controlm/descriptors/prod.json` contains placeholders and must not be deployed
  as-is.

## Architecture

`docker-compose.yml` is the single master application definition. The default
stack contains:

- `redpanda` and `redpanda-console` for Kafka-compatible event transport and
  inspection;
- `azurite` for the Azure Blob-compatible file boundary;
- `postgres` for ingress, bronze, silver, dbt schemas, and run metadata;
- `kafka-ingest` for continuous idempotent landing of Kafka events;
- `databricks-local` for independently triggerable Jobs API-compatible stages;
- `wms-sftp` and `wms-ack-writer` for delivery and configurable acceptance;
- Airflow 3.3 API server, scheduler, DAG processor, and triggerer services; and
- a `toolbox` image containing the Python operator CLI and dbt.

The enrolled Control-M Agent is intentionally outside Compose because its identity
belongs to the host and connected SaaS tenant. Its jobs enter the application only
through `controlm/scripts/run_stage.sh`. Do not copy Agent credentials into this
repository or a container image.

The generators and processing stages contain no orchestration decisions. The
continuous Kafka ingress and WMS acknowledgement writer observe their own inputs
but never schedule the next pipeline stage.

## Repository map and sources of truth

| Path | Responsibility |
|---|---|
| `README.md` | Project overview, quick start, comparison, and public boundaries |
| `docs/ARCHITECTURE.md` | Component boundaries and idempotency rules |
| `docs/OPERATIONS.md` | Startup, access, connection details, and safe shutdown |
| `docs/RUNSHEET.md` | Canonical presentation sequence and talking points |
| `demo_design.md` | Original design intent; some implementation choices were superseded by the self-contained profile |
| `_demo_explainer.md` | Supporting narrative/explanation, not executable configuration |
| `docker-compose.yml` | Master local topology, pinned service versions, ports, and environment wiring |
| `Makefile` | Canonical operator and developer command interface |
| `demo/` | Python CLI, generators, gates, stages, WMS adapter, failures, and Jobs API surrogate |
| `infra/postgres/init.sql` | Initial schemas and persistent table contracts |
| `airflow/dags/` | Control plane A orchestration only |
| `dbt/kmart_retail/` | Staging, intermediate, marts, macros, tests, and Postgres profile |
| `controlm/workflows/` | Control plane B workflow-as-code source |
| `controlm/descriptors/` | Environment-specific Control-M substitutions |
| `controlm/scripts/` | Host-Agent entry points into Compose |
| `databricks/` | Optional real-Databricks notebooks and job definitions |
| `generators/` | Thin standalone generator entry points |
| `failures/` | Reversible scenario wrappers and the canonical reset operation |
| `tests/` | Fast local contract tests |
| `runtime/` | Generated presentation files and state; ignored except for `.gitkeep` |

When documentation disagrees with executable behaviour, first determine whether
the code or docs changed intentionally. `README.md` explicitly records where the
self-contained implementation supersedes the original `demo_design.md`. Update all
affected sources in the same change instead of leaving a new discrepancy.

## Environment and prerequisites

- Use Docker Engine with Compose v2 and allocate at least 4 GB of memory.
- Python code and images target Python 3.12.
- Copy `.env.example` to `.env` through `make prepare`; do not commit `.env`.
- The standard presentation date is `2026-08-14`. Most Make targets accept
  `DATE=YYYY-MM-DD`; always pass the date explicitly in reproducible examples.
- Run application commands through Make and the toolbox container unless a task
  specifically concerns host integration.
- The Control-M commands require the host CLI, a configured environment, and an
  enrolled/running Agent. A normal local contributor may not have those.

Treat `.env`, generated Airflow credentials, Control-M CLI configuration, and any
future Azure values as secrets even though the checked-in defaults are isolated
demo credentials. Never print, copy, or commit credentials discovered on the host.

## Important commands

Start with `make help`; the Makefile is the canonical command interface.

### Prepare and operate the stack

```bash
make prepare
make up
make health
make ps
make logs
make down
```

For a cold presentation environment:

```bash
make demo-ready DATE=2026-08-14
```

`make down` retains named-volume data. `make clean` removes containers **and all
named data volumes** and is destructive.

### Prepare and run the shared data plane

```bash
make seed
make simulate DATE=2026-08-14
make gate-eod DATE=2026-08-14
make gate-asn DATE=2026-08-14
make bronze DATE=2026-08-14
make silver DATE=2026-08-14
make dbt DATE=2026-08-14
make replen DATE=2026-08-14
make deliver DATE=2026-08-14
make gate-ack DATE=2026-08-14
```

Running this chain manually is the proof that the data plane is independent of
both orchestrators.

### Run the control planes

```bash
make run-airflow DATE=2026-08-14
make controlm-build
make controlm-deploy
make run-controlm DATE=2026-08-14
```

`make controlm-build` validates/compiles against the configured development
environment. `make controlm-deploy` changes the connected Control-M tenant, and
`make run-controlm` orders live work there. Deployment or ordering requires
explicit intent; do not use either as an incidental validation step.

`make controlm-service` uses sudo and changes a host systemd service. It is a
one-time host operation, not a normal development command.

### Inject and reverse failures

```bash
make fail-1 STORES=1 DATE=2026-08-14
make fail-1 STORES=8 DATE=2026-08-14
make fail-2 DATE=2026-08-14
make fail-3 DATE=2026-08-14
make fail-4 ROWS=400 DATE=2026-08-14
make fail-5 SECONDS=45
make reset DATE=2026-08-14
```

WMS outcomes are controlled independently with `make wms-ack`,
`make wms-never-ack`, `make wms-late`, and `make wms-reject`. Always run
`make reset DATE=...` after a failure exercise. Reset is designed to restore
snapshotted stock, normal modes, standard inputs, conformed data, and date-scoped
acknowledgements.

### Validate changes

```bash
make lint
make test
```

`make lint` compiles Python, validates the Control-M workflow JSON, validates the
Compose model, and checks shell syntax. `make test` includes lint and runs the
contract suite inside the toolbox image. The toolbox image is normally built by
`make up`; prepare the stack first if it is not present.

Use proportionate integration checks in addition to those fast checks:

- Python gate, naming, or seed changes: extend `tests/test_demo_contracts.py` and
  run `make test`.
- Compose or image changes: run `make lint`, `make up`, and `make health`.
- Stage/schema changes: prepare a date, run the affected upstream and downstream
  stages, rerun them to prove idempotency, then run `make reset` and `make health`.
- dbt changes: run `make dbt DATE=...` against prepared data and inspect both model
  results and tests.
- Airflow changes: verify the DAG loads in Airflow and complete a manual run.
- Control-M workflow changes: run `make controlm-build` when the authenticated CLI
  is available. Do not substitute deployment for validation.
- Failure changes: prove both the intended failing state and a successful reset.

## Development rules for agents

1. Preserve data-plane independence. Put reusable business logic in `demo/` or
   dbt, and keep Airflow/Control-M files as orchestration adapters.
2. Preserve rerun safety. New writes need a deterministic natural key, an upsert,
   or an explicit trading-date replacement rule. Test the same date twice.
3. Thread the trading date explicitly through commands and templates. Python uses
   `YYYY-MM-DD`; Control-M filenames and `%%DEMO_DATE` use `YYYYMMDD`. Do not confuse
   the replay data date with Control-M's live order/business date used by the SLA.
4. Keep gates observable and policy-oriented. Log/emit expected counts, actual
   counts, decisions, missing inputs, and useful failure reasons.
5. Validate schemas before destructive partition operations. The ASN contract must
   fail before silver data for that date is deleted or replaced.
6. Keep stage-run metadata accurate. Work wrapped by `stage_run` must finish as
   `SUCCESS` or `FAILED` with useful row counts/messages.
7. Maintain both orchestration adapters when a shared stage name, command, job ID,
   path, or contract changes.
8. Use pinned dependencies and image versions deliberately. If a version changes,
   update the relevant requirements/Docker/Compose files and the claims in the
   README where necessary.
9. Prefer existing Make targets over undocumented raw container commands. Add or
   update `make help` descriptions when introducing operator actions.
10. Keep shell scripts in strict mode (`set -euo pipefail`), quote expansions, and
    retain explicit date validation at host/container boundaries.
11. Keep JSON files valid and deterministic. Control-M workflow source belongs in
    `controlm/workflows/`; environment substitutions belong in descriptors or
    `controlm/build.py`.
12. Do not hand-edit generated content in `runtime/`, `airflow/logs/`,
    `airflow/config/`, or dbt `target/`/`logs/`.
13. Preserve unrelated user changes in a dirty worktree. Never reset generated or
    persistent demo state unless the requested task requires it.
14. Update documentation when behaviour, commands, ports, demo claims, thresholds,
    topology, or presentation steps change.

## Database and migration caution

`infra/postgres/init.sql` only runs when the Postgres volume is first initialized.
Editing it does not migrate an existing named volume. For schema evolution, make
the intended fresh-install behaviour explicit and either add an idempotent runtime
migration path or document that a deliberate `make clean` is required. Never run
`make clean` automatically just to make a schema edit appear to work.

The schemas have distinct roles:

- `ingress`: immutable/idempotent event landing;
- `bronze`: raw typed inputs plus raw ASN header/rows;
- `silver`: conformed business inputs and seeded reference/history data;
- `staging` and `intermediate`: dbt transformation layers;
- `gold`: business-facing stock, velocity, product, and replenishment models;
- `meta`: demo modes, pipeline-run audit, and WMS-delivery state.

## External and destructive actions

Agents must distinguish local, reversible demo work from external or destructive
operations:

- `make controlm-deploy` mutates the connected SaaS tenant.
- `make run-controlm` orders external work in that tenant.
- `make seed-sla-history` orders many Control-M runs.
- `make controlm-service` changes the host service and requires sudo.
- `make clean` permanently deletes the local Compose volumes.
- `make reset` intentionally mutates demo data for one date, but is the normal,
  reversible recovery mechanism after failure injection.

Do not perform the first five operations merely to inspect, lint, or diagnose a
change. Use read-only configuration inspection, `make lint`, and
`make controlm-build` where appropriate.

## Common pitfalls

- A healthy local Jobs API does not mean Azure Databricks is configured.
- `runtime/` mirrors files for host-side Control-M File Watchers; Azurite remains
  the application file store. Keep both sides aligned when changing file naming.
- Airflow's deliberate omission of WMS acknowledgement and SLA is not unfinished
  work.
- Control-M's host paths currently assume
  `/home/azureuser/retail-data-demo`. A portability change must update the workflow,
  wrapper scripts, service definition, documentation, and deployment assumptions
  together.
- The Airflow admin password is generated under ignored local state. Never put it
  in docs or source.
- `make fail-4` depends on its snapshot for recovery. Do not overwrite or bypass
  the failure/reset pairing.
- dbt retry markers under `runtime/` are date-scoped and are part of Control-M's
  retry-from-point-of-failure story.
- Avoid changing the default Airflow schedule from manual unless the environment
  is deliberately prepared; an automatic schedule can start against stale or
  unseeded data.

## Definition of done

A change is complete when the relevant code, tests, orchestration adapters, and
documentation agree; `make lint` and the applicable tests pass; rerun and reset
behaviour remain safe; generated files and secrets are not committed; and the demo
still tells an accurate story about a shared idempotent data plane, Airflow's data
engineering strengths, and Control-M's end-to-end service ownership.
