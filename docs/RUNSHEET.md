# Presentation runsheet

This is the operator sequence for a customer presentation. Use trading date
`2026-08-14`, an ordinary Friday with all 325 demo stores expected.

## One-time Control-M preparation — before presentation day

The repository currently contains a validated workflow, but it has not been
deployed by this project. A live Control-M run therefore has one explicit external
prerequisite:

```bash
cd /home/azureuser/retail-data-demo
make controlm-build
make controlm-deploy
```

`controlm-deploy` changes the connected `se-dev` tenant. Run it only after the
tenant owner approves the deployment. Then perform a smoke run and confirm that
the folder is visible in Control-M Monitoring:

```bash
make demo-ready DATE=2026-08-14
make run-controlm DATE=2026-08-14
```

If the SLA forecast is part of the presentation, build the successful-run history
well in advance and confirm that every ordered run has completed:

```bash
make seed-sla-history N=15 DATE=2026-08-14
```

Do not leave deployment, smoke testing or SLA-history generation until the live
session.

## 30–60 minutes before the audience arrives

Start and prepare the whole containerised application:

```bash
cd /home/azureuser/retail-data-demo
make demo-ready DATE=2026-08-14
make kafka-topics
make ps
```

Open these views before screen sharing:

1. Airflow at `http://localhost:8080`.
2. Redpanda Console at `http://localhost:8081`.
3. Control-M Monitoring and Services in the `se-dev` tenant.
4. Two terminals in `/home/azureuser/retail-data-demo`.

Retrieve Airflow's generated local password if required:

```bash
cat airflow/config/simple_auth_manager_passwords.json.generated
```

Run `make health` immediately before the session. All checks must be green.

## 90-minute live agenda

| Elapsed time | Activity |
|---|---|
| 00:00–00:05 | Business outcome and assumptions |
| 00:05–00:13 | Shared data plane and readiness gates |
| 00:13–00:25 | Airflow run and data-engineering strengths |
| 00:25–00:40 | Control-M run and business-service strengths |
| 00:40–00:48 | Side-by-side comparison |
| 00:48–01:05 | One or two failure scenarios |
| 01:05–01:25 | Kmart discovery discussion and questions |
| 01:25–01:30 | Recap, reset and close |

### 1. Set the business context — 5 minutes

Explain the outcome first: after stores close trade, the platform waits for enough
store-completion events and the supplier advance shipping notice (ASN), builds a
replenishment recommendation, sends it to the warehouse management system (WMS),
and confirms intake before the 06:00 pick-wave deadline.

State that the business thresholds and timing are demo assumptions to validate
with Kmart; they are not represented as Kmart's current production process.

### 2. Show the shared data plane — 8 minutes

In Redpanda Console, show `pos.transactions.v1` and `pos.store-eod.v1`. Then run:

```bash
make gate-eod DATE=2026-08-14
make gate-asn DATE=2026-08-14
```

Point out that both gates are ready. The data generators, Kafka, storage,
transformations and WMS are common to both orchestrators; this makes the comparison
about control-plane capabilities rather than two different pipelines.

### 3. Run control plane A: Airflow — 12 minutes

Return the date to a known green state and trigger the DAG:

```bash
make reset DATE=2026-08-14
make run-airflow DATE=2026-08-14
```

In the Airflow graph, show:

- the two rescheduling sensors converging on bronze;
- the three actual `DatabricksRunNowOperator` tasks;
- the Cosmos expansion of ten dbt models and their tests;
- the WMS delivery task as Airflow's end of responsibility.

Be precise about the Databricks tasks: the real open-source Airflow provider is
being exercised, but its connection points to the local `databricks-local`
Jobs API surrogate. The demo does not contact Azure Databricks or simulate Spark,
Delta Lake, clusters, Azure authentication or Azure runtime performance.

Airflow's strongest story here is data-team productivity: Python authoring, a rich
provider ecosystem, excellent dbt/Cosmos lineage, and detailed per-model operation.

### 4. Run control plane B: Control-M — 15 minutes

Reset the shared data plane. Optionally stop Airflow to prove that Control-M does
not invoke or depend on it, then order the same trading date:

```bash
make reset DATE=2026-08-14
make airflow-stop
make run-controlm DATE=2026-08-14
```

Follow the ordered folder in Control-M Monitoring and show:

- store-completeness and ASN gates converging on bronze;
- native File Watcher jobs for ASN arrival and WMS acknowledgement;
- the same bronze, silver, dbt, replenishment and delivery stages;
- `SLA_PickWave` defining the complete business service through WMS acceptance;
- the 06:00 service forecast and the likely critical path, if history was prepared.

After the run, restart Airflow:

```bash
make airflow-start
```

Control-M's strongest story is end-to-end operational ownership across data and
non-data boundaries: agent execution, native file events, downstream confirmation,
central monitoring, service forecasting and business-SLA management.

### 5. Compare the control planes — 8 minutes

Use the completed runs to make the distinction concrete:

| Question | Airflow | Control-M |
|---|---|---|
| Where does it shine? | Python/data engineering, providers, dbt/Cosmos detail | Cross-platform operations, events, service ownership, SLA prediction |
| End point in this demo | Order written to WMS | WMS acknowledgement received and service measured |
| Does either own the transformations? | No; both invoke the same idempotent stages | No; both invoke the same idempotent stages |

The message is coexistence, not replacement: Airflow remains a strong data
orchestrator while Control-M governs the wider business service.

### 6. Demonstrate one or two failures — 17 minutes

Choose scenarios based on the audience rather than attempting all five live.

For the policy discussion, one late store proceeds with a named exception, whereas
eight late stores hold the run:

```bash
make reset DATE=2026-08-14
make fail-1 STORES=1 DATE=2026-08-14
make gate-eod DATE=2026-08-14

make reset DATE=2026-08-14
make fail-1 STORES=8 DATE=2026-08-14
make gate-eod DATE=2026-08-14
```

The final command deliberately exits non-zero because `HOLD` is a failed gate.

For a data-contract failure, show that an unannounced ASN column is rejected before
silver changes:

```bash
make reset DATE=2026-08-14
make fail-3 DATE=2026-08-14
make bronze DATE=2026-08-14
make silver DATE=2026-08-14
```

The silver command deliberately fails. Do not wait through the 90-minute ASN
non-arrival scenario live; demonstrate its immediate state with `make fail-2`
followed by `make gate-asn`, and explain how each orchestrator owns the wait and
timeout.

### 7. Kmart discovery discussion — 20 minutes

Use what the audience just saw to validate the assumptions in the scenario:

- Is replenishment primarily overnight, intraday, or both?
- What are the real store-close and distribution-centre cut-off times?
- Which platforms currently own POS completeness and supplier ASN arrival?
- What constitutes successful WMS intake: file receipt, validation or pick-wave
  creation?
- Where do Airflow, Azure Databricks and Control-M already operate today?
- Which team owns an end-to-end missed service deadline and its escalation?
- For the next iteration, should the portable local Jobs API surrogate remain, or
  should the demo add a separately configured real-Azure Databricks profile?

### 8. Recap and close — 5 minutes

Reinforce three points: both orchestrators used the same idempotent data plane;
Airflow exposed excellent data-engineering and dbt detail; Control-M extended
ownership through downstream acknowledgement and a forecastable business SLA.

Always return the demo to green and confirm health before ending the presentation:

```bash
make reset DATE=2026-08-14
make health
```

## Optional failure appendix

Run these only when time and audience interest permit:

1. `make fail-2` removes and suppresses the ASN. `make gate-asn` reports it absent
   immediately; Airflow and Control-M retain their configured 90-minute wait story.
2. `make fail-4 ROWS=400`, followed by `make dbt`, demonstrates quality-test
   failure. After `make reset`, `make dbt-retry` resumes from the prior dbt failure.
3. `make fail-5 SECONDS=45` adds contention without causing a technical failure;
   use prepared Control-M history to show the SLA forecast move.
4. `make wms-never-ack`, `make wms-late` and `make wms-reject` separate successful
   data delivery from successful business intake.

## Post-session shutdown

Stop the containers without losing seeded state when the session is over:

```bash
make down
```

Use `make clean` only when deliberately discarding all named volumes.
