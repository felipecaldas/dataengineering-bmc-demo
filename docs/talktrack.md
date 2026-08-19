# 23-minute demo talk track — Airflow and Control-M

This is the presenter script for **Trade Day Close to Store Replenishment**.
It is written for a technically experienced retail data-engineering audience.
The timed presentation is exactly 23 minutes:

| Time | Section |
|---:|---|
| 00:00–03:00 | Business process, architecture and purpose |
| 03:00–08:00 | Airflow run: strengths and limitations |
| 08:00–23:00 | Control-M run: end-to-end operational ownership |

Use trading date `2026-08-14` throughout. The retailer, store estate, thresholds,
calendar, volumes and SLA are fictional demo assumptions. Do not describe them as
Kmart production facts.

## Numbered command sequence

The Makefile wraps each multi-command block in this talk track. Supply another
valid trading date with `DATE=YYYY-MM-DD` whenever required.

| Command | Purpose |
|---|---|
| `make step0` | One-time interactive authentication and external provisioning |
| `make step1 DATE=...` | Full Airflow rehearsal and cloud warm-up |
| `make step2 DATE=...` | Prepare the live Airflow source state |
| `make step3 DATE=...` | Trigger Airflow and publish the live inputs |
| `make step4 DATE=...` | Prepare and order the delayed-ACK Control-M run |
| `make simulate DATE=...` | Release the Control-M inputs at the presenter cue |
| `make step5 DATE=...` | Restore green state, health-check and stop Compose |
| `make step6 DATE=... ROWS=400` | Start and run the negative-stock failure demo |
| `make step7 DATE=...` | Recover the failure and restore normal WMS ACK |

The granular Make targets remain available for troubleshooting. The wrappers
stop at the same presenter-controlled boundaries as the original commands; in
particular, `step4` orders Control-M but does not publish its inputs.

## Presenter notation

- **Say** is suggested wording. It is deliberately conversational; do not read it
  like a legal script.
- **Do** is a command or UI action.
- **Show** identifies the screen that should be visible to the audience.
- **Watch for** describes the expected result.
- **If slow** gives a way to keep talking while a remote job is running.

## The one sentence to remember

> Airflow and Control-M orchestrate the same Azure data plane; Airflow gives the
> data-engineering team a strong code-first workflow, while Control-M carries the
> same work into the wider operational service, including external readiness,
> downstream acknowledgement and the business SLA.

## Before the audience joins

These steps are outside the 23-minute presentation.

### One-time or change-driven provisioning

Run these only when the environment, notebooks, dbt project or Control-M workflow
has changed. They create or update external resources.

The ignored `.env` must already contain the Azure, Databricks and dbt Cloud
values described in `docs/OPERATIONS.md`. On a first checkout, run `make prepare`,
populate `.env`, and then use the wrapper; rerunning `prepare` inside `step0` is
harmless.

```bash
make step0
```

`step0` pauses for the configured Databricks browser login. It then creates or
updates Azure Databricks, publishes the dbt deployment branch, creates or updates
dbt Cloud resources, configures the Control-M dbt integration, validates the
workflow and deploys it. These are external changes; do not run `step0` casually
during the presentation.

### Demo-day preflight

Run a complete rehearsal first. This confirms credentials, keeps the Databricks
cluster warm, and proves that the Airflow path completes before the Control-M path
is ordered for the same date.

```bash
make step1 DATE=2026-08-14
```

Confirm that the rehearsal Airflow DAG is complete, then prepare the source state
for the live Airflow run:

```bash
make step2 DATE=2026-08-14
```

Do not start the timed presentation unless both health commands pass and the
rehearsal shows the cloud path can finish within the available window.

### Open and arrange these screens

1. Rendered `docs/ARCHITECTURE.md`, positioned at **Component view**.
2. Airflow at `http://localhost:8080`, already logged in and showing
   `trade_close_to_replenishment`.
3. Control-M Monitoring, filtered to folder `TradeCloseToReplenishment`.
4. A terminal in the repository root with a large, readable font.
5. Optionally, separate authenticated tabs for the two Databricks jobs and the
   three dbt Cloud jobs.

Log in before screen sharing. Never display `.env`, generated runtime JSON,
Airflow passwords, Databricks/dbt tokens, Control-M profiles or connection pages
that reveal credentials.

## Timed presentation

## 00:00–03:00 — Overall demo and retail business process

### 00:00 — Start the Airflow run

The source state was seeded and armed during preflight. Trigger Airflow before
the introduction so its remote work progresses while the architecture is being
explained.

**Do — terminal**

```bash
make step3 DATE=2026-08-14
```

The wrapper triggers Airflow first. Airflow begins by waiting; it then publishes
65,000 POS events, 325 store-EOD markers and the supplier ASN.

**Show**

Switch immediately to the component diagram in `docs/ARCHITECTURE.md`.

### 00:20 — Establish the business problem

**Say**

> The business process is trade-day close to store replenishment. At the end of
> a trading day, a retailer has to establish that stores have closed, capture the
> day's sales, combine that demand with stock, product, history and inbound
> supplier information, calculate replenishment, and get an accepted order to the
> warehouse before the next pick wave.

> This is a common retail pattern because it crosses domains. It starts with
> store events, moves through a cloud data platform, and ends in a downstream
> operational system. A technically successful transformation is not enough if
> the warehouse never receives or accepts the order.

### 00:55 — Walk through the data

**Say**

> For this fictional trading date, the demo represents 325 stores. The default
> generation has 65,000 POS transactions, 325 EOD markers, 2,000 products,
> 26,000 stock positions, 28 days of sales history and 5,000 ASN lines.

> Store completeness is a policy gate. One hundred percent proceeds normally;
> 99.5 percent or above can proceed with exceptions; 98 to below 99.5 percent can
> proceed with a trade-operations alert; below 98 percent is held. Those are demo
> thresholds, not statements about Kmart's process.

### 01:30 — Explain the shared architecture

Point across the diagram from left to right.

**Say**

> Redpanda and the generators simulate upstream systems. ADLS Gen2 is the shared
> object boundary. Azure Databricks validates a six-file manifest before writing
> six Bronze Delta tables. dbt Cloud builds four Silver staging views, two Silver
> intermediate tables and four tested Gold marts. Databricks then writes one
> deterministic replenishment CSV, which is delivered to the WMS simulator.

> Bronze, Silver and Gold are Databricks schemas and objects. There is no
> PostgreSQL business-data path and no local Databricks surrogate. Airflow's
> SQLite file is only its own demo metadata.

> We use real ADLS rather than Azurite so the local adapters and Azure Databricks
> see the same objects. Keeping Azurite would introduce a second object store and
> an extra copy bridge that adds no value to this comparison.

### 02:20 — Explain the purpose of the comparison

**Say**

> The comparison is intentionally fair. Airflow and Control-M call the same two
> Databricks jobs, the same three dbt Cloud jobs and the same delivery adapter.
> Neither orchestrator owns a different transformation implementation, and
> neither calls the other.

> The deliberate difference is scope. Airflow ends when data engineering has
> delivered the order. Control-M continues until the warehouse acknowledges it
> and the overall service is measured against a 06:00 pick-wave deadline. The
> intended conclusion is coexistence, not that one product must replace the
> other.

**Transition**

> Let us first look at the workflow from the data-engineering team's point of
> view in Airflow.

## 03:00–08:00 — Airflow: strengths and limitations

### 03:00 — Show the live DAG

**Do — Airflow UI**

Open the newest run of `trade_close_to_replenishment`, select Graph view, enable
auto-refresh, and fit the whole graph on screen.

Follow this path:

1. `validate_cloud_configuration`
2. `wait_for_store_eod_threshold` and `wait_for_supplier_asn`
3. `stage_inputs_to_azure`
4. `databricks_ingest_bronze`
5. `dbt_stage` → `dbt_intermediate` → `dbt_gold`
6. `databricks_export_replenishment`
7. `deliver_order_to_wms`

**Say**

> The DAG makes the dependency graph immediately understandable to a data
> engineer. The trading date is explicit, cloud configuration is checked before
> work starts, and the two readiness conditions converge before any ingestion.

> The sensors use reschedule mode, so waiting does not occupy a worker slot. Once
> the inputs are ready, the DAG uses provider-native Databricks and dbt Cloud
> operators rather than hiding remote execution behind generic shell commands.

### 04:10 — Show task-level evidence

**Do — Airflow UI**

Open the log for whichever task is active or most recently completed. Prefer, in
this order:

- `databricks_ingest_bronze`, to show the remote job ID and polling;
- one of the three dbt tasks, to show the dbt Cloud run;
- `stage_inputs_to_azure`, to show the date, manifest and source counts.

**Say**

> This is where Airflow is strongest in this comparison: code-first workflow
> authoring, Git review, Python extensibility, task-local logs, retries, and
> provider integrations that are natural to a data-engineering team.

> The transformation logic still lives in Databricks and dbt. That separation
> keeps this DAG an orchestration definition and lets the same data plane be
> driven elsewhere without duplicating the business logic.

### 05:20 — Call out the engineering controls

**Say**

> The landing step snapshots only the active simulation generation and publishes
> the manifest last. The Databricks ingest validates the exact ordered headers,
> row counts, checksums, required values, natural-key uniqueness and date windows
> before its first Delta write. Same-date reruns replace deterministic windows,
> so a retry converges rather than appending duplicates.

> Splitting dbt into Stage, Intermediate and Gold gives us visible quality
> boundaries. If a test fails, export and delivery do not proceed.

### 06:15 — Be candid about Airflow's limitations here

**Say**

> Airflow can certainly be extended beyond this DAG, so these are not claims that
> Airflow is incapable. The limitation is the operational scope we have built and
> assigned to the data-engineering team.

> In this implementation, Airflow knows that it placed a file on SFTP, but its
> workflow ends there. It does not own the warehouse acknowledgement, a native
> cross-platform service view, or the 06:00 business SLA. Adding those concerns
> would mean more sensors, connections, custom operational policy and ongoing
> ownership inside the Airflow platform.

> Its UI is excellent for this DAG, but the unit of visibility remains the data
> workflow. Operations teams often need one service view that also covers event
> readiness, host agents, files, SaaS jobs, downstream acceptance and SLA risk.

> Also, this packaged Airflow uses standalone mode and SQLite. That is appropriate
> for a one-person demo, not a production Airflow architecture.

### 07:20 — Summarise Airflow

**Show**

Return to the whole graph. If the run is complete, point to the green delivery
task. If it is still running, show the most recent successful rehearsal run next
to the active run.

**Say**

> Airflow gives the data team a clear, testable and extensible way to orchestrate
> its cloud data work. That is a real strength. The question for the next section
> is what happens when this pipeline is only one part of a wider retail service.

**Safety gate before continuing**

The live Airflow run must be complete before resetting and ordering Control-M for
the same date. Never run the two control planes concurrently against the same
date; both intentionally replace the same deterministic targets.

If Airflow is unexpectedly still active, keep the last completed run on screen
and finish the current run before starting the commands below. Do not trade data
integrity for the stopwatch.

## 08:00–23:00 — Control-M: the wider operational service

### 08:00 — Prepare and order the Control-M path

**Do — terminal, after Airflow is complete**

```bash
make step4 DATE=2026-08-14
```

`step4` adds at least a 30-second acknowledgement delay. Delivery still succeeds,
but the ACK File Watcher remains visible long enough to explain. It deliberately
does not run `simulate`; that remains the presenter-controlled release at 09:15.

**Do — Control-M UI**

Open the newly ordered `TradeCloseToReplenishment` folder in Monitoring. Show the
two readiness branches converging on `StageInputsToAzure`.

The green-state reset recreates the standard ASN, so `WaitSupplierASN` may turn
green quickly. `WaitForStoreEODThreshold` should remain waiting until the event
simulation runs.

**Say**

> We have ordered the same business date in Control-M before publishing the store
> events. This makes readiness operationally visible rather than burying it in a
> transformation job.

> One branch waits for the Kafka-derived store-completeness event. The readiness
> projector calculates the policy; the BMC Event Handler only maps the neutral
> event into Control-M. The second branch is a native File Watcher for the
> supplier ASN copy visible to the enrolled host Agent. Missing stores and a
> missing supplier file are therefore distinct upstream conditions.

### 09:15 — Release the business inputs

**Do — terminal**

```bash
make simulate DATE=2026-08-14
```

**Show**

Return to Control-M Monitoring and keep auto-refresh on.

**Watch for**

- `WaitForStoreEODThreshold` becomes eligible after the completeness event.
- `WaitSupplierASN` is green or completes when the host-visible ASN appears.
- Both dependencies converge on `StageInputsToAzure`.

**Say**

> Control-M does not own or recalculate the retail rule. It consumes the readiness
> signal and shows it in the context of the wider service. That boundary matters:
> business policy stays reusable, while operations gets a clear waiting state,
> timeout and ownership point.

### 10:15 — Explain the Azure landing and Bronze boundary

**Show**

Select `StageInputsToAzure` and then `IngestBronze` as they run or from their
latest output.

**Say**

> The Agent invokes the same landing adapter used by Airflow. It takes a bounded
> Kafka high-watermark snapshot, selects only this simulation ID, verifies all six
> source objects and publishes the manifest last in ADLS Gen2.

> `IngestBronze` then invokes the same pre-provisioned Azure Databricks job that
> Airflow used. The notebook validates the whole contract before changing any
> table. Product is a whole-snapshot replacement; the other sources replace
> explicit Delta date windows. That is what makes reruns safe.

> Notice the division of responsibility: Control-M owns when and under what
> operational conditions the job runs; Databricks owns Spark, Delta and the data
> contract.

### 12:00 — Focus on native dbt Cloud visibility

**Show**

Point to `DbtStage`, `DbtIntermediate` and `DbtGold`. Open a job's details and,
without exposing its credential profile, show the dbt Cloud job identity, status
and output.

**Say**

> These are native `Job:DBT` tasks, not a second implementation of dbt. They call
> the exact same shared dbt Cloud jobs as Airflow: Stage builds Silver views,
> Intermediate builds the conformed Silver tables, and Gold builds and tests the
> four business marts.

> That gives operations a first-class view of the SaaS work inside the whole
> service. A failed dbt test is not just a log line inside a generic command; it
> stops Gold, export and delivery at a named quality boundary.

> `DbtGold` is configured with limited reruns and retry from the point of failure.
> The important operational idea is controlled recovery: retry the failed
> boundary without manually replaying successful upstream ingestion.

Do not claim that every job has the same retry policy; the checked-in workflow
applies this explicit retry behavior to `DbtGold`.

### 14:20 — Explain cross-platform ownership

**Say**

> At this point the workflow has crossed a Kafka event, an Agent-visible file,
> host commands, Azure Databricks and dbt Cloud. Control-M presents those as one
> operational service with one business date and one dependency chain.

> This is the key strength for a central operations team: application boundaries
> do not become monitoring gaps. The data-engineering implementation remains in
> its native tools, while Control-M supplies enterprise scheduling, run control,
> operational ownership and a consistent recovery view.

### 15:30 — Follow export and delivery

**Show**

Point to `ExportReplenishment`, `DeliverToWMS` and `ConfirmWMSIntake`.

**Say**

> `ExportReplenishment` invokes the same Databricks export notebook. It reads the
> tested Gold result, sorts by store and SKU, creates stable order IDs and
> overwrites `REPLEN_ORDER_20260814.csv` in ADLS.

> `DeliverToWMS` transfers that deterministic object to SFTP. This is exactly
> where the Airflow DAG ended. Control-M continues because successful file
> transfer is not the same business outcome as successful warehouse intake.

### 16:45 — Make downstream acknowledgement visible

**Watch for**

`ConfirmWMSIntake` waits while the WMS simulator holds the acknowledgement for at
least 30 seconds, then completes when `REPLEN_ACK_20260814.txt` appears.

**Say**

> We deliberately delayed the WMS acknowledgement. The data pipeline is already
> technically complete, but the retail service is not. The native ACK File
> Watcher makes that distinction visible without changing the Databricks or dbt
> implementation.

> In a failure exercise this boundary can remain waiting or receive an explicit
> rejection. That is operationally different from a transformation failure: the
> owner, evidence and recovery action are different.

If the acknowledgement has already arrived, open the completed File Watcher and
point to its detected path and completion time.

### 18:15 — Show the 06:00 SLA

**Show**

Select `SLA_PickWave` and the service/SLA view available in the connected tenant.

**Say**

> The final boundary is not merely that every job is green. The business service
> is expected to complete before the 06:00 pick wave. Control-M associates the
> cross-platform chain with that service deadline and can identify problematic
> work when history indicates the service is at risk.

> The honest boundary is that useful prediction requires successful history in
> this tenant. The workflow definition creates the SLA structure; it does not
> manufacture a credible forecast without runtime history.

### 19:40 — Summarise Control-M's strengths

**Say**

> The Control-M value shown here is not another SQL engine or another copy of the
> pipeline. It is operational reach: event and file readiness, an enrolled Agent,
> native dbt Cloud work, Databricks commands, downstream acceptance, controlled
> recovery and a business-service SLA in one view.

> It also separates responsibilities cleanly. Data engineers retain Databricks,
> dbt and Airflow where those tools are strongest. Operations can govern the full
> service without asking the data team to rebuild every external dependency as a
> custom DAG concern.

### 20:50 — Acknowledge Control-M's trade-offs

**Say**

> Control-M also has a cost of ownership. It needs an enrolled Agent, connection
> profiles, plug-ins, certificate trust, tenant governance and deployment of the
> workflow definition. For a small data-only pipeline, that can be more machinery
> than a team needs.

> Its value increases when the service crosses many platforms, teams and
> operational boundaries, which is exactly the situation represented here.
> Airflow remains the more natural code-first authoring environment for many data
> engineers; Control-M supplies the wider operational control plane.

### 21:45 — Side-by-side conclusion

**Show**

Keep the completed Control-M folder visible, or return briefly to the architecture
diagram with both control planes on screen.

**Say**

> Both runs used the same ADLS landing, the same Databricks Bronze tables, the
> same dbt Cloud Silver and Gold jobs, the same deterministic export and the same
> WMS adapter. The data results are not the differentiator.

> Airflow gave the data-engineering team a concise, Python-native DAG with strong
> provider integration and task-level observability. Control-M placed that same
> work inside a broader retail service and continued through acknowledgement and
> SLA ownership.

> My conclusion is coexistence. Use Airflow where a data team benefits from
> code-first orchestration close to the pipeline. Use Control-M to coordinate and
> govern the wider business service across data, applications, files, hosts and
> operational deadlines.

### 22:40 — Close

**Say**

> The point of the demo is simple: one governed, idempotent Azure data plane; two
> control planes with different strengths; and a clear hand-off from data
> completion to business completion.

Pause for questions at 23:00.

## Talk while the cloud is running

Remote job timing will vary. Never stare silently at a spinner. Use the row that
matches the currently active Control-M job:

| Active state | Point to make |
|---|---|
| EOD wait | Policy is calculated outside the orchestrator; Control-M exposes the operational wait and timeout. |
| ASN wait | File non-arrival is distinct from store incompleteness and has a different owner. |
| Landing | The adapter snapshots one simulation generation and publishes the manifest last. |
| Bronze ingest | All six files are validated before the first Delta write. |
| dbt Stage | Four source-facing views isolate the date and normalize the Bronze contract. |
| dbt Intermediate | Conformed stock and 28-day sales are tested before business marts. |
| dbt Gold | Four tested marts produce the replenishment need; retry is bounded. |
| Export | Stable sort order, order IDs and filename keep replays deterministic. |
| Delivery | Technical delivery crosses from the data platform into an application boundary. |
| ACK wait | File transfer succeeded, but the business service is still incomplete. |
| SLA | The service deadline spans every preceding technology and ownership domain. |

## After the timed demo

Restore normal WMS behavior and green source state:

```bash
make step5 DATE=2026-08-14
```

`step5` restores normal acknowledgement behavior, resets the date, health-checks
the green environment and runs `make down`. Named-volume state is retained. Do
not use `make clean` unless deleting all local Redpanda, WMS and Airflow state is
intentional.

## Optional Control-M failure extension

Do not put this inside the core 23 minutes. If the audience asks for recovery,
the most useful extension is a business-quality failure because it proves that
transport success does not imply trustworthy output.

### Negative-stock quality failure

Make sure no Airflow or Control-M run is active for the date, then run:

```bash
make step6 DATE=2026-08-14 ROWS=400
```

`step6` starts and health-checks the local stack first, so it is safe to run
after `step5` has stopped Compose.

**Say**

> Bronze accepts these structurally valid rows. The accepted-range test fails at
> the dbt Intermediate quality boundary, so Gold, export and WMS delivery never
> run. Control-M shows the exact failed service boundary and prevents a bad order
> from leaving the data platform.

After the discussion, stop or hold the failed folder and recover:

```bash
make step7 DATE=2026-08-14
```

## Likely questions and concise answers

### Are Bronze, Silver and Gold local tables?

No. They are Azure Databricks schemas and objects. Bronze and the persisted
Silver/Gold models are Delta tables; the four Silver staging models are views.

### Why was PostgreSQL removed?

It created a second business-data implementation and did not provide a useful
dbt Cloud path in this demo environment. Both orchestrators now invoke the same
Databricks and dbt Cloud implementation.

### Why was Azurite removed?

The real Azure Databricks workspace needs cloud-reachable storage. ADLS Gen2 lets
the local adapters and Databricks use the same objects. Keeping Azurite would
require a second object store and a separate copy bridge.

### Is Control-M calculating store completeness?

No. The readiness projector owns the policy and emits an orchestrator-neutral
Kafka event. BMC Event Handler maps that event into Control-M.

### Are Airflow and Control-M running different transformations?

No. They use the same two Databricks job IDs, the same three dbt Cloud job IDs,
the same trading-date contract and the same delivery adapter.

### Could Airflow also wait for the WMS acknowledgement?

Yes. It could be extended with more sensors and SLA logic. This demo deliberately
ends the Airflow DAG at the data-engineering delivery boundary so the comparison
can show Control-M's broader operational ownership.

### Is the 06:00 forecast immediately predictive?

No. The service definition and deadline are real workflow constructs, but useful
prediction needs successful execution history in the connected Control-M tenant.

### Is this a performance benchmark or Kmart production design?

No. The sources and WMS are simulations, the cluster is deliberately small, and
the business facts are fictional. The demo evaluates orchestration boundaries
and operating-model choices, not production sizing.
