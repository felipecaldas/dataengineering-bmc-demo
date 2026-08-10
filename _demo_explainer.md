# Trade Day Close to Store Replenishment — Demo Explainer

This is how I would present the demo to the Kmart data team.

## The opening

Before we start, this is an industry-informed retail scenario, not a claim about
Kmart's current replenishment process. We would first validate the actual cut-off,
store calendar, ASN ownership, and WMS hand-off.

The technical question is still valid regardless of those details:

> How do we operate a critical data pipeline when its business outcome crosses
> stores, suppliers, data platforms and a warehouse management system?

The demo follows one trading day from store close through to a replenishment order
accepted by the WMS.

The business deadline is 06:00, when DC pick-wave planning begins. Missing it could
delay picking, truck departure and store replenishment.

The important point is that "the dbt models completed" is not the business outcome.

The outcome is:

> The correct replenishment order was accepted by the WMS before the pick-wave
> deadline.

## One data plane, two independent control planes

Both Airflow and Control-M operate exactly the same underlying pipeline:

```text
Store POS events ──> Kafka ──> Bronze ──> Silver ──> dbt Gold
                                  ▲                      │
Supplier ASN ──> Azure Blob ──────┘                      │
                                                         ▼
                                               Replenishment order
                                                         │
                                                         ▼
                                                     WMS SFTP
                                                         │
                                                         ▼
                                                   WMS ACK/REJECT
```

The local demonstration contains:

- 325 stores across all eight Australian jurisdictions
- 2,000 products
- 65,000 POS events for one trading day
- 5,000 supplier ASN lines
- 28 days of sales history for velocity calculations
- Ten dbt models and 17 quality tests
- A replenishment output containing 7,921 order lines
- A real SFTP boundary with configurable acknowledgement behaviour

The two orchestrators never call each other. This is important: we are comparing
two independent control planes over one shared, idempotent data plane.

## Why Australian store close is not a cron expression

A naive batch might start at 01:00 every night. That is not a reliable definition
of "trading is complete."

Perth closes later in UTC than Melbourne. State holidays differ. Melbourne Cup Day
affects Victoria but not the rest of Australia. Store openings, closures and
refurbishments also change the expected count.

The demo therefore calculates the expected stores for the specific trading date.

On an ordinary day:

```text
Expected stores: 325
```

On Melbourne Cup Day:

```text
Expected stores: 265
```

The missing 60 are the Victorian stores that are closed under the demo calendar
policy. The pipeline does not wait for events that should never arrive.

The completeness policy is:

| Reporting percentage | Decision |
|---|---|
| At least 99.5% | Proceed, recording named exceptions |
| 98% to 99.5% | Proceed with a trade-operations alert |
| Below 98% | Hold: likely an integration or network incident |

This gives us two meaningful outcomes:

- One missing store: 324/325, or 99.692% — proceed with an exception.
- Eight missing stores: 317/325, or 97.538% — hold the national run.

This is much more realistic than simply asking whether 325 messages exist.

## Control plane A: Airflow

We first run the pipeline in Airflow 3.3.

The DAG begins with two rescheduling sensors:

- Store EOD completeness
- Supplier ASN arrival

When both are ready, the flow continues:

```text
EOD sensor ──────┐
                 ├──> Bronze ──> Silver ──> dbt ──> Replenishment ──> WMS delivery
ASN sensor ──────┘
```

The three processing stages use the actual open-source Airflow Databricks provider
and `DatabricksRunNowOperator`, but the configured connection deliberately points
to the containerised `databricks-local` Jobs API surrogate—not to an Azure
Databricks workspace. The surrogate implements only the Jobs API interactions
needed by this demo and executes local Python/Postgres stages. It is not Spark,
Delta Lake, an Azure cluster, Azure authentication or a performance simulation.
No host Airflow connection, token or workspace setting was imported or reused.

The dbt portion uses Cosmos. This is where Airflow genuinely shines.

Cosmos expands the ten-model dbt project into separate Airflow tasks:

- Four staging models
- Two intermediate models
- Four gold models
- Associated tests after the appropriate models

The complete successful Airflow run contains 26 tasks, all green.

### What I would highlight positively about Airflow

Airflow is excellent when the data engineering team owns the workflow.

Its strengths are clear:

- Python-native authoring
- Rapid development by data engineers
- Excellent dbt integration through Cosmos
- A visually useful per-model graph
- Granular task logs and retries
- Dynamic task generation and mapping
- Strong ecosystem of data-platform providers
- Straightforward source control and code review

If a dbt model fails, the engineer can see exactly which model and test failed.
Healthy branches remain visible. This is a significant usability advantage over
presenting dbt as one opaque batch job.

We should say that clearly. The comparison loses credibility if we pretend Airflow
is weak at data orchestration.

### The boundary we expose

The Airflow DAG intentionally finishes after writing the order to WMS.

Airflow could absolutely be extended with another sensor for the WMS
acknowledgement. It is not technically incapable of doing that. The real questions
are:

- Does the data team want to own that external-system sensor?
- Who owns escalation when the WMS rejects a file?
- How is the 06:00 business deadline represented?
- How do operators know the likely business impact before a task fails?
- How much custom SLA and remediation logic should be built around the DAG?

Airflow can implement much of this, but generally through additional DAG code,
callbacks, alert integrations and organisational conventions.

## Control plane B: Control-M

We then show the same pipeline as a Control-M SMART folder.

The workflow contains nine validated jobs:

```text
WaitAllStoresEOD ─────┐
                      ├──> BronzeIngest
WaitSupplierASN ──────┘
                              │
                              ▼
                         SilverConform
                              │
                              ▼
                            DbtGold
                              │
                              ▼
                    ReplenishmentCalc
                              │
                              ▼
                       DeliverToWMS
                              │
                              ▼
                    ConfirmWMSIntake
                              │
                              ▼
                        SLA_PickWave
```

All jobs run through the enrolled `fmo-azureuser` Agent on this machine.

The workflow has passed authoritative `ctm build` validation against `se-dev`:

- One SMART folder
- Nine jobs
- Valid development deploy descriptor

### What is stronger in the Control-M representation

#### 1. The business deadline is a first-class object

`SLA_PickWave` represents the 06:00 business deadline.

Control-M can use historical runtime information across the service to forecast
whether the deadline is at risk—even while every technical job is still running
successfully.

That is different from waiting for a task to fail.

The operational question becomes:

> Are we still likely to meet the 06:00 pick-wave commitment?

rather than:

> Which task is red?

That distinction is particularly important for workload contention, unexpectedly
long cluster startup, supplier lateness and downstream delays.

#### 2. The workflow continues across ownership boundaries

The Control-M service does not finish when the data team writes a file.

It waits for:

```text
REPLEN_ACK_YYYYMMDD.txt
```

from the WMS.

The WMS can:

- Acknowledge normally
- Never acknowledge
- Acknowledge late
- Explicitly reject the order

Control-M therefore represents the end-to-end service outcome, including a system
outside the data platform.

#### 3. Non-arrival is represented explicitly

The ASN and WMS acknowledgement are native File Watcher jobs with defined time
limits.

An absent supplier file is therefore visible as a specific upstream non-arrival
condition, rather than only as a generic transformation failure later in the night.

This supports clearer ownership:

- Supplier file absent: supplier/integration ownership
- Schema contract failed: data contract ownership
- dbt quality test failed: data engineering ownership
- WMS rejected: downstream fulfilment/WMS ownership
- SLA forecast late: service or operations ownership

#### 4. Centralised operational control

Control-M's strength is not writing SQL or replacing dbt. Its strength is
coordinating heterogeneous work:

- Files
- Commands
- Data platforms
- Application jobs
- Transfers
- Downstream acknowledgements
- Business service deadlines

That gives central operations one service view and one audited execution model
across multiple teams.

#### 5. Workflow as code

This is not a GUI-only demonstration.

The workflow is JSON, with separate environment descriptors. It is validated with:

```bash
make controlm-build
```

The same definition can be transformed for different environments without
hand-editing every job.

No tenant credentials are stored in the repository, and deployment remains a
separate explicit action.

## The five failures

This is the core of the presentation.

### Failure 1: stores have not closed cleanly

First, withhold one EOD marker:

```text
324 / 325 = 99.692%
Decision: PROCEED_WITH_EXCEPTIONS
```

The pipeline continues, but store 325 is identified as the exception.

Then withhold eight:

```text
317 / 325 = 97.538%
Decision: HOLD
```

This demonstrates that orchestration needs business policy, not only technical
presence or absence.

Both Airflow and Control-M use the same policy in this demo. The difference is how
the result is operated and how its SLA impact is presented.

### Failure 2: supplier ASN never arrives

The ASN is removed and generation is suppressed.

Airflow's ASN sensor remains waiting and eventually times out.

Control-M's File Watcher records a specific non-arrival against the upstream gate.
The service forecast can then show whether there is still recovery time before
06:00.

The discussion is not "which tool can detect a missing file?" Both can.

The discussion is:

> Who is alerted, what business service is affected, and how much recovery time
> remains?

### Failure 3: ASN schema drift

The supplier adds a `carton_id` column without notice.

Bronze accepts the raw file, preserving its original structure. Silver validates
the schema before changing the conformed tables and fails with:

```text
ASN schema contract failed:
added=['carton_id']
```

This prevents partially loaded silver data.

We should be honest here: this is an explicit data contract implemented by the
demo. We are not claiming native Control-M Data Assurance schema-drift functionality
that has not been configured and verified.

Airflow and Control-M both detect the same failure. Control-M's advantage is the
wider service context around the failure, not ownership of the validation rule
itself.

### Failure 4: phantom stock

We inject negative stock into 400 store/SKU positions.

The dbt accepted-range test fails:

```text
on_hand_units must be at least zero
Failure rows: 400
```

This is where Airflow/Cosmos looks particularly good. The failed test is visible
beside the individual model, while unrelated models remain green.

The Control-M representation is deliberately one dbt job, so it has less attractive
per-model visualisation. However, on rerun it invokes `dbt retry`.

The validated recovery resumed eight affected nodes—the failed quality gate and
skipped descendants—instead of rebuilding all 27 nodes.

The fair comparison is:

- Airflow/Cosmos: better visual representation of dbt lineage and per-model
  execution.
- Control-M: dbt remains responsible for dbt recovery, while Control-M manages the
  surrounding business service and downstream impact.

### Failure 5: nothing fails, but the service will be late

We inject contention into Silver. The task still succeeds, but it takes longer than
normal.

Airflow sees a running task and then a successful task. Without extra SLA logic,
there is no technical failure to alert on.

Control-M's intended demonstration is that historical service runtime predicts a
06:00 miss before the chain fails. Operations can then intervene while recovery time
remains.

This is potentially the strongest Control-M moment—but it requires honesty: after
deployment, we must seed and verify approximately 12–15 successful historical runs
before relying on the forecast in front of a customer.

## The fair conclusion

I would not conclude that "Control-M is better than Airflow."

I would conclude:

> Airflow is a very strong data orchestration framework. Control-M is an enterprise
> service orchestration platform whose scope can extend beyond the data platform.

If the workflow is primarily:

```text
warehouse transformation ──> dbt ──> analytics
```

Airflow and Cosmos are likely the more natural developer experience.

If the business outcome is:

```text
stores closed
+ supplier delivered
+ data processed correctly
+ WMS accepted the result
+ DC deadline will be met
```

Control-M's cross-platform control, downstream acknowledgement, operational
ownership and SLA forecasting become much more valuable.

The demo is not asking Kmart to give up dbt, Databricks or data engineering
practices. Those remain the data plane.

It asks a different question:

> Where should responsibility for the end-to-end retail service begin and end—and
> which team should own the operational code required to bridge all those systems?

The detailed operator sequence is in `docs/RUNSHEET.md`, with the implementation
boundaries in `docs/ARCHITECTURE.md`.
