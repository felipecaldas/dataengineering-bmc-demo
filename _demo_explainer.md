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

## Shared business contracts, two independent control planes

Airflow and Control-M use the same fictional retail inputs, business rules, dbt
project, trading-date contract and deterministic WMS interface. They never invoke
one another. Their current physical adapters are intentionally different: Airflow
uses the self-contained local profile, while the integrated Control-M path
synchronises validated inputs to real Azure Databricks and invokes dbt Cloud.

The common business flow is:

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
two independent control planes over shared, idempotent business contracts—not
claiming that their current runtime adapters are identical.

Redpanda is the Kafka-compatible operational event platform. It carries simulated
checkout transactions and per-store EOD markers. Azurite represents the
supplier-facing Azure Blob boundary where the ASN arrives, and the ASN is mirrored
to a host-visible path for Control-M's native File Watcher. These systems carry and
retain inputs; they do not schedule the pipeline.

## Why Australian store close is not a cron expression

A naive batch might start at 01:00 every night. That is not a reliable definition
of "trading is complete."

Perth closes later in UTC than Melbourne. State holidays differ. Melbourne Cup Day
affects Victoria but not the rest of Australia. Store openings, closures and
refurbishments also change the expected count.

The Airflow gate calculates the expected stores for the specific trading date from
the fictional store and trading-calendar reference data.

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

The event-driven Control-M presentation is deliberately pinned to `2026-08-14`, an
ordinary Friday when all 325 fictional stores are expected. Its Python projector
currently uses that configured canonical estate for each armed generation; it does
not query Databricks or the local calendar. Do not claim that the Control-M event
path is holiday-aware without first extending its arm contract to carry the
date-specific expected store set.

For the standard date, this gives us two meaningful outcomes:

- One missing store: 324/325, or 99.692% — proceed with an exception.
- Eight missing stores: 317/325, or 97.538% — hold the national run.

The projector counts unique store IDs, so duplicate Kafka deliveries do not inflate
the result. A complete 325-store day emits immediately. An incomplete but eligible
day waits for a three-second unique-marker quiet window before publishing, which
allows a 324-store run to settle as `PROCEED_WITH_EXCEPTIONS` rather than announcing
the transient 319-store state while the final messages are still arriving.

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

The integrated workflow contains twelve validated jobs. Its EOD boundary is now a
Kafka-driven Control-M wait condition rather than a polling command:

```text
store EOD markers -> Python threshold projector -> readiness topic
                                                   │
                                                   ▼
                                   BMC Event Handler setevent
                                                   │
                                                   ▼
                               WaitForStoreEODThreshold ─┐
                                                        ├──> LandKafkaEvents
                               WaitSupplierASN ──────────┘
                              │
                              ▼
                    ValidateSourceContract
                              │
                              ▼
                       SyncDeltaSources
                              │
                              ▼
               DbtBronze -> DbtSilver -> DbtGold
                              │
                              ▼
                      ExportAzureOrder
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

The Dummy wait is evaluated by Control-M itself. Host commands, File Watchers and
plug-in jobs use the enrolled `fmo-azureuser` Agent on this machine.

The workflow has passed authoritative `ctm build` validation against `se-dev`:

- One SMART folder
- Twelve jobs
- Valid development deploy descriptor

### How the Event Handler changes the story

Before a live run, the operator explicitly arms the trading date. Arming creates a
new generation so retained Kafka messages from a previous replay cannot release a
new Control-M order prematurely.

The background Python `eod-readiness` service consumes the arm command and
`pos.store-eod.v1`. For each armed date it records the unique closed-store IDs,
current decision and emission flag in the compacted
`retail.store-eod-readiness-state.v1` topic. The state update and source offset are
committed in one Kafka transaction. On restart, the service restores the latest
state from Kafka; it does not need Postgres or Databricks as an event ledger.

When the policy is ready, the projector publishes one business event to
`retail.store-eod-readiness.v1`. The Kubernetes-hosted BMC Event Handler consumes
only committed messages from that topic and maps the message's deterministic
`event_name`, such as `RETAIL_EOD_READY_20260814`, to `setevent` on Control-M server
`IN01`.

`WaitForStoreEODThreshold` is a Control-M Dummy job waiting for that date-scoped
event. Once satisfied, it ends OK and deletes the consumed Control-M event. The
threshold logic therefore remains in reusable Python business code, the Event
Handler remains a thin integration adapter, and Control-M visibly owns the wait
condition and downstream service.

There is one honest reliability boundary: the application publishes its state and
readiness event transactionally, but BMC documents that the Event Handler itself
does not provide an end-to-end idempotency guarantee. A retry after the API action
but before the handler commits its Kafka offset remains possible. The deterministic
event name/date and single handler replica constrain that residual demo risk; we do
not call the whole chain exactly-once. See BMC's
[Event-Driven Workflows documentation](https://documents.bmc.com/supportu/controlm-saas/en-US/Documentation/Control-M_for_Event-Driven_Workflows.htm).

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

The current three-job Bronze/Silver/Gold representation is an implementation
choice, not a limit on programmatic workflow authoring. The
[Control-M Python Client](https://controlm.github.io/ctm-python-client/) supports
designing, scheduling and running workflows as code, while the native
[`Job:DBT` integration](https://documents.bmc.com/supportu/controlm-saas/en-US/Documentation/API_CodeRef_JobTypes_DataProcessing.htm)
connects Control-M to pre-existing dbt platform jobs. Expanding every dbt node into
the Control-M graph is possible, but would be an additional integration contract to
generate and maintain.

## The five failures

This is the core of the presentation.

### Failure 1: stores have not closed cleanly

First, withhold one EOD marker:

```text
324 / 325 = 99.692%
Decision: PROCEED_WITH_EXCEPTIONS
```

The pipeline continues, but store 325 is identified as the exception.
For the Control-M path, the date is armed before the test messages are published.
After the final unique marker, the three-second quiet window expires, the Python
projector publishes `PROCEED_WITH_EXCEPTIONS`, and the Event Handler creates the
date-scoped Control-M event.

Then withhold eight:

```text
317 / 325 = 97.538%
Decision: HOLD
```

This demonstrates that orchestration needs business policy, not only technical
presence or absence.

At 317 stores, the projector publishes no readiness message. Consequently, the
Event Handler performs no `setevent` action and `WaitForStoreEODThreshold` remains
in Wait Condition. Releasing the missing markers allows the same armed generation
to cross the policy boundary and continue.

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

The current Control-M representation exposes three native dbt Cloud jobs:
`DbtBronze`, `DbtSilver` and `DbtGold`. That is more operational detail than the
earlier single-command version, and Control-M captures each remote run's status and
logs. It still does not automatically expand all ten models and their individual
tests into the Control-M graph; the failed dbt Cloud layer exposes the detailed dbt
result and blocks later layers.

The fair comparison is:

- Airflow/Cosmos: better visual representation of dbt lineage and per-model
  execution.
- Control-M: layer-level dbt Cloud visibility and broader management of the
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

## The ASN's role and honest boundary

ASN means **Advance Shipping Notice**. It is the file a supplier sends before
goods physically arrive, describing what it intends to deliver. Each demo row
contains:

- ASN identifier
- Trading date
- Product SKU
- Expected quantity
- Expected arrival date
- Supplier identifier

The ASN matters here for three reasons:

1. **Business readiness:** the process confirms that expected supplier deliveries
   have been declared before the trading day is closed.
2. **Cross-platform orchestration:** it lets Control-M converge a Kafka-derived
   store-readiness event with an independently arriving supplier file through its
   native File Watcher.
3. **Data-contract protection:** the exact ordered header is validated before the
   conformed source snapshot is synchronised to Azure.

The current replenishment calculation does not directly consume ASN quantities. It
uses:

```text
replenishment = target stock - on-hand stock - on-order stock
```

`on_order_units` currently comes from the stock-position source, not the ASN. The
ASN is therefore a readiness, conformance and observability input in this demo. A
future production-style enhancement could reconcile ASN quantities with
`on_order_units` and use confirmed inbound supply, taking care not to count the
same incoming stock twice.

## Three-to-five-minute demo explanation

This is a fictional, Kmart-style **trade-day close to store replenishment** story.
It is not a claim about Kmart's real process; the estate, thresholds, cut-off and
system boundaries are demonstration assumptions. The business outcome we care
about is not merely that a set of dbt models completed. It is that the correct
replenishment order was accepted by the warehouse management system before the
06:00 distribution-centre pick-wave deadline.

The story begins with two different kinds of upstream input. Redpanda is our
Kafka-compatible event platform. It receives simulated checkout transactions and
one end-of-day marker from each store. Azurite represents the supplier-facing Blob
boundary where an Advance Shipping Notice, or ASN, arrives. Redpanda carries
fast-moving operational events, while the ASN is an independently supplied
business file. Neither platform schedules the workflow; they carry and retain its
inputs.

The key new capability is how store readiness reaches Control-M. Before the live
run, we arm the presentation date so retained messages from an earlier replay
cannot trigger it. A background Python service listens to the store EOD topic,
counts unique store IDs and applies the completeness policy. It persists the
observed IDs and emission state in a compacted Kafka topic, with Kafka transactions
protecting the state, input offset and readiness publication. It does not need
Postgres or Databricks as its event ledger.

For the standard date, 325 stores are expected. At 318 stores or fewer, the
decision is `HOLD` and no readiness event is produced. From 319 stores the date is
eligible to proceed under the policy. A complete 325-store result emits
immediately; an eligible incomplete result waits for a three-second quiet window
so the final decision accurately reflects, for example, 324 out of 325 stores.

The Python service then publishes one business message to the readiness topic. A
BMC Event Handler running in Kubernetes consumes that committed message and maps
its deterministic name, such as `RETAIL_EOD_READY_20260814`, to a Control-M
`setevent` action. In the Control-M workflow,
`WaitForStoreEODThreshold` is a Dummy job visibly waiting for that event. When it
arrives, the job completes and consumes the condition. This is the event-driven
moment: Control-M is no longer running a shell command that polls the EOD gate.

That store-readiness branch converges with `WaitSupplierASN`, a native Control-M
File Watcher. Once both inputs are ready, Control-M lands and validates the source
inputs, including checking the exact ASN schema before the conformed snapshot can
change. Local Postgres still supports this source-ingestion, contract-validation,
metadata and WMS-acknowledgement plane, but it is not the transformation target for
the integrated Control-M path.

Control-M synchronises the validated six-table snapshot into real Delta tables in
Azure Databricks. The current Standard-tier workspace uses a dedicated
auto-terminating all-purpose cluster and the legacy Hive Metastore—not Unity
Catalog or a SQL Warehouse. Control-M then invokes three pre-existing dbt Cloud
jobs through its native `Job:DBT` integration: `DbtBronze` runs the four staging
models, `DbtSilver` runs the two intermediate models, and `DbtGold` runs the four
business-facing marts, with their tests.

The Bronze label needs one qualification: it does not consume Kafka directly.
Kafka landing and source conformance happen before Azure synchronisation, and the
Bronze job creates staging views over that validated Delta handoff. Control-M
monitors each remote dbt Cloud run and prevents downstream work when a model or
test fails.

After Gold succeeds, an Azure Databricks notebook reads
`gold.fct_replenishment_need`, creates deterministic order IDs and exports a
date-specific CSV. Control-M downloads and validates that result, delivers it over
SFTP, waits for the WMS acknowledgement and measures the complete service against
the 06:00 deadline. The 12-job workflow therefore spans Kafka readiness, supplier
file arrival, source validation, Azure synchronisation, dbt Cloud execution, WMS
delivery, downstream acceptance and SLA management.

Airflow remains an independent comparison path. It does not call Control-M, and
Control-M does not call Airflow. Airflow and Cosmos provide excellent model-level
visibility across the same ten-model dbt project, while the current Control-M
implementation provides layer-level dbt Cloud visibility and a wider operational
service boundary. The conclusion is coexistence: Airflow is particularly strong
inside the data-engineering graph, while Control-M governs the cross-platform
business service from external readiness through downstream acceptance and SLA.
