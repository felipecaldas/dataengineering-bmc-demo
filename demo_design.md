# Kmart DataOps Session 2 — Demo Design

> Historical design record. The implemented Azure Databricks/dbt Cloud profile
> is documented in `docs/ARCHITECTURE.md`; where this file mentions PostgreSQL,
> local emulation, dbt Core or superseded job names, the as-built documentation
> and code take precedence.

**Use case:** Trade Day Close to Store Replenishment
**Stack:** Kafka (local) · Azure Blob file drop · Databricks on Azure · dbt Core (local) · Airflow (local) · Control-M
**Session:** 17 August 2026, 90 minutes

---

## ⚠️ Validate these assumptions first

This design is built on standard Australian discount department store operating patterns. It is credible, but it is not Kmart's actual operation and I have not verified it. Before building, confirm with Sachin:

- Whether replenishment runs on an overnight cycle at all, or intraday
- The real cut-off time and what it feeds (DC pick wave is the assumption here)
- Whether POS data reaches the data platform via event stream or nightly extract
- Whether ASN / supplier inbound data is in the data team's scope
- Store count and state distribution

**If any of these are wrong, the demo lands as a vendor guessing about retail.** If Sachin can give us the real cycle, use it and discard this. The structure below survives substitution — only the business labels change.

---

## 1. Why this use case

Three criteria drove the choice.

**It crosses system boundaries.** A pure in-warehouse dbt transformation chain is Airflow's ideal use case. Demonstrating it with Control-M would tell the room nothing they don't already believe. This pipeline starts with an event stream and a supplier file, and ends with a handoff to a warehouse management system the data team does not own. That is where orchestration stops being a scheduler and starts being a contract between teams.

**It has a hard, expensive deadline.** Not "the dashboard is stale" — a truck departs or it doesn't.

**Every failure mode is one a retail data engineer has personally been paged for.** Nothing invented for effect.

### The business chain

Discount department store retail runs on availability. Thin margins mean lost sales are not recoverable through price. If a fast-moving line is out of stock on a Saturday, that revenue is simply gone.

The overnight cycle:

| Time (AEDT) | Event |
|---|---|
| 21:00 | East coast stores close |
| 00:00 | WA stores close (21:00 AWST) — **trading day is now complete** |
| 00:30 | Supplier ASN file lands (expected DC inbound) |
| 01:00 | Batch begins |
| 01:00–02:30 | Bronze → Silver in Databricks |
| 02:30–04:00 | dbt gold models |
| 04:00–05:30 | Replenishment calculation and allocation |
| 05:30 | Order file delivered to WMS |
| **06:00** | **HARD SLA** — DC pick wave planning starts |
| 06:30 | Pick waves released |
| 10:00 | Trucks depart |
| Next AM | Stores receive |

**Miss 06:00 → pick wave slips → truck misses departure → ~325 stores lose a day of replenishment on their fastest-moving lines.**

That sentence is the anchor for the entire session. Say it early, and refer back to it at every failure.

### The Australian detail worth using

The batch cannot start on a clock. It starts when the trading day is *complete*, and Australia makes that genuinely hard:

- AEDT / ACST / AWST — Perth closes three hours after Melbourne in daylight saving
- State-specific public holidays: Melbourne Cup Day is Victoria only; WA Day, Labour Day and show days all differ by state
- Late-night trading varies by store and by state
- Daylight saving applies in some states and not others, so the AEDT–AWST gap is 2 hours for part of the year and 3 for the rest

So "start the batch at 01:00" is wrong on roughly a third of nights. The correct trigger is *all stores have reported end-of-day for trading day D*, with a timeout.

This is a genuinely good Control-M moment and it is not a manufactured one.

---

## 2. Architecture

### 2.1 The structural point: one data plane, two control planes

Sachin asked for **Airflow vs Control-M (standalone)**. That means we build the same pipeline twice, over identical infrastructure, with a different orchestrator each time. This is the single most important thing to get right — if the two builds share orchestration, the comparison is meaningless.

```
┌─────────────────────── CONTROL PLANE A ────────────────────────┐
│  Apache Airflow (local, Docker)                                 │
│  • custom sensor: store EOD completeness                        │
│  • custom sensor: ASN file arrival                              │
│  • DatabricksRunNowOperator × 2                                 │
│  • Cosmos DbtTaskGroup                                          │
│  • PythonOperator: WMS delivery                                 │
│  NO Control-M involved                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                    both drive the same ↓
┌─────────────────────── DATA PLANE (shared) ────────────────────┐
│  Kafka (local)  ·  Azure Blob  ·  Databricks (Azure)           │
│  dbt Core (local)  ·  WMS stub (local SFTP)                     │
│  Neither orchestrator is referenced by any of these components. │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────── CONTROL PLANE B ────────────────────────┐
│  Control-M (Agent local, Server per environment)                │
│  • Job:Kafka — store EOD completeness                           │
│  • Job:FileWatcher — ASN arrival, WMS ack                       │
│  • Job:Databricks × 2                                           │
│  • Job:dbt                                                      │
│  • Job:FileTransfer — WMS delivery                              │
│  • Job:SLAManagement — 06:00 business deadline                  │
│  NO Airflow involved                                            │
└─────────────────────────────────────────────────────────────────┘
```

**Build rule for implementation:** the data plane must contain zero orchestration logic. No scheduling, no retry policy, no dependency management inside Databricks notebooks, dbt, or the generators. Every notebook is a pure function of its inputs, triggerable by either control plane, idempotent on re-run. If a Databricks notebook decides what runs next, the comparison is contaminated.

### 2.2 Component inventory

| # | Component | Role | Hosting | Driven by A | Driven by B |
|---|---|---|---|---|---|
| 1 | Kafka broker | POS event transport | Local Docker | — | — |
| 2 | Store simulator | Produces POS + EOD events | Local Python | Runs continuously, independent of both | ← same |
| 3 | ASN generator | Drops supplier file | Local Python → Azure Blob | Independent | ← same |
| 4 | Azure Blob | Inbound/outbound file landing | Azure | — | — |
| 5 | Databricks: `bronze_ingest` | Kafka → Delta bronze | Azure job 440 | `DatabricksRunNowOperator` | `Job:Databricks` |
| 6 | Databricks: `silver_conform` | Dedupe, conform, TZ-normalise | Azure job 441 | `DatabricksRunNowOperator` | `Job:Databricks` |
| 7 | dbt Core project | Silver → gold marts | Local, `dbt-databricks` | `DbtTaskGroup` (Cosmos) | `Job:dbt` |
| 8 | Databricks: `replen_calc` | Order line generation | Azure job 447 | `DatabricksRunNowOperator` | `Job:Databricks` |
| 9 | WMS stub | SFTP sink + ack file writer | Local Docker | Python task (write only) | `Job:FileTransfer` + `Job:FileWatcher` |
| 10 | **Airflow** | **Control plane A** | Local Docker | — | absent |
| 11 | **Control-M Agent** | **Control plane B** | Local | absent | — |

Rows 10 and 11 are the demo. Everything else is scaffolding.

### 2.3 Annotated data flow

Orchestration responsibility is shown in the right margin. `[A]` = Airflow, `[B]` = Control-M, `[—]` = neither, runs independently.

```
  Store POS tills (simulated: 325 stores)
        │  transaction events, throughout trading day        [—] continuous
        │  EOD marker per store at store close               [—] continuous
        ▼
  ┌──────────────────────────────────────────────┐
  │ Kafka                                        │
  │   pos.transactions.v1                        │
  │   pos.store-eod.v1                           │
  └──────────────────────────────────────────────┘
        │
        │  ◀── GATE 1: trading day complete?          [A] custom sensor, polls Databricks
        │                                            [B] Job:Kafka + condition
        ▼
  Databricks BRONZE  (Delta, append-only)          [A] DatabricksRunNowOperator job 440
        ▲                                          [B] Job:Databricks
        │
        │  ASN_YYYYMMDD.csv → Azure Blob, ~00:30    [—] dropped by generator
        │  ◀── GATE 2: file arrived?                 [A] custom sensor, polls Blob
        │                                            [B] Job:FileWatcher:Create
        ▼
  Databricks SILVER  dedupe, conform, TZ-normalise [A] DatabricksRunNowOperator job 441
        ▼                                          [B] Job:Databricks
  dbt Core → GOLD                                  [A] Cosmos DbtTaskGroup (10 tasks)
     stock position, sell-through, replen need     [B] Job:dbt (1 job, dbt retry on fail)
        ▼
  Databricks replen_calc                           [A] DatabricksRunNowOperator job 447
     → REPLEN_ORDER_YYYYMMDD.csv to Blob           [B] Job:Databricks
        ▼
  Transfer to WMS SFTP                             [A] PythonOperator
        ▼                                          [B] Job:FileTransfer (MFT)
  ┌──────────────────────────────────────────────┐
  │ WMS stub                                     │
  │  consumes order file, writes ACK             │
  └──────────────────────────────────────────────┘
        │
        │  ◀── GATE 3: WMS acknowledged?             [A] NOT MODELLED — DAG ends at write
        ▼                                            [B] Job:FileWatcher on ack path
  06:00 SLA — DC pick wave                         [A] NOT MODELLED — exists in a runbook
                                                    [B] Job:SLAManagement, forecast from 01:00
```

**The two `NOT MODELLED` lines are the argument.** They are not omissions in Build A — they are outside what an Airflow DAG is structurally able to represent, because both concern state beyond the data platform boundary. Point at them explicitly; do not let them pass as a build shortcut.

### 2.4 The completeness signal — and why it is not 325

Earlier drafts said "325 messages means the trading day is closed." **That is wrong and it is exactly the kind of thing that would get pulled apart in the room.** The expected count is a query, not a constant. It varies because:

- **State public holidays.** Melbourne Cup Day closes VIC stores only. WA Day closes WA only. Show days are regional. On those dates the expected count is lower, and it is lower by a state-specific amount.
- **Store lifecycle.** New store openings, permanent closures, stores dark for refurbishment.
- **Partial trading days.** ANZAC Day morning, Christmas Eve, Boxing Day trading restrictions vary by state.

The correct implementation:

```sql
-- silver.expected_trading_stores(trading_date)
select count(*) as expected_stores
from silver.dim_store s
join silver.trading_calendar c
  on  c.state_code    = s.state_code
  and c.calendar_date = :trading_date
where s.status = 'TRADING'
  and s.open_date <= :trading_date
  and (s.close_date is null or s.close_date > :trading_date)
  and c.is_trading_day = true
```

Requires two seeded reference tables:

```
silver.dim_store          store_id, state_code, status,
                          open_date, close_date, timezone, close_time_local

silver.trading_calendar   calendar_date, state_code, is_trading_day,
                          holiday_name, trading_restriction
```

Seed `trading_calendar` with real Australian state public holidays for the demo year. This is worth doing accurately — if someone in the room spots that Melbourne Cup Day is missing, we lose the retail-credibility point we were trying to make.

**The completeness policy.** Real retail does not hold national replenishment for one store. The rule should be explicit and visible:

| Reporting | Action |
|---|---|
| ≥ 99.5% of expected | Proceed. Log named exceptions. Missing stores replenish on prior-day velocity |
| 98.0% – 99.5% | Proceed with alert to trade ops |
| < 98.0% | Hold. Likely a network or integration incident, not a store issue |

This threshold table improves Failure 1 considerably. Holding back **one** store is 324/325 = 99.7% → proceed with exception. Holding back **eight** stores — a state network link down — is 97.5% → hold. Two different correct answers from the same signal, which is a far better demonstration than a binary present/absent check.

**Demo point:** both builds need this policy. In Build A it is Python the data team writes, tests and owns. In Build B the calendars are declarative configuration and the threshold is a condition. Neither is free; they are owned by different people.

### 2.5 Kafka topics

```
pos.transactions.v1     key = store_id, 6 partitions
  {transaction_id, store_id, till_id, product_sku, qty,
   unit_price_ex_gst, transaction_ts_local, transaction_ts_utc}

pos.store-eod.v1        key = store_id, 3 partitions
  {store_id, trading_date, transaction_count, total_ex_gst,
   eod_ts_local, eod_ts_utc}
```

`store-eod` is the completeness signal for GATE 1. Both control planes must consume it — Airflow indirectly (poll the bronze/silver table), Control-M via `Job:Kafka`.

Publish both local and UTC timestamps. Timezone normalisation is a real silver-layer concern and it makes the AEDT/AWST problem visible in the data rather than only in the narrative.

### 2.6 dbt project

```
dbt/kmart_retail/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── stg_pos_transactions.sql
│   │   ├── stg_store_eod.sql
│   │   ├── stg_asn_inbound.sql
│   │   └── stg_product_master.sql
│   ├── intermediate/
│   │   ├── int_daily_sales_by_store_sku.sql
│   │   └── int_stock_on_hand.sql
│   └── marts/
│       ├── dim_product.sql
│       ├── fct_stock_position.sql
│       ├── fct_sell_through.sql
│       └── fct_replenishment_need.sql
└── seeds/
    ├── dim_store.csv
    └── trading_calendar.csv
```

Ten models — enough for the Cosmos task graph to be visibly non-trivial, small enough to build in under two minutes.

**`fct_replenishment_need.sql`** — the model that produces the money:

```sql
{{ config(materialized='table') }}

with position as (
    select * from {{ ref('fct_stock_position') }}
),

velocity as (
    select
        store_id,
        product_sku,
        avg(units_sold) as avg_daily_units
    from {{ ref('int_daily_sales_by_store_sku') }}
    where sale_date >= dateadd(day, -28, current_date())
    group by 1, 2
),

target as (
    select
        p.store_id,
        p.product_sku,
        p.on_hand_units,
        p.on_order_units,
        v.avg_daily_units,
        ceil(v.avg_daily_units * (p.lead_time_days + p.review_period_days))
            + p.safety_stock_units as target_units
    from position p
    join velocity v
        on  p.store_id    = v.store_id
        and p.product_sku = v.product_sku
    where p.is_active_line
)

select
    store_id,
    product_sku,
    on_hand_units,
    on_order_units,
    avg_daily_units,
    target_units,
    greatest(0, target_units - on_hand_units - on_order_units)
        as replenishment_units
from target
where greatest(0, target_units - on_hand_units - on_order_units) > 0
```

Standard min/max replenishment with velocity-driven targets. Any retail data engineer will recognise it, which is the point — we are demonstrating that we understand their domain, not just our product.

**Tests** (`schema.yml`) — these carry the Data Assurance comparison, so they must exist and must actually fire:

```yaml
models:
  - name: fct_stock_position
    columns:
      - name: store_sku_key
        tests: [unique, not_null]
      - name: on_hand_units
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: true

  - name: fct_replenishment_need
    columns:
      - name: replenishment_units
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 5000
```

### 2.7 WMS stub

A deliberately dumb local service, but it must exist — GATE 3 is one of our two strongest differentiators and it cannot be mimed.

- SFTP endpoint (Docker, `atmoz/sftp` or equivalent)
- Watches `/inbound/replen/`
- On a valid file: waits a configurable delay, writes `/ack/REPLEN_ACK_YYYYMMDD.txt`
- Configurable failure modes: never ack, ack late, reject with error file

The "reject with error file" mode is worth building even if unused — if the room asks "what if WMS rejects it," having the answer live is far stronger than describing it.

---

## 3. Build A — Airflow

This is what the Kmart team would have under Astronomer. **Build it properly and show it fairly.** A strawman here destroys our credibility for the rest of the session.

```python
from datetime import datetime, timedelta
from airflow.sdk import DAG, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

with DAG(
    dag_id="trade_close_to_replenishment",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    @task.sensor(poke_interval=120, timeout=7200, mode="reschedule")
    def wait_for_all_stores_eod(**context) -> bool:
        """Poll silver.store_eod until all trading stores have reported."""
        trading_date = context["ds"]
        expected = get_expected_store_count(trading_date)   # custom: state holidays
        actual = query_databricks(
            f"select count(distinct store_id) from silver.store_eod "
            f"where trading_date = '{trading_date}'"
        )
        return actual >= expected

    @task.sensor(poke_interval=300, timeout=5400, mode="reschedule")
    def wait_for_asn_file(**context) -> bool:
        return blob_exists(f"inbound/ASN_{context['ds_nodash']}.csv")

    bronze_to_silver = DatabricksRunNowOperator(
        task_id="bronze_to_silver",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.databricks_silver_job_id }}",
    )

    dbt_gold = DbtTaskGroup(
        group_id="dbt_gold",
        project_config=ProjectConfig("/opt/airflow/dags/dbt/kmart_retail"),
        profile_config=profile_config,
    )

    replen = DatabricksRunNowOperator(
        task_id="replenishment_calc",
        job_id="{{ var.value.databricks_replen_job_id }}",
    )

    @task
    def deliver_to_wms(**context):
        ...   # write file, then what? see below

    [wait_for_all_stores_eod(), wait_for_asn_file()] >> bronze_to_silver \
        >> dbt_gold >> replen >> deliver_to_wms()
```

### Talk track for Build A

**Give Airflow full credit first.** `DbtTaskGroup` expands all ten dbt models into individual tasks automatically. The dependency graph comes from the manifest. Add a model tomorrow, the DAG grows on next parse. Say out loud that this is elegant and that we do not match it.

**Then point at what we wrote by hand, without editorialising:**

- `get_expected_store_count()` — Australian state public holiday logic, in Python, owned by the data team, tested by the data team, correct at their own risk. Get it wrong on Melbourne Cup Day and the sensor waits for stores that were never going to trade.
- Both sensors time out. Neither distinguishes *late* from *never coming*. At 02:00 the DAG is in the same state whether the file is five minutes away or the vendor's SFTP has been down since Friday.
- `deliver_to_wms` writes a file. Airflow's responsibility ends at the write. Whether WMS consumed it, and whether the pick wave started, is outside the DAG's world entirely.
- Nothing in this DAG knows about 06:00. The SLA exists in a runbook and in people's heads.

**The question to leave hanging:** *"Everything here is solvable — you'd write it. The question is how much of it you want to own."*

---

## 4. Build B — Control-M

⚠️ **Every JSON snippet below is illustrative.** Field names, plug-in names and Flow syntax must be verified against current Automation API documentation before the session. Getting Control-M's own syntax wrong in front of this audience would be worse than any capability gap.

```json
{
  "TradeCloseToReplen": {
    "Type": "Folder",
    "ControlmServer": "CTM-PROD",
    "SiteStandard": "RetailBatch",
    "OrderMethod": "Manual",

    "WaitAllStoresEOD": {
      "Type": "Job:Kafka",
      "ConnectionProfile": "KAFKA-POS",
      "Topic": "pos.store-eod.v1",
      "Description": "Trading day complete when all trading stores report EOD"
    },

    "WaitSupplierASN": {
      "Type": "Job:FileWatcher:Create",
      "ConnectionProfile": "AZURE-BLOB-INBOUND",
      "Path": "inbound/ASN_%%ODATE.csv",
      "SearchInterval": 60,
      "TimeLimit": 90
    },

    "BronzeToSilver": {
      "Type": "Job:Databricks",
      "ConnectionProfile": "DATABRICKS-PROD",
      "DatabricksJobId": "441",
      "IdempotencyToken": "silver-%%ODATE"
    },

    "DbtGold": {
      "Type": "Job:dbt",
      "ConnectionProfile": "DBT-PROD",
      "RunCommand": "build",
      "ActionIfFailure": {
        "Type": "Action:Rerun",
        "RunCommand": "retry"
      }
    },

    "ReplenishmentCalc": {
      "Type": "Job:Databricks",
      "ConnectionProfile": "DATABRICKS-PROD",
      "DatabricksJobId": "447"
    },

    "DeliverToWMS": {
      "Type": "Job:FileTransfer",
      "ConnectionProfileSrc": "AZURE-BLOB-OUTBOUND",
      "ConnectionProfileDest": "WMS-SFTP",
      "FileTransfers": [{
        "Src": "outbound/REPLEN_ORDER_%%ODATE.csv",
        "Dest": "/inbound/replen/"
      }]
    },

    "ConfirmWMSIntake": {
      "Type": "Job:FileWatcher:Create",
      "ConnectionProfile": "WMS-SFTP",
      "Path": "/ack/REPLEN_ACK_%%ODATE.txt",
      "TimeLimit": 20
    },

    "SLA_PickWave": {
      "Type": "Job:SLAManagement",
      "ServiceName": "Store Replenishment",
      "ServicePriority": "1",
      "CompleteBy": { "Time": "0600", "Days": "0" },
      "AverageRunTimeTolerance": "20"
    },

    "flow": {
      "Type": "Flow",
      "Sequence": ["WaitAllStoresEOD", "BronzeToSilver", "DbtGold",
                   "ReplenishmentCalc", "DeliverToWMS",
                   "ConfirmWMSIntake", "SLA_PickWave"]
    }
  }
}
```

### The five things to point at

1. **`SLA_PickWave` is a job in the flow.** The business deadline is a first-class object in the workflow definition, not tribal knowledge. It knows the average runtime of the whole chain and can forecast against it.
2. **`ConfirmWMSIntake`** — the flow does not end when we write the file. It ends when the downstream system acknowledges it. That step has no equivalent in Build A because Airflow's world stops at the data platform boundary.
3. **`%%ODATE`** — one variable carries the trading date through Kafka, Databricks, dbt and file naming, without anyone templating it per operator.
4. **Calendars are declarative.** State-specific holiday calendars are configuration, not the Python function we wrote in Build A.
5. **`ActionIfFailure` → `dbt retry`** — point-of-failure resumption without a per-model job graph.

**Be honest here:** the dbt task group in Build A is a nicer *visual* representation of the dbt DAG than a single Control-M job. Say so. Then argue that the operational outcome — not rebuilding nine healthy models to fix one — is the thing that matters at 04:00.

---

## 5. The five failure scenarios

This is the core of the session — roughly 30 minutes. Run each in both builds, back to back.

### Failure 1 — A Perth store's till is offline

*324 of 325 stores report EOD. One doesn't.*

**Airflow:** the sensor polls for two hours and times out. The DAG fails at 03:00 with `AirflowSensorTimeout`. Someone must decide whether to proceed on 324 stores or wait — and that decision requires knowing whether a missing store materially affects national replenishment. There is no partial-completion concept.

**Control-M:** the completeness condition is not met; the flow holds rather than fails, and SLA impact is calculated immediately. An operator sees "1 of 325 stores outstanding, service still forecast to meet 06:00" and can make an informed call, or a policy can proceed automatically past a defined threshold.

**Talk track:** *"The difference isn't detection — both noticed. It's that one of them can tell you whether it matters."*

### Failure 2 — The supplier ASN file never arrives

*Vendor SFTP has been down since 22:00.*

**Airflow:** the file sensor waits, then times out at 02:30. Log line: file not found. No distinction between *late* and *never*.

**Control-M:** file watch with a time limit fires a specific non-arrival event at 01:30, routes the alert to the vendor management contact rather than the data team, and the SLA job recalculates. If we can get an authenticated Control-M MFT step in the demo, show that the *transfer itself* is orchestrated and monitored, not just its arrival.

**Talk track:** *"This is a supplier problem, not a data engineering problem. Who gets woken up under each model?"*

### Failure 3 — Schema drift on the ASN file

*The vendor adds a `carton_id` column without notice.*

**Airflow:** silver load fails, or worse, succeeds with a silently misaligned column. If it succeeds, downstream dbt tests may catch it at 03:30 — an hour of compute after the fact.

**Control-M with Data Assurance:** the check runs before the load, execution halts, bad data never lands. ⚠️ *Verify Data Assurance's actual schema-drift detection capability before scripting this beat — do not script a demo around a capability that turns out to be a quality-rule engine only.*

**Talk track — and concede here:** *"Your dbt tests would catch this too. The difference is when, and how much compute and how many downstream models you've built on top of bad data by then."*

### Failure 4 — Phantom stock on hand

*Negative `on_hand_units` for ~400 store/SKU combinations after a stock adjustment feed problem.*

This is the most realistic failure in the set. Phantom inventory is endemic in retail and it produces *plausible* wrong answers, not errors.

**Airflow:** `dbt build` runs the `accepted_range` test, fails after the model is built, and the DAG stops. Cosmos means you rerun that model alone — a genuine advantage, show it.

**Control-M:** Data Assurance gates progression on the check, so `ReplenishmentCalc` never starts on bad data, and `ActionIfFailure` → `dbt retry` resumes from the failure point.

**Talk track:** *"Nobody's dashboard broke. The replenishment order would have been wrong for 400 lines and the DC would have shipped it. Which of these stops the truck?"*

### Failure 5 — The forecast miss (the closing beat)

*At 02:10, nothing has failed. Bronze-to-silver ran 22 minutes long because the Databricks cluster was contended.*

**Airflow:** nothing happens. There is no failure. The first anyone knows is 06:05, when the DC calls.

**Control-M:** at 02:10 the SLA job forecasts completion at 06:40 based on historical runtime distribution across the whole chain, and alerts. Then show workload policies raising concurrency on the replenishment job to recover the window, and show the forecast returning to green.

**Talk track:** *"This is the one I'd want you to weigh most heavily. Every other failure in this session was something both tools eventually noticed. This one, Airflow never noticed at all — because nothing failed. Astronomer's own flagship reference, Janus Henderson, built a custom remediation platform on top of Astro to get near this, and they still describe it as reducing time-to-repair rather than predicting."*

---

## 6. Life of a developer — the side-by-side

Two scenarios, roughly 12 minutes. Do these live.

### Scenario A — "It's 03:00 and it broke"

Split screen, same failure (Failure 4).

| | Airflow | Control-M |
|---|---|---|
| Alert | Email / Slack from callback | Alert with SLA impact and forecast |
| First question answered | Which task failed | Whether the 06:00 SLA is still achievable |
| Blast radius | Read the DAG graph | Service view: what business outcome is at risk |
| Fix | Clear task, rerun | Rerun action, or automated recovery rule |
| Downstream | Unknown — WMS is outside the DAG | `ConfirmWMSIntake` still in flow |
| Escalation | Whoever is on the data team roster | Routed by service, to the owning team |

**Then run the MCP beat.** In VS Code or Claude, against the Control-M MCP server:

> *"The store replenishment service is at risk. What's the blast radius and what do you recommend?"*

The agent queries workflow state, identifies the failed check, assesses SLA impact, proposes remediation. Execute it — under existing Control-M RBAC, fully audited.

**The line that matters:** *"The interesting part isn't that an agent can read logs. It's that when it acts, it acts through the same authorisation and audit path as a human operator. For batch that feeds financial reporting, that's the whole question."*

### Scenario B — "Add a second supplier's ASN feed"

**Airflow:** new sensor task, extend the dbt staging model, update the union in the intermediate model, PR, review, merge, deploy. Fast, entirely within the data team's control. **Concede that this is genuinely good.**

**Control-M:** add a file watch job and a Flow entry in JSON, or generate it via `ctm-python-client`, PR, `ctm build` validation in CI, deploy via Automation API with a deploy descriptor for the target environment.

**Show the git diff and the CI run.** Most of this room believes Control-M is a GUI tool. Demonstrating a clean code-review-and-deploy path is one of the highest-value ten minutes in the session, and it is a belief we can change with evidence rather than argument.

**Then ask the discovery question directly:** *"Today, how long does it take you to get a new Control-M job into production, and who does it?"* If the answer involves a ticket and a wait, the problem is delegated administration, not the product — and that is something we can actually fix.

---

## 7. Session runsheet

| Time | Segment | Notes |
|---|---|---|
| 1:00–1:08 | Scope and what we are not claiming | Section 2 of the pre-read, verbally |
| 1:08–1:15 | The business chain and the 06:00 SLA | No product. Retail only. Establish domain credibility |
| 1:15–1:32 | Build A in Airflow, built fairly | Give Cosmos full credit |
| 1:32–1:45 | Build B in Control-M | Including code-as-JSON and CI deploy |
| 1:45–2:12 | The five failures, both builds | The core. Do not let this get compressed |
| 2:12–2:22 | Developer life + MCP | Scenario A, then Scenario B |
| 2:22–2:30 | Roadmap boundary, open questions, next steps | Label roadmap explicitly |

**Protect the failure block.** If earlier segments run long, cut Build A's walkthrough, not the failures. The failures are the only part of this session that Astronomer's own material cannot answer.

---

## 8. Implementation specification

This section is written to be handed to a coding agent. Sections 1–7 are the *why*; this is the *what to build*.

### 8.0 Rules that must not be violated

1. **The data plane contains no orchestration.** No scheduling, retry logic, or "what runs next" decisions inside Databricks notebooks, dbt models, or generators. Every unit is a pure, idempotent function of its inputs.
2. **The two control planes never touch.** Airflow must not call Control-M. Control-M must not call Airflow. If you find yourself building a bridge between them, stop — that is a different demo.
3. **Every stage is idempotent.** Re-running any component for the same `trading_date` must produce the same result. Use `MERGE` on natural keys, not `INSERT`. The entire demo depends on being able to re-run failed steps live.
4. **Every stage is independently triggerable** from a CLI. `make bronze DATE=2026-08-14` must work with no orchestrator running at all. This is what makes both builds possible over the same plane, and it is also how you debug at 11pm the night before.
5. **Failure injection is a single reversible command.** No editing files live in front of the room.

### 8.1 Repository layout

```
kmart-demo/
├── README.md
├── Makefile                      # every operation, one command each
├── docker-compose.yml
├── .env.example
├── infra/
│   ├── kafka/                    # broker + topic creation
│   ├── airflow/                  # Dockerfile, requirements
│   ├── wms-stub/                 # SFTP + ack writer
│   └── controlm/                 # agent config notes
├── generators/
│   ├── store_simulator.py        # POS + EOD events → Kafka
│   ├── asn_generator.py          # supplier file → Azure Blob
│   ├── seed_reference.py         # dim_store, trading_calendar, product master
│   └── seed_history.py           # 28 days of sales history for velocity
├── databricks/
│   ├── notebooks/
│   │   ├── 01_bronze_ingest.py
│   │   ├── 02_silver_conform.py
│   │   └── 03_replen_calc.py
│   └── jobs/                     # job JSON definitions
├── dbt/kmart_retail/             # see 2.6
├── airflow/dags/
│   └── trade_close_to_replenishment.py     # CONTROL PLANE A
├── controlm/
│   ├── workflows/trade_close_to_replen.json # CONTROL PLANE B
│   ├── descriptors/{dev,prod}.json
│   └── build.py                  # ctm-python-client generation
├── failures/
│   ├── f1_late_store.sh
│   ├── f2_no_asn.sh
│   ├── f3_schema_drift.sh
│   ├── f4_phantom_stock.sh
│   ├── f5_slow_cluster.sh
│   └── reset.sh                  # returns everything to green
└── docs/
    └── RUNSHEET.md               # what to type, in order, on the day
```

### 8.2 Environment and versions

⚠️ **Verify current versions before pinning** — this design was written against knowledge current to mid-2026 and the Airflow 3.x / Cosmos / dbt-databricks compatibility matrix moves quickly. Check the Cosmos release notes for the Airflow version you pin.

| Component | Target | Notes |
|---|---|---|
| Airflow | 3.x | Airflow 2 is EOL. Use `apache/airflow` Docker Compose, not Astro CLI — we should not demo our competitor's tooling |
| Cosmos | latest compatible | Must support the pinned Airflow major version |
| dbt Core | 1.9+ | `dbt retry` requires 1.6+; pin higher for safety |
| dbt-databricks | latest | |
| Kafka | Redpanda or Confluent | Redpanda is lighter and single-container; either is fine |
| Python | 3.11 or 3.12 | Match Airflow image |
| Databricks | Azure workspace | Real, external. Serverless SQL warehouse is simplest |
| Control-M | Agent local | Server per environment. ⚠️ Confirm SaaS vs on-prem for the demo |

`.env.example`:

```bash
# Databricks
DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN=
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
DATABRICKS_CATALOG=kmart_demo

# Azure Blob
AZURE_STORAGE_ACCOUNT=
AZURE_STORAGE_CONTAINER=kmart-demo
AZURE_STORAGE_KEY=

# Kafka
KAFKA_BOOTSTRAP=localhost:9092

# Demo control
DEMO_TRADING_DATE=2026-08-14
STORE_COUNT=325
SKU_COUNT=2000
TXN_PER_STORE=200
```

### 8.3 Data volumes

Sized for realism at demo speed, not for scale:

| Dataset | Volume | Rationale |
|---|---|---|
| Stores | 325 across 8 states/territories | Enough for the state-holiday logic to matter |
| Active SKUs | 2,000 | Recognisable retail assortment; fast to build |
| Transactions per store per day | ~200 | 65,000 events per trading day |
| Transaction lines | ~3 per transaction | ~195,000 line items per day |
| Sales history | 28 days, **pre-aggregated** | Do NOT replay 28 days of raw events. Seed `int_daily_sales_by_store_sku` directly — velocity needs the history, nothing else does |
| ASN lines | ~5,000 | Inbound DC deliveries |

The pre-aggregated history point matters: replaying 28 days of raw events through Kafka would take far too long and adds nothing to the demonstration.

### 8.4 Build phases with acceptance criteria

Build strictly in order. Do not begin a phase until the previous one passes.

**Phase 0 — Infrastructure**
`docker compose up` brings Kafka, WMS stub and Airflow to healthy. Databricks and Azure Blob connectivity verified from the host.
*Accept:* `make health` reports all green; a test message round-trips through Kafka; a test blob uploads and lists.

**Phase 1 — Reference data and history**
`seed_reference.py` populates `dim_store`, `trading_calendar` (real AU state holidays for the demo year), and product master. `seed_history.py` populates 28 days of `int_daily_sales_by_store_sku`.
*Accept:* `select count(*) from silver.dim_store` returns 325. The `expected_trading_stores` query returns 325 for an ordinary Tuesday and a correctly reduced number for Melbourne Cup Day.

**Phase 2 — Generators**
`store_simulator.py` produces POS events and per-store EOD markers, honouring each store's local close time and timezone. `asn_generator.py` drops the supplier file to Blob.
*Accept:* a full trading day generates ~65k transaction events and exactly one EOD message per trading store; WA stores emit EOD later in UTC than VIC stores.

**Phase 3 — Databricks bronze and silver**
Three notebooks, each idempotent, each parameterised on `trading_date`, each independently runnable.
*Accept:* `make bronze DATE=...` then `make silver DATE=...` succeeds with no orchestrator running. Running each twice produces identical row counts.

**Phase 4 — dbt project**
Ten models, tests that actually fire.
*Accept:* `dbt build` completes clean. `dbt build --select fct_margin` builds one model. Deliberately corrupting `on_hand_units` makes the `accepted_range` test fail, and `dbt retry` afterwards resumes from that model rather than restarting.

**Phase 5 — WMS stub**
*Accept:* dropping an order file produces an ack within the configured delay. Each configured failure mode behaves as specified.

**Phase 6 — Control plane A (Airflow)**
Full DAG per Section 3. Cosmos configured properly — this must be a fair build, not a strawman.
*Accept:* end-to-end run green. Cosmos renders ten discrete dbt tasks in the graph view. Both sensors work. Clearing a single dbt model task re-runs only that model.

**Phase 7 — Control plane B (Control-M)**
Workflow per Section 4, plus deploy descriptors and a CI validation step.
*Accept:* end-to-end run green with Airflow **stopped**. `ctm build` validates in CI. Deploy to two environments from one definition via descriptors. SLA job present and forecasting.

**Phase 8 — SLA history seeding** ⚠️ **CRITICAL, EASILY FORGOTTEN**
Run the Control-M workflow to successful completion at least 12–15 times across several days before the session, with realistic runtime variation.
*Accept:* the SLA job produces a completion forecast with a meaningful confidence band. Without this history, Failure 5 — our strongest scenario — does not work at all.

**Phase 9 — Failure injection**
Five scripts plus `reset.sh`. Each must be one command, reversible, and leave no residue.
*Accept:* each failure reproduces on demand in both control planes; `reset.sh` returns to green in under two minutes.

**Phase 10 — Rehearsal**
Full session run-through against the runsheet, with an internal sceptic asking the Section 10 questions.

### 8.5 Failure injection contracts

| Script | Mechanism | Reset |
|---|---|---|
| `f1_late_store.sh N` | Simulator withholds EOD for N stores | Release withheld markers |
| `f2_no_asn.sh` | ASN generator skipped for the date | Drop the file |
| `f3_schema_drift.sh` | Generator emits variant with extra `carton_id` column | Revert to standard schema |
| `f4_phantom_stock.sh` | `UPDATE silver.stock_on_hand SET on_hand_units = -12` for ~400 rows | Restore from a snapshot table |
| `f5_slow_cluster.sh` | Inject a sleep into `02_silver_conform` via job parameter | Remove parameter |

`f1` takes a count so you can demonstrate both sides of the 99.5% threshold — one store proceeds with exception, eight stores holds.

`f4` must snapshot before mutating. Do not rely on rebuilding from bronze under time pressure.

### 8.6 Makefile targets

Every operation must be a single command. On the day, nobody should be typing Python.

```
make up / down / health / reset
make seed
make simulate DATE=          # generate a trading day
make bronze DATE=            # each stage independently
make silver DATE=
make dbt DATE=
make replen DATE=
make run-airflow DATE=       # control plane A, end to end
make run-controlm DATE=      # control plane B, end to end
make fail-1 STORES=1
make fail-2 ... fail-5
make seed-sla-history N=15
```

---

## 9. Build checklist

| Component | Effort | Notes |
|---|---|---|
| Kafka local + two topics | 0.5d | Docker Compose; producer script simulating 325 stores |
| Store EOD simulator | 0.5d | Must support "hold back store 312" for Failure 1 |
| ASN file generator | 0.25d | Plus a drifted-schema variant for Failure 3 |
| Databricks bronze/silver jobs | 1d | Small volume; realism matters more than scale |
| dbt project, 10 models + tests | 1.5d | Include `accepted_range` on `on_hand_units` |
| Airflow DAG (Build A) | 1d | Cosmos configured properly — no strawman |
| Control-M workflow (Build B) | 1.5d | Plus deploy descriptors and CI pipeline |
| Data Assurance rules | 1d | ⚠️ Verify capability before committing |
| SLA / BIM configuration | 0.5d | Needs historical runtime data — seed several runs in advance |
| MCP server + client setup | 0.5d | ⚠️ Verify GA status |
| Failure injection scripts | 1d | Each must be a single command, reversible |
| Rehearsal | 1d | Full run-through with an internal sceptic |

**Roughly 10–11 days of build.** With the session on 17 August, this needs a decision on scope now. If the timeline is tight, drop Failures 1 and 3 and keep 2, 4 and 5 — Failure 5 is non-negotiable, it is the strongest thing we have.

**Critical: seed the SLA history.** BIM forecasting needs historical runtime data to predict against. Run the pipeline successfully a dozen times over the preceding days, or the forecast beat will not work. This is the single most likely thing to be forgotten and it kills the closing scenario.

---

## 10. Questions we will be asked

Prepare answers. Rehearse them. Never bluff.

**"How do we backfill 90 days after finding a logic bug?"**
Concede. `dbt build --select` with a date variable, driven by a Control-M cyclic job over a date range, is workable but it is scripting. Airflow's native backfill is better. Ask how often they actually do this — the honest answer may be twice a year.

**"Why can't we just see each dbt model as a task?"**
You can, via `--select` per job, but the graph is hand-maintained and will drift from the manifest. Show `dbt retry` as the better pattern and let them judge. Do not pretend the visual DAG isn't nice.

**"Can we generate the Control-M JSON from the dbt manifest?"**
Yes, via Automation API from CI. Be honest that this is code they would own, which Cosmos gives away.

**"Does Control-M understand Iceberg?"**
No native table-format awareness. We orchestrate the engines that operate on Iceberg tables. ⚠️ Verify current position before answering.

**"Can each engineer get their own environment?"**
Not in Control-M in the way dbt targets provide. Concede cleanly.

**"Isn't this just two tools doing one job?"**
The honest answer depends on their answer to whether Control-M stays for the non-data estate. Do not assert; ask.

---

## 11. What good looks like

Not "they cancel the Astronomer evaluation." Realistically:

- The data team believes BMC understands data engineering and retail operations
- At least one person says some version of *"I hadn't thought about the SLA prediction"*
- They ask a follow-up question, particularly about the WMS handoff or delegated deployment
- Nobody catches us overclaiming

That is achievable in 90 minutes. Winning the authoring comparison is not, and reaching for it is what puts the rest at risk.
