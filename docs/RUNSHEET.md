# Presentation runsheet — Control-M, dbt Cloud and Azure Databricks

This is the canonical sequence for the **Trade Day Close to Store Replenishment**
presentation. Use trading date `2026-08-14`, an ordinary Friday with all 325
fictional demo stores expected.

The business thresholds, schedule, estate and integration boundaries are demo
assumptions. Never represent them as Kmart's actual production process.

## What this profile demonstrates

This profile is one end-to-end business service:

1. Compose produces deterministic POS and EOD messages in Redpanda and a supplier
   ASN in Azurite plus the host-visible File Watcher path.
2. A Python projector publishes the 98.0% EOD readiness fact to Kafka; the BMC
   Event Handler creates the matching Control-M event, which converges with the
   ASN File Watcher.
3. Local idempotent landing and contract stages prepare the source snapshot. The
   exact ASN header is validated before the local silver partition can change.
4. Control-M synchronizes the six validated source tables to real Azure
   Databricks Delta tables.
5. Three native Control-M `Job:DBT` jobs trigger pre-existing dbt Cloud jobs:
   `DbtBronze`, `DbtSilver`, and `DbtGold`.
6. The tested Databricks gold result is exported as the deterministic WMS CSV,
   delivered over SFTP, acknowledged, and included in the 06:00 demo SLA.

The three dbt job labels are presentation zones, with this exact mapping:

| Control-M job | dbt selector | Physical dbt resources |
|---|---|---|
| `DbtBronze` | `tag:bronze` | Four `staging` views over validated Delta sources |
| `DbtSilver` | `tag:silver` | Two `intermediate` tables |
| `DbtGold` | `tag:gold` | Four `gold` marts and their tests |

Do not imply that `DbtBronze` consumes Kafka directly. Kafka landing, the ASN
contract and the six-table bridge occur before the dbt jobs. The source Delta
database remains named `silver` because it represents the already-conformed
handoff contract.

The Azure workspace is Standard-tier. This profile uses a dedicated
auto-terminating all-purpose cluster and legacy Hive Metastore, not a SQL Warehouse
or Unity Catalog. The connected dbt tenant rejects `databricks_v0` on Fusion, so
the deployment environment uses the Core `latest` release track.

## One-time preparation

From the repository root, install/authenticate the Databricks CLI and provision
the cluster:

```bash
make install-databricks-cli
databricks auth login \
  --host https://WORKSPACE.azuredatabricks.net \
  --profile retail-demo-azure
make databricks-azure-provision
```

The root `.env` must contain the dbt Cloud account host, account ID and service
token, plus a valid Databricks PAT for the deployment credential. These values are
read at execution time and never written to source or generated state.

Publish only the dbt project to its dedicated deployment branch, then create or
update the dbt Cloud resources:

```bash
make dbt-cloud-publish-controlm
make dbt-cloud-controlm-provision
```

The provisioner is idempotent by name. It configures:

- project subdirectory `dbt/kmart_retail`;
- deployment branch `demo/dbt-cloud-controlm`;
- environment `Azure Databricks Control-M` on Core `latest`;
- a Databricks deployment credential whose token remains encrypted in dbt Cloud;
- jobs `Retail Demo Bronze`, `Retail Demo Silver`, and `Retail Demo Gold`.

It records only resource IDs under ignored `runtime/dbt_cloud/azure.json`.

Store the dbt Cloud service token in the centralized Control-M connection profile:

```bash
make controlm-dbt-trust
make controlm-dbt-provision
```

The trust target idempotently imports the public ISRG Root X1 CA into the host
Application Integrator trust store and retains a backup; it does not disable TLS
verification. Run it when an older Agent otherwise reports `PKIX path building
failed` for the dbt Cloud account URL.

The resulting profile is `FMO_AZURE_DBT`. Installed dbt plug-in version 1.0.01
does not implement Control-M's connection-profile test operation, so the first
`Job:DBT` execution is the effective connection test. This limitation is not a
reason to put the service token in the workflow JSON.

The plug-in also embeds overridden commands directly in a JSON request. Keep YAML
variables single-quoted as rendered by the checked-in workflow; embedded double
quotes make version 1.0.01 produce invalid JSON.

Render the generated dbt job IDs, validate all 12 jobs, and deploy explicitly:

```bash
make controlm-build
make controlm-deploy
```

`controlm-deploy` changes the connected `se-dev` tenant. Do not run it as an
incidental lint step.

Verify or reconcile the local Event Handler before the rehearsal:

```bash
/home/azureuser/controlm-event-driven/scripts/install-handler.sh
/home/azureuser/controlm-event-driven/scripts/status.sh
```

It must show one ready `retail-event-handler` pod. The token remains in the
Kubernetes Secret and must never be shown during the presentation.

## 30–60 minutes before the audience arrives

Run a complete smoke test:

```bash
cd /home/azureuser/retail-data-demo
make demo-controlm-azure DATE=2026-08-14
```

That target starts the stack, seeds reference data, arms a fresh EOD generation,
publishes the dbt deployment branch, provisions dbt Cloud and the Control-M
profile, builds and deploys the workflow, orders one run, and only then publishes
the date's POS/EOD events and ASN. Ordering is asynchronous; monitor the folder
until all 12 jobs end OK.

The first `make demo-ready` immediately after image recreation can briefly report
Airflow health as `starting`. Wait for the API container to become healthy, then
continue with `make health`; do not recreate the stack again.

Expected source counts are:

| Source | Rows / replacement window |
|---|---:|
| Kafka `pos.transactions.v1` | 65,000 messages for the run |
| Kafka `pos.store-eod.v1` | 325 markers for the run |
| Delta `product_master` | 2,000 rows |
| Delta `pos_transactions` | 65,000 for `2026-08-14` |
| Delta `store_eod` | 325 for `2026-08-14` |
| Delta `asn_inbound` | 5,000 for `2026-08-14` |
| Delta `stock_on_hand` | 26,000 for `2026-08-14` |
| Delta `sales_history` | 364,000 for the preceding 28 days |

Verify the operator views before screen sharing:

1. Redpanda Console at `http://localhost:8081`.
2. Azure Databricks Compute and the legacy-Hive `silver`, `staging`,
   `intermediate`, and `gold` databases.
3. dbt Cloud deployment environment `Azure Databricks Control-M` and its three
   latest job runs.
4. Control-M Monitoring for `TradeCloseToReplenishment`.
5. The WMS acknowledgement under `runtime/wms/ack/`.

Never display `.env`, `~/.databrickscfg`, dbt CLI configuration, Control-M CLI
configuration, or generated runtime JSON.

## Live operator sequence

If the smoke test is already complete, start the services, seed reference data,
and arm one fresh deterministic EOD generation. Do not simulate the date yet:

```bash
make postgres-start
make health
make seed
make eod-readiness-arm DATE=2026-08-14
make eod-readiness-status DATE=2026-08-14
```

Show the three EOD readiness topics:

```bash
make kafka-topics
```

Build, deploy and order the prepared workflow before producing source events:

```bash
make controlm-build
make controlm-deploy
make run-controlm DATE=2026-08-14
```

In Control-M Monitoring, show `WaitForStoreEODThreshold` in Wait Condition. Then
publish the live source events and ASN:

```bash
make simulate DATE=2026-08-14
```

In Redpanda Console, open `retail.store-eod-readiness.v1`. The single public
message shows the actual/expected counts, percentage, decision, missing stores,
and `RETAIL_EOD_READY_20260814`. The BMC Event Handler maps that field to
`setevent`; it does not calculate the threshold.

The order wrapper passes both date formats and complete File Watcher paths:

- `DEMO_DATE=20260814` for filenames;
- `DEMO_ISO_DATE=2026-08-14` for dbt variables;
- `ASN_PATH=.../ASN_20260814.csv`;
- `ACK_PATH=.../REPLEN_ACK_20260814.txt`.

Using complete path variables avoids Control-M interpreting the period after an
AutoEdit date variable as part of the variable expression.

In Monitoring, follow this sequence:

1. `WaitForStoreEODThreshold` and `WaitSupplierASN` converge.
2. `LandKafkaEvents` and `ValidateSourceContract` finish idempotently.
3. `SyncDeltaSources` starts the real Azure notebook and verifies all six counts.
4. `DbtBronze`, `DbtSilver`, and `DbtGold` invoke dbt Cloud through the native
   plug-in and wait for each remote result.
5. `ExportAzureOrder` downloads the deterministic CSV from the tested gold table.
6. `DeliverToWMS` writes the file through SFTP.
7. `ConfirmWMSIntake` detects the acknowledgement.
8. `SLA_PickWave` closes the complete service.

The cluster can take several minutes to start after auto-termination. That is
expected, observable runtime—not a reason to bypass the Azure stages.

The download uses a same-directory temporary file followed by atomic replacement,
so it safely replaces a deterministic CSV previously written by a container even
when that older file has a different host owner.

## 90-minute agenda

| Elapsed time | Activity |
|---|---|
| 00:00–00:05 | Business outcome, fictional assumptions and service boundary |
| 00:05–00:15 | Kafka events, EOD policy and supplier ASN gate |
| 00:15–00:25 | Pre-silver contract and idempotent Delta synchronization |
| 00:25–00:45 | Native Control-M dbt Bronze, Silver and Gold jobs |
| 00:45–00:58 | Azure gold export, WMS delivery and acknowledgement |
| 00:58–01:10 | SLA and Control-M operational ownership |
| 01:10–01:25 | One failure scenario and customer discovery |
| 01:25–01:30 | Reset, recap and close |

### Talking points

- The data date is explicit and replayable; Control-M's order date remains the
  live date used by the 06:00 SLA.
- The 319th unique marker makes a 325-store date eligible. A complete day emits
  immediately; an eligible incomplete day waits for a three-second quiet window.
  The compacted Kafka state record, input offset, and public readiness message are
  committed transactionally; Databricks does not need to stay awake as a signal
  ledger.
- The ASN schema is checked before destructive silver replacement.
- Delta windows, dbt tables and the WMS filename are deterministic and safe to
  rerun for the same date.
- Control-M owns dependencies, retries, cross-platform visibility,
  acknowledgement and SLA. It does not own the transformation SQL.
- dbt Cloud owns the model graph and tests; Azure Databricks supplies the compute.
- The service token provisions and triggers dbt Cloud. The separate Databricks
  deployment credential authorizes SQL execution.
- The optional Airflow path remains a separate local control plane. It still uses
  `databricks-local`, Cosmos and Postgres, and intentionally stops at delivery.

The EOD readiness projector and BMC Event Handler do not use Postgres. Keep
Postgres running for the still-local ingress, source-validation, stage-metadata and
WMS acknowledgement components in this transitional profile. The separate
Postgres-isolation proof remains valid only after `make databricks-azure-sync`; it
is not part of this end-to-end Control-M sequence.

## Recommended failure demonstrations

Late-store policy:

```bash
make eod-readiness-arm DATE=2026-08-14
make fail-1 STORES=1 DATE=2026-08-14
make run-controlm DATE=2026-08-14
make simulate DATE=2026-08-14
# 324/325 publishes PROCEED_WITH_EXCEPTIONS.

make reset DATE=2026-08-14
make eod-readiness-arm DATE=2026-08-14
make fail-1 STORES=8 DATE=2026-08-14
make run-controlm DATE=2026-08-14
make simulate DATE=2026-08-14
# 317/325 publishes no readiness event; the Dummy job remains waiting.
```

Run `make reset DATE=2026-08-14` to publish the withheld markers and restore the
normal modes after the hold demonstration.

ASN schema drift:

```bash
make reset DATE=2026-08-14
make fail-3 DATE=2026-08-14
make bronze DATE=2026-08-14
make silver DATE=2026-08-14
```

The silver command must fail before changing the partition. Do not sync that
failed input to Azure.

dbt quality gate:

```bash
make reset DATE=2026-08-14
make fail-4 ROWS=400 DATE=2026-08-14
make databricks-azure-sync DATE=2026-08-14
```

Order the Control-M folder. `DbtSilver` must expose the negative-stock test
failure and prevent Gold/WMS work. Afterwards run `make reset`, resynchronize and
rerun the failed path.

WMS outcomes remain independently configurable with `make wms-never-ack`, `make
wms-late`, and `make wms-reject`.

## Restore and shutdown

Always leave the local date green:

```bash
make postgres-start
make reset DATE=2026-08-14
make health
```

The Azure cluster terminates after its inactivity timeout. Stop Compose while
retaining named volumes with `make down`. Use `make clean` only when deliberately
discarding all local named-volume data; it is destructive.

## Prior rehearsal evidence

The pre-event-driven integrated profile was exercised on `2026-08-10` with data
date `2026-08-14`. This evidence validates the downstream Azure/dbt/WMS path, not
the newly added Event Handler boundary:

- Control-M Run service ID `d18b666c-915a-4c40-aa01-b89408f1456b`, folder
  `IN01:1et9m`: all 12 jobs ended OK.
- Azure source-sync run `657714571780050`: 2,000 products, 65,000 POS rows, 325
  EOD rows, 5,000 ASN rows, 26,000 stock rows, and 364,000 history rows.
- dbt Cloud runs `70506183612595`, `70506183612597`, and `70506183612598`:
  Bronze, Silver, and Gold all succeeded with no failed model or test result.
- Azure export run `661859322459567`: 7,921 deterministic order lines.
- WMS received the 7,922-line file including its header and produced
  `ACCEPTED REPLEN_ORDER_20260814.csv`.

The rehearsal uncovered and fixed three environment-boundary issues now covered
by scripts and tests: the Agent's old CA store, double quotes in the version 1.0.01
plug-in override payload, and replacement of an older root-owned outbound file.
Kafka broker offsets are intentionally cumulative; use the idempotent ingress
counts (65,000 POS and 325 EOD for this date) when proving the logical run contract.
