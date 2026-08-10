# Architecture and trust boundaries

```text
                 CONTROL PLANE A
  Airflow sensors -> Databricks operators -> Cosmos/dbt -> SFTP write
                         |                         |
                         +-----------+-------------+
                                     v
                         SHARED DATA PLANE
  store simulator -> Redpanda -> ingress -> bronze -> silver -> gold
  ASN generator  -> Azurite ------------------^             |
  WMS SFTP/ack   <------------------------------------------+
                                     ^
                         +-----------+-------------+
                         |                         |
                 CONTROL PLANE B
  Control-M host Agent -> Compose stage commands -> file watches -> SLA
```

The generators, database transforms, local Jobs API and WMS contain no schedule,
dependency, retry or next-step choice. Continuous Kafka ingress and the WMS watcher
are transport adapters: they observe their own inputs and never order pipeline work.

The host Agent is the one intentional Compose boundary because it is already enrolled
to the SaaS tenant. `controlm/systemd/controlm-agent.service` keeps that identity
running across reboots; stage workloads and all data-plane dependencies remain in
the master Compose application.

## Containers

| Service | Responsibility | Persistent volume |
|---|---|---|
| `redpanda` | Kafka-compatible event transport | `redpanda-data` |
| `redpanda-console` | Topic/operator view | none |
| `azurite` | Azure Blob-compatible file store | `azurite-data` |
| `postgres` | Ingress, bronze, silver, gold, metadata | `postgres-data` |
| `kafka-ingest` | Continuous, idempotent event landing | Postgres |
| `databricks-local` | Jobs API and independently triggerable stages | Postgres/Blob |
| `wms-sftp` | Downstream SFTP boundary | `wms-data` |
| `wms-ack-writer` | Configurable ack/reject behaviour | `wms-data` |
| Airflow services | Control plane A only | Airflow database/log bind |

## Idempotency keys

| Stage | Key / replacement rule |
|---|---|
| Kafka ingress | deterministic event ID |
| Bronze POS | `transaction_id` |
| Bronze/silver EOD | `(store_id, trading_date)` |
| Bronze ASN raw | `trading_date` |
| Silver ASN | replace the trading-date partition; `(asn_id, product_sku)` |
| dbt | tables/views replace atomically |
| Replenishment export | deterministic date path and sorted lines |
| WMS delivery | deterministic filename, overwritten safely |

## Local versus Azure profile

The default profile uses Azurite and a Jobs API-compatible service so a presenter
can start the whole data plane offline with Compose. The files under `databricks/`
and empty Azure variables in `.env.example` preserve the handoff to a real workspace.
Moving to Azure changes adapters and Control-M connection profiles, not stage order,
business gates, dbt lineage, failure contracts or idempotency keys.

Airflow's actual `DatabricksRunNowOperator` calls the local surrogate through
`AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks-local:8000` with a demo-only token.
There is no Azure workspace connection in the default profile, and no settings were
copied from the host Airflow installation.
