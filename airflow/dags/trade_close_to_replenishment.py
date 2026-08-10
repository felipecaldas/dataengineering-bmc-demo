"""Control plane A: Airflow-only orchestration of the retail data plane."""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

from demo.gates import asn_ready, eod_status
from demo.wms import deliver_to_wms


LOGGER = logging.getLogger(__name__)
DBT_PROJECT = Path("/opt/airflow/dbt/kmart_retail")
TRADING_DATE = "{{ dag_run.conf.get('trading_date', ds) }}"
DEMO_SCHEDULE = os.getenv("DEMO_AIRFLOW_SCHEDULE") or None


def eod_gate(trading_date: str) -> bool:
    status = eod_status(pendulum.parse(trading_date).date())
    LOGGER.info(
        "EOD gate: %s/%s stores (%.3f%%), decision=%s, missing=%s",
        status.actual,
        status.expected,
        status.percentage,
        status.decision,
        status.missing_store_ids,
    )
    return status.ready


def asn_gate(trading_date: str) -> bool:
    ready = asn_ready(pendulum.parse(trading_date).date())
    LOGGER.info("ASN gate: trading_date=%s ready=%s", trading_date, ready)
    return ready


def wms_delivery(trading_date: str) -> dict:
    return deliver_to_wms(pendulum.parse(trading_date).date())


profile_config = ProfileConfig(
    profile_name="kmart_retail",
    target_name="demo",
    profiles_yml_filepath=DBT_PROJECT / "profiles.yml",
)


with DAG(
    dag_id="trade_close_to_replenishment",
    description="Trade day close to WMS replenishment — Airflow control plane",
    start_date=pendulum.datetime(2026, 1, 1, tz="Australia/Melbourne"),
    # Manual by default so a freshly started presentation stack cannot order a
    # stale trading date before it has been seeded. Set DEMO_AIRFLOW_SCHEDULE to
    # "0 1 * * *" to demonstrate the production daily schedule from the design.
    schedule=DEMO_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["retail", "dataops", "control-plane-a"],
) as dag:
    wait_for_stores = PythonSensor(
        task_id="wait_for_store_eod_threshold",
        python_callable=eod_gate,
        op_kwargs={"trading_date": TRADING_DATE},
        poke_interval=10,
        timeout=2 * 60 * 60,
        mode="reschedule",
    )

    wait_for_asn = PythonSensor(
        task_id="wait_for_supplier_asn",
        python_callable=asn_gate,
        op_kwargs={"trading_date": TRADING_DATE},
        poke_interval=10,
        timeout=90 * 60,
        mode="reschedule",
    )

    bronze = DatabricksRunNowOperator(
        task_id="databricks_bronze_ingest",
        databricks_conn_id="databricks_default",
        job_id=440,
        job_parameters={"trading_date": TRADING_DATE},
        idempotency_token="airflow-bronze-{{ run_id }}",
        polling_period_seconds=1,
    )

    silver = DatabricksRunNowOperator(
        task_id="databricks_silver_conform",
        databricks_conn_id="databricks_default",
        job_id=441,
        job_parameters={"trading_date": TRADING_DATE},
        idempotency_token="airflow-silver-{{ run_id }}",
        polling_period_seconds=1,
    )

    dbt_gold = DbtTaskGroup(
        group_id="dbt_gold",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        operator_args={"vars": {"trading_date": TRADING_DATE}},
    )

    replenishment = DatabricksRunNowOperator(
        task_id="databricks_replenishment_calc",
        databricks_conn_id="databricks_default",
        job_id=447,
        job_parameters={"trading_date": TRADING_DATE},
        idempotency_token="airflow-replen-{{ run_id }}",
        polling_period_seconds=1,
    )

    deliver = PythonOperator(
        task_id="deliver_order_to_wms",
        python_callable=wms_delivery,
        op_kwargs={"trading_date": TRADING_DATE},
    )

    [wait_for_stores, wait_for_asn] >> bronze >> silver >> dbt_gold >> replenishment >> deliver

# Intentionally absent from Control Plane A:
# - WMS acknowledgement monitoring
# - the 06:00 pick-wave SLA service definition and forecast
