"""Control plane A over the shared Azure Databricks and dbt Cloud data plane."""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from demo.gates import asn_ready, eod_status
from demo.landing import stage_inputs
from demo.wms import deliver_to_wms


LOGGER = logging.getLogger(__name__)
RUNTIME_ROOT = Path(os.getenv("DEMO_RUNTIME_ROOT", "/opt/airflow/runtime"))
TRADING_DATE = "{{ dag_run.conf.get('trading_date', ds) }}"
DEMO_SCHEDULE = os.getenv("DEMO_AIRFLOW_SCHEDULE") or None
STORAGE_BASE = os.environ.get("DATABRICKS_STORAGE_BASE_PATH", "").rstrip("/")


def _job_ids(relative_path: str, required: tuple[str, ...]) -> dict[str, int]:
    path = RUNTIME_ROOT / relative_path
    try:
        state = json.loads(path.read_text())
        values = state["job_ids"]
        return {key: int(values[key]) for key in required}
    except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        LOGGER.warning("Generated job state is incomplete: %s", path)
        return {key: 0 for key in required}


DATABRICKS_JOBS = _job_ids("databricks/azure.json", ("ingest", "export"))
DBT_CLOUD_JOBS = _job_ids(
    "dbt_cloud/azure.json", ("stage", "intermediate", "gold")
)


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


def stage_azure_inputs(trading_date: str) -> dict:
    return stage_inputs(pendulum.parse(trading_date).date())


def wms_delivery(trading_date: str) -> dict:
    return deliver_to_wms(pendulum.parse(trading_date).date())


def dbt_step(tag: str) -> str:
    return (
        f"dbt build --select tag:{tag} "
        f'--vars "{{trading_date: \'{TRADING_DATE}\'}}"'
    )


def validate_cloud_configuration() -> dict:
    missing = []
    if not STORAGE_BASE:
        missing.append("DATABRICKS_STORAGE_BASE_PATH")
    missing.extend(
        f"Databricks job:{key}" for key, value in DATABRICKS_JOBS.items() if value <= 0
    )
    missing.extend(
        f"dbt Cloud job:{key}" for key, value in DBT_CLOUD_JOBS.items() if value <= 0
    )
    if missing:
        raise RuntimeError(
            "Cloud job configuration is incomplete; provision jobs and restart Airflow: "
            + ", ".join(missing)
        )
    return {
        "databricks_jobs": DATABRICKS_JOBS,
        "dbt_cloud_jobs": DBT_CLOUD_JOBS,
        "storage_base": STORAGE_BASE,
    }


with DAG(
    dag_id="trade_close_to_replenishment",
    description="Trade day close to WMS delivery through Azure Databricks and dbt Cloud",
    start_date=pendulum.datetime(2026, 1, 1, tz="Australia/Melbourne"),
    schedule=DEMO_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["retail", "dataops", "control-plane-a", "azure-databricks", "dbt-cloud"],
) as dag:
    configuration = PythonOperator(
        task_id="validate_cloud_configuration",
        python_callable=validate_cloud_configuration,
    )

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

    stage = PythonOperator(
        task_id="stage_inputs_to_azure",
        python_callable=stage_azure_inputs,
        op_kwargs={"trading_date": TRADING_DATE},
    )

    ingest = DatabricksRunNowOperator(
        task_id="databricks_ingest_bronze",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOBS["ingest"],
        job_parameters={
            "trading_date": TRADING_DATE,
            "landing_path": f"{STORAGE_BASE}/landing/trading_date={TRADING_DATE}",
        },
        idempotency_token="airflow-ingest-{{ run_id }}",
        polling_period_seconds=10,
    )

    dbt_stage = DbtCloudRunJobOperator(
        task_id="dbt_stage",
        dbt_cloud_conn_id="dbt_cloud_default",
        job_id=DBT_CLOUD_JOBS["stage"],
        trigger_reason=f"Airflow retail demo stage for {TRADING_DATE}",
        steps_override=[dbt_step("stage")],
        check_interval=10,
        timeout=3600,
    )
    dbt_intermediate = DbtCloudRunJobOperator(
        task_id="dbt_intermediate",
        dbt_cloud_conn_id="dbt_cloud_default",
        job_id=DBT_CLOUD_JOBS["intermediate"],
        trigger_reason=f"Airflow retail demo intermediate for {TRADING_DATE}",
        steps_override=[dbt_step("intermediate")],
        check_interval=10,
        timeout=3600,
    )
    dbt_gold = DbtCloudRunJobOperator(
        task_id="dbt_gold",
        dbt_cloud_conn_id="dbt_cloud_default",
        job_id=DBT_CLOUD_JOBS["gold"],
        trigger_reason=f"Airflow retail demo gold for {TRADING_DATE}",
        steps_override=[dbt_step("gold")],
        check_interval=10,
        timeout=3600,
    )

    export = DatabricksRunNowOperator(
        task_id="databricks_export_replenishment",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOBS["export"],
        job_parameters={
            "trading_date": TRADING_DATE,
            "outbound_path": f"{STORAGE_BASE}/outbound",
        },
        idempotency_token="airflow-export-{{ run_id }}",
        polling_period_seconds=10,
    )

    deliver = PythonOperator(
        task_id="deliver_order_to_wms",
        python_callable=wms_delivery,
        op_kwargs={"trading_date": TRADING_DATE},
    )

    configuration >> [wait_for_stores, wait_for_asn]
    [wait_for_stores, wait_for_asn] >> stage
    stage >> ingest >> dbt_stage >> dbt_intermediate >> dbt_gold >> export >> deliver

# Deliberate Control Plane A boundary: acknowledgement and 06:00 service-SLA
# measurement remain in Control-M, after the same WMS delivery boundary.
