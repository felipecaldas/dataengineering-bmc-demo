from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import yaml

from demo.eod_readiness import EodReadinessProjector
from demo.gates import asn_name, classify_percentage, order_name
from demo.landing import EOD_HEADER, POS_HEADER, TABLES
from demo.seed import STATE_PLAN, _store_plan, landing_prefix
from demo.simulate import ASN_HEADER
from controlm.build import render


ROOT = Path(__file__).parents[1]


def _load_databricks_provisioner():
    path = ROOT / "databricks" / "provision_cluster.py"
    spec = spec_from_file_location("databricks_provision_cluster", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load the Databricks provisioner")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RetailContractsTest(unittest.TestCase):
    def test_canonical_store_plan_has_325_stores_across_all_jurisdictions(self) -> None:
        stores = _store_plan(325)
        self.assertEqual(325, len(stores))
        self.assertEqual(
            {"NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"},
            {state for _, state, _ in stores},
        )
        self.assertEqual(325, sum(count for _, count, _ in STATE_PLAN))

    def test_scaled_store_plan_preserves_requested_count(self) -> None:
        for count in (8, 17, 100):
            self.assertEqual(count, len(_store_plan(count)))

    def test_eod_policy_boundaries(self) -> None:
        self.assertEqual("PROCEED", classify_percentage(100.0, 0))
        self.assertEqual("PROCEED_WITH_EXCEPTIONS", classify_percentage(99.5, 1))
        self.assertEqual("PROCEED_WITH_TRADE_OPS_ALERT", classify_percentage(98.0, 4))
        self.assertEqual("HOLD", classify_percentage(97.99, 8))

    def test_eod_readiness_emits_once_after_settlement(self) -> None:
        trading_date = date(2026, 8, 14)
        observed_at = datetime(2026, 8, 14, 12, tzinfo=timezone.utc)
        projector = EodReadinessProjector(range(1, 326))
        projector.commit(projector.arm(trading_date, "arm-1"))
        for store_id in range(1, 320):
            state, event = projector.observe(
                {"trading_date": trading_date.isoformat(), "store_id": store_id},
                observed_at,
            )
            projector.commit(state)
        self.assertIsNone(event)
        self.assertEqual(
            [], projector.settled_events(observed_at + timedelta(seconds=2), 3)
        )
        settled = projector.settled_events(observed_at + timedelta(seconds=3), 3)
        self.assertEqual(1, len(settled))
        state, event = settled[0]
        projector.commit(state)
        self.assertEqual("PROCEED_WITH_TRADE_OPS_ALERT", event["decision"])
        self.assertEqual("RETAIL_EOD_READY_20260814", event["event_name"])
        self.assertEqual(319, event["actual_stores"])
        self.assertEqual([], projector.settled_events(observed_at + timedelta(seconds=4), 3))

    def test_eod_readiness_requires_arm_and_rearm_resets_generation(self) -> None:
        trading_date = date(2026, 8, 14)
        projector = EodReadinessProjector(range(1, 6))
        state, event = projector.observe(
            {"trading_date": trading_date.isoformat(), "store_id": 1}
        )
        self.assertIsNone(state)
        self.assertIsNone(event)
        projector.commit(projector.arm(trading_date, "arm-1"))
        second = projector.arm(trading_date, "arm-2")
        self.assertEqual(2, second.generation)
        self.assertEqual((), second.observed_store_ids)

    def test_azure_object_contracts_are_date_scoped(self) -> None:
        trading_date = date(2026, 8, 14)
        prefix = "landing/trading_date=2026-08-14"
        self.assertEqual(prefix, landing_prefix(trading_date))
        self.assertEqual(f"{prefix}/asn_inbound.csv", asn_name(trading_date))
        self.assertEqual("outbound/REPLEN_ORDER_20260814.csv", order_name(trading_date))
        self.assertEqual(6, len(TABLES))
        self.assertEqual(9, len(POS_HEADER))
        self.assertEqual(6, len(EOD_HEADER))

    def test_asn_header_is_exact_and_drift_column_is_not_allowed(self) -> None:
        self.assertEqual(
            [
                "asn_id",
                "trading_date",
                "product_sku",
                "expected_units",
                "expected_arrival_date",
                "supplier_id",
            ],
            ASN_HEADER,
        )
        self.assertNotIn("carton_id", ASN_HEADER)

    def test_azure_databricks_cluster_contract(self) -> None:
        provisioner = _load_databricks_provisioner()
        config = provisioner._desired_configuration()
        self.assertEqual(0, config["num_workers"])
        self.assertEqual(
            "singleNode", config["spark_conf"]["spark.databricks.cluster.profile"]
        )
        self.assertGreaterEqual(config["autotermination_minutes"], 10)
        self.assertEqual(
            "/sql/protocolv1/o/123456789/cluster-id",
            provisioner._http_path(
                "https://adb-123456789.7.azuredatabricks.net", "cluster-id"
            ),
        )

    def test_bronze_notebook_validates_all_inputs_before_delta_writes(self) -> None:
        notebook = (ROOT / "databricks" / "notebooks" / "00_ingest_bronze.py").read_text()
        self.assertIn("set(manifest.get(\"tables\", {})) != set(TABLES)", notebook)
        self.assertIn("actual_header != expected_header", notebook)
        self.assertIn("checksum does not match", notebook)
        self.assertIn("contains duplicate natural keys", notebook)
        self.assertLess(notebook.index("frames = {}"), notebook.index("CREATE DATABASE"))
        self.assertIn('.option("replaceWhere", predicate)', notebook)
        self.assertIn('spark.sql("CREATE DATABASE IF NOT EXISTS bronze")', notebook)

    def test_dbt_maps_bronze_sources_to_silver_and_gold_models(self) -> None:
        project_root = ROOT / "dbt" / "kmart_retail"
        source = yaml.safe_load((project_root / "models" / "sources.yml").read_text())
        self.assertEqual("bronze", source["sources"][0]["name"])
        self.assertEqual("bronze", source["sources"][0]["schema"])
        project = yaml.safe_load((project_root / "dbt_project.yml").read_text())
        models = project["models"]["kmart_retail"]
        self.assertEqual("silver", models["staging"]["+schema"])
        self.assertEqual(["stage"], models["staging"]["+tags"])
        self.assertEqual("silver", models["intermediate"]["+schema"])
        self.assertEqual(["intermediate"], models["intermediate"]["+tags"])
        self.assertEqual("gold", models["marts"]["+schema"])
        self.assertFalse((project_root / "profiles.yml").exists())

    def test_shared_dbt_cloud_jobs_are_stage_intermediate_and_gold(self) -> None:
        provisioner = (ROOT / "dbt" / "provision_controlm_jobs.py").read_text()
        self.assertIn('"stage": {', provisioner)
        self.assertIn('"intermediate": {', provisioner)
        self.assertIn('"gold": {', provisioner)
        self.assertIn('"selector": "tag:stage"', provisioner)
        self.assertIn('"selector": "tag:intermediate"', provisioner)
        self.assertNotIn('"selector": "tag:bronze"', provisioner)
        self.assertNotIn('"selector": "tag:silver"', provisioner)

    def test_controlm_invokes_the_shared_cloud_jobs_in_order(self) -> None:
        folder = json.loads(
            (ROOT / "controlm" / "workflows" / "trade_close_to_replenishment.json").read_text()
        )["TradeCloseToReplenishment"]
        expected = {
            "DbtStage": "tag:stage",
            "DbtIntermediate": "tag:intermediate",
            "DbtGold": "tag:gold",
        }
        for name, selector in expected.items():
            job = folder[name]
            self.assertEqual("Job:DBT", job["Type"])
            self.assertEqual("${DBT_CONNECTION_PROFILE}", job["ConnectionProfile"])
            command = job["Variables"][0]["UCM-DefineCommands-N001-element"]
            self.assertIn(selector, command)
            self.assertIn("%%DEMO_ISO_DATE", command)
        self.assertEqual(
            [
                "StageInputsToAzure",
                "IngestBronze",
                "DbtStage",
                "DbtIntermediate",
                "DbtGold",
                "ExportReplenishment",
                "DeliverToWMS",
                "ConfirmWMSIntake",
                "SLA_PickWave",
            ],
            folder["ProcessingFlow"]["Sequence"],
        )

    def test_controlm_renderer_uses_shared_dbt_cloud_job_ids(self) -> None:
        source = ROOT / "controlm" / "workflows" / "trade_close_to_replenishment.json"
        state = {
            "job_ids": {"stage": 101, "intermediate": 102, "gold": 103},
            "controlm_connection_profile": "DEMO_DBT",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text(json.dumps(state))
            folder = render(source, "SERVER", "agent", "runner", path)[
                "TradeCloseToReplenishment"
            ]
        self.assertEqual("101", folder["DbtStage"]["DBT Job Id"])
        self.assertEqual("102", folder["DbtIntermediate"]["DBT Job Id"])
        self.assertEqual("103", folder["DbtGold"]["DBT Job Id"])
        self.assertEqual("DEMO_DBT", folder["DbtGold"]["ConnectionProfile"])

    def test_airflow_uses_same_cloud_jobs_and_ends_at_delivery(self) -> None:
        dag = (ROOT / "airflow" / "dags" / "trade_close_to_replenishment.py").read_text()
        for key in ("ingest", "export", "stage", "intermediate", "gold"):
            self.assertIn(f'["{key}"]', dag)
        self.assertIn("DbtCloudRunJobOperator", dag)
        self.assertIn("DatabricksRunNowOperator", dag)
        self.assertIn("validate_cloud_configuration", dag)
        self.assertIn("stage_inputs_to_azure", dag)
        self.assertNotIn("gate-ack", dag)
        self.assertNotIn("SLA_PickWave", dag)

    def test_compose_has_no_postgres_azurite_or_databricks_surrogate(self) -> None:
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
        services = set(compose["services"])
        self.assertNotIn("postgres", services)
        self.assertNotIn("azurite", services)
        self.assertNotIn("databricks-local", services)
        self.assertNotIn("kafka-ingest", services)
        self.assertEqual("standalone", compose["services"]["airflow"]["command"])
        airflow_environment = compose["x-airflow-environment"]
        self.assertIn("sqlite", airflow_environment["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
        self.assertEqual("0:0", compose["services"]["airflow-init"]["user"])
        self.assertIn(
            "airflow-init", compose["services"]["airflow"]["depends_on"]
        )

    def test_make_removes_obsolete_local_data_plane_commands(self) -> None:
        makefile = (ROOT / "Makefile").read_text()
        for target in (
            "postgres-start:",
            "postgres-stop:",
            "bronze:",
            "silver:",
            "dbt:",
            "dbt-retry:",
            "replen:",
            "databricks-azure-sync:",
        ):
            self.assertNotIn(target, makefile)
        for target in (
            "stage-inputs:",
            "databricks-ingest:",
            "dbt-stage:",
            "dbt-intermediate:",
            "dbt-gold:",
            "databricks-export:",
        ):
            self.assertIn(target, makefile)

    def test_azure_order_export_matches_wms_contract(self) -> None:
        notebook = (
            ROOT / "databricks" / "notebooks" / "04_export_replenishment.py"
        ).read_text()
        self.assertIn('spark.table("gold.fct_replenishment_need")', notebook)
        self.assertIn('.orderBy("store_id", "product_sku")', notebook)
        self.assertIn("RPL-{date_key}-{index:06d}", notebook)
        self.assertIn('dbutils.widgets.text("outbound_path", "")', notebook)
        self.assertIn('destination = f"{outbound_path}/REPLEN_ORDER_{date_key}.csv"', notebook)
        self.assertIn(
            '["order_id", "trading_date", "store_id", "product_sku", "replenishment_units"]',
            notebook,
        )

    def test_generated_state_never_contains_cloud_tokens(self) -> None:
        provisioner = (ROOT / "dbt" / "provision_cloud_databricks.py").read_text()
        state_block = provisioner.split("state = {", 1)[1].split("}", 1)[0]
        self.assertNotIn("token", state_block)
        databricks = (ROOT / "databricks" / "provision_cluster.py").read_text()
        state_block = databricks.split("state.update({", 1)[1].split("})", 1)[0]
        self.assertNotIn("token", state_block)

    def test_controlm_order_passes_both_dates_and_complete_paths(self) -> None:
        wrapper = (ROOT / "controlm" / "scripts" / "order_workflow.sh").read_text()
        self.assertIn('"DEMO_DATE":"%s"', wrapper)
        self.assertIn('"DEMO_ISO_DATE":"%s"', wrapper)
        self.assertIn('"ASN_PATH":"%s"', wrapper)
        self.assertIn('"ACK_PATH":"%s"', wrapper)


if __name__ == "__main__":
    unittest.main()
