from __future__ import annotations

import json
import unittest
from datetime import date, datetime, timedelta, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from demo.databricks_export import EXPORT_SPECS, _parameters, export_directory
from demo.databricks_order import ORDER_HEADER
from demo.eod_readiness import EodReadinessProjector
from demo.gates import asn_name, classify_percentage, order_name
from demo.seed import STATE_PLAN, _store_plan
from demo.simulate import ASN_HEADER


def _load_databricks_provisioner():
    path = Path(__file__).parents[1] / "databricks" / "provision_cluster.py"
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

    def test_eod_readiness_emits_once_at_the_325_store_boundary(self) -> None:
        trading_date = date(2026, 8, 14)
        observed_at = datetime(2026, 8, 14, 12, tzinfo=timezone.utc)
        projector = EodReadinessProjector(range(1, 326))
        projector.commit(projector.arm(trading_date, "arm-1"))
        event = None
        for store_id in range(1, 319):
            state, event = projector.observe(
                {"trading_date": trading_date.isoformat(), "store_id": store_id},
                observed_at,
            )
            projector.commit(state)
        self.assertIsNone(event)
        self.assertEqual("HOLD", projector.states[trading_date.isoformat()].decision)

        state, event = projector.observe(
            {"trading_date": trading_date.isoformat(), "store_id": 319},
            observed_at,
        )
        projector.commit(state)
        self.assertIsNone(event)
        self.assertEqual(
            "PROCEED_WITH_TRADE_OPS_ALERT",
            projector.states[trading_date.isoformat()].decision,
        )
        self.assertEqual(
            [], projector.settled_events(observed_at + timedelta(seconds=2), 3)
        )
        settled = projector.settled_events(observed_at + timedelta(seconds=3), 3)
        self.assertEqual(1, len(settled))
        settled_state, event = settled[0]
        projector.commit(settled_state)
        self.assertEqual("PROCEED_WITH_TRADE_OPS_ALERT", event["decision"])
        self.assertEqual("RETAIL_EOD_READY_20260814", event["event_name"])
        self.assertEqual(319, event["actual_stores"])

        state, duplicate = projector.observe(
            {"trading_date": trading_date.isoformat(), "store_id": 319}
        )
        projector.commit(state)
        self.assertIsNone(duplicate)
        state, later = projector.observe(
            {"trading_date": trading_date.isoformat(), "store_id": 320}
        )
        projector.commit(state)
        self.assertIsNone(later)

    def test_eod_readiness_emits_immediately_at_one_hundred_percent(self) -> None:
        trading_date = date(2026, 8, 14)
        projector = EodReadinessProjector(range(1, 6))
        projector.commit(projector.arm(trading_date, "arm-1"))
        event = None
        for store_id in range(1, 6):
            state, event = projector.observe(
                {"trading_date": trading_date.isoformat(), "store_id": store_id}
            )
            projector.commit(state)
        self.assertEqual("PROCEED", event["decision"])
        self.assertEqual(5, event["actual_stores"])

    def test_eod_readiness_requires_an_arm_and_rearm_starts_a_new_generation(self) -> None:
        trading_date = date(2026, 8, 14)
        projector = EodReadinessProjector(range(1, 326))
        state, event = projector.observe(
            {"trading_date": trading_date.isoformat(), "store_id": 1}
        )
        self.assertIsNone(state)
        self.assertIsNone(event)

        first = projector.arm(trading_date, "arm-1")
        projector.commit(first)
        second = projector.arm(trading_date, "arm-2")
        self.assertEqual(2, second.generation)
        self.assertEqual((), second.observed_store_ids)
        self.assertFalse(second.emitted)

    def test_file_contracts_use_control_m_odate_format(self) -> None:
        trading_date = date(2026, 8, 14)
        self.assertEqual("inbound/ASN_20260814.csv", asn_name(trading_date))
        self.assertEqual("outbound/REPLEN_ORDER_20260814.csv", order_name(trading_date))

    def test_asn_contract_does_not_include_drift_column(self) -> None:
        self.assertNotIn("carton_id", ASN_HEADER)
        self.assertEqual(6, len(ASN_HEADER))

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

    def test_azure_databricks_export_contract_is_date_scoped(self) -> None:
        trading_date = date(2026, 8, 14)
        specs = {spec.table: spec for spec in EXPORT_SPECS}
        self.assertEqual(
            {
                "product_master",
                "pos_transactions",
                "store_eod",
                "asn_inbound",
                "stock_on_hand",
                "sales_history",
            },
            set(specs),
        )
        self.assertEqual((), _parameters(specs["product_master"], trading_date))
        self.assertEqual(
            (trading_date,), _parameters(specs["pos_transactions"], trading_date)
        )
        history_parameters = _parameters(specs["sales_history"], trading_date)
        self.assertEqual("2026-07-17", history_parameters[0].isoformat())
        self.assertEqual(trading_date, history_parameters[1])
        self.assertEqual("20260814", export_directory(trading_date).name)

    def test_azure_databricks_notebook_uses_delta_replacement(self) -> None:
        notebook = (
            Path(__file__).parents[1]
            / "databricks"
            / "notebooks"
            / "00_load_silver.py"
        ).read_text()
        self.assertIn('.option("replaceWhere", predicate)', notebook)
        self.assertIn('spark.sql("CREATE DATABASE IF NOT EXISTS silver")', notebook)
        self.assertIn("Delta verification failed", notebook)

    def test_dbt_models_do_not_use_postgres_cast_shorthand(self) -> None:
        project = Path(__file__).parents[1] / "dbt" / "kmart_retail"
        model_sql = "\n".join(
            path.read_text() for path in (project / "models").rglob("*.sql")
        )
        self.assertNotIn("::", model_sql)
        date_macro = (project / "macros" / "retail_dateadd_days.sql").read_text()
        self.assertIn("databricks__retail_dateadd_days", date_macro)

    def test_dbt_cloud_databricks_provisioning_keeps_credentials_external(self) -> None:
        provisioner = (
            Path(__file__).parents[1] / "dbt" / "provision_cloud_databricks.py"
        ).read_text()
        self.assertIn('DBT_CLOUD_DBT_VERSION", "latest"', provisioner)
        self.assertIn('dotenv.get("DBT_CLOUD_SERVICE_TOKEN")', provisioner)
        self.assertIn('"adapter_version": "databricks_v0"', provisioner)
        state_block = provisioner.split("state = {", 1)[1].split("}", 1)[0]
        self.assertNotIn("token", state_block)

    def test_controlm_integrated_profile_uses_three_native_dbt_jobs(self) -> None:
        workflow_path = (
            Path(__file__).parents[1]
            / "controlm"
            / "workflows"
            / "trade_close_to_replenishment.json"
        )
        folder = json.loads(workflow_path.read_text())["TradeCloseToReplenishment"]
        expected = {
            "DbtBronze": "tag:bronze",
            "DbtSilver": "tag:silver",
            "DbtGold": "tag:gold",
        }
        for name, selector in expected.items():
            job = folder[name]
            self.assertEqual("Job:DBT", job["Type"])
            self.assertEqual("${DBT_CONNECTION_PROFILE}", job["ConnectionProfile"])
            command = job["Variables"][0]["UCM-DefineCommands-N001-element"]
            self.assertIn(selector, command)
            self.assertIn("%%DEMO_ISO_DATE", command)
            self.assertNotIn('"', command)
        sequence = folder["ProcessingFlow"]["Sequence"]
        self.assertLess(sequence.index("SyncDeltaSources"), sequence.index("DbtBronze"))
        self.assertLess(sequence.index("DbtGold"), sequence.index("ExportAzureOrder"))

    def test_controlm_waits_for_the_date_scoped_eod_readiness_event(self) -> None:
        workflow_path = (
            Path(__file__).parents[1]
            / "controlm"
            / "workflows"
            / "trade_close_to_replenishment.json"
        )
        folder = json.loads(workflow_path.read_text())["TradeCloseToReplenishment"]
        job = folder["WaitForStoreEODThreshold"]
        self.assertEqual("Job:Dummy", job["Type"])
        expected_event = {
            "Event": "RETAIL_EOD_READY_%%DEMO_DATE",
            "Date": "OrderDate",
        }
        self.assertEqual(expected_event, job["eventsToWaitFor"]["Events"][0])
        self.assertEqual(expected_event, job["eventsToDelete"]["Events"][0])
        self.assertEqual(
            "WaitForStoreEODThreshold", folder["EodToLanding"]["Sequence"][0]
        )

    def test_azure_order_export_matches_wms_contract(self) -> None:
        self.assertEqual(
            [
                "order_id",
                "trading_date",
                "store_id",
                "product_sku",
                "replenishment_units",
            ],
            ORDER_HEADER,
        )
        notebook = (
            Path(__file__).parents[1]
            / "databricks"
            / "notebooks"
            / "04_export_replenishment.py"
        ).read_text()
        self.assertIn('spark.table("gold.fct_replenishment_need")', notebook)
        self.assertIn(".orderBy(\"store_id\", \"product_sku\")", notebook)
        self.assertIn("RPL-{date_key}-{index:06d}", notebook)
        adapter = (
            Path(__file__).parents[1] / "databricks" / "export_replenishment.py"
        ).read_text()
        self.assertIn("download.replace(destination)", adapter)

    def test_controlm_order_passes_both_dates_and_complete_paths(self) -> None:
        wrapper = (
            Path(__file__).parents[1] / "controlm" / "scripts" / "order_workflow.sh"
        ).read_text()
        self.assertIn('"DEMO_DATE":"%s"', wrapper)
        self.assertIn('"DEMO_ISO_DATE":"%s"', wrapper)
        self.assertIn('"ASN_PATH":"%s"', wrapper)
        self.assertIn('"ACK_PATH":"%s"', wrapper)

    def test_integrated_runsheet_documents_the_event_driven_boundary(self) -> None:
        runsheet = (Path(__file__).parents[1] / "docs" / "RUNSHEET.md").read_text()
        self.assertIn("make demo-controlm-azure DATE=2026-08-14", runsheet)
        self.assertIn("DbtBronze", runsheet)
        self.assertIn("DbtSilver", runsheet)
        self.assertIn("DbtGold", runsheet)
        self.assertIn("WaitForStoreEODThreshold", runsheet)
        self.assertIn("do not use Postgres", runsheet)
        self.assertIn("make controlm-dbt-trust", runsheet)

    def test_explainer_includes_the_event_handler_and_short_talk_track(self) -> None:
        explainer = (Path(__file__).parents[1] / "_demo_explainer.md").read_text()
        self.assertIn("### How the Event Handler changes the story", explainer)
        self.assertIn("retail.store-eod-readiness-state.v1", explainer)
        self.assertIn("RETAIL_EOD_READY_20260814", explainer)
        self.assertIn("WaitForStoreEODThreshold", explainer)
        self.assertIn("## Three-to-five-minute demo explanation", explainer)
        self.assertNotIn("WaitAllStoresEOD", explainer)


if __name__ == "__main__":
    unittest.main()
