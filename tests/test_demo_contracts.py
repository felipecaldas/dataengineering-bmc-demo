from __future__ import annotations

import unittest
from datetime import date

from demo.gates import asn_name, classify_percentage, order_name
from demo.seed import STATE_PLAN, _store_plan
from demo.simulate import ASN_HEADER


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

    def test_file_contracts_use_control_m_odate_format(self) -> None:
        trading_date = date(2026, 8, 14)
        self.assertEqual("inbound/ASN_20260814.csv", asn_name(trading_date))
        self.assertEqual("outbound/REPLEN_ORDER_20260814.csv", order_name(trading_date))

    def test_asn_contract_does_not_include_drift_column(self) -> None:
        self.assertNotIn("carton_id", ASN_HEADER)
        self.assertEqual(6, len(ASN_HEADER))


if __name__ == "__main__":
    unittest.main()

