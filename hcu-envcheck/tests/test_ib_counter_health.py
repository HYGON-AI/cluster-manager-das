# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import unittest

from hcu_envcheck.ib_counters import (
    CounterRule,
    DEFAULT_IB_COUNTER_RULES,
    evaluate_ib_counter_samples,
)


def counter_sample(value=0):
    return {name: value for name in DEFAULT_IB_COUNTER_RULES}


class IbCounterHealthTests(unittest.TestCase):
    def test_stable_nonzero_lifetime_totals_pass(self):
        before = counter_sample(0)
        before["symbol_error"] = "91"
        before["port_xmit_wait"] = "132237365"
        result = evaluate_ib_counter_samples(
            before, dict(before), interval_seconds=5
        )
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(result["coverage"]["required_complete"])
        self.assertEqual(result["reason_codes"], ["IB_COUNTERS_STABLE"])
        self.assertIn("symbol_error", result["historical_nonzero_counters"])

    def test_error_counter_growth_is_fail(self):
        before = counter_sample(0)
        after = counter_sample(0)
        after["link_downed"] = "1"
        result = evaluate_ib_counter_samples(before, after, interval_seconds=5)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["metrics"]["link_downed"]["delta"], 1)
        self.assertEqual(
            result["metrics"]["link_downed"]["reason_code"],
            "IB_COUNTER_ERROR_GROWTH",
        )

    def test_known_failure_takes_precedence_over_missing_required_evidence(self):
        before = counter_sample(0)
        after = counter_sample(0)
        after["link_downed"] = 1
        after.pop("symbol_error")
        result = evaluate_ib_counter_samples(before, after, interval_seconds=5)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("IB_COUNTER_EVIDENCE_MISSING", result["reason_codes"])
        self.assertIn("IB_COUNTER_ERROR_GROWTH", result["reason_codes"])

    def test_xmit_wait_growth_is_warning_not_link_failure(self):
        before = counter_sample(0)
        after = counter_sample(0)
        before["port_xmit_wait"] = 100
        after["port_xmit_wait"] = 140
        result = evaluate_ib_counter_samples(before, after, interval_seconds=4)
        self.assertEqual(result["status"], "WARN")
        metric = result["metrics"]["port_xmit_wait"]
        self.assertEqual(metric["delta"], 40)
        self.assertEqual(metric["rate_per_second"], 10)
        self.assertEqual(metric["reason_code"], "IB_CONGESTION_SIGNAL_GROWTH")

    def test_missing_required_counter_is_unknown(self):
        before = counter_sample(0)
        after = counter_sample(0)
        after.pop("port_rcv_errors")
        result = evaluate_ib_counter_samples(before, after, interval_seconds=5)
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertFalse(result["coverage"]["required_complete"])
        self.assertEqual(
            result["metrics"]["port_rcv_errors"]["reason_code"],
            "IB_COUNTER_EVIDENCE_MISSING",
        )

    def test_missing_optional_xmit_wait_does_not_hide_required_pass(self):
        before = counter_sample(0)
        after = counter_sample(0)
        before.pop("port_xmit_wait")
        after.pop("port_xmit_wait")
        result = evaluate_ib_counter_samples(before, after, interval_seconds=5)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["metrics"]["port_xmit_wait"]["status"], "UNKNOWN")

    def test_counter_reset_or_unknown_width_wrap_is_unknown(self):
        before = counter_sample(10)
        after = counter_sample(10)
        after["symbol_error"] = 1
        result = evaluate_ib_counter_samples(before, after, interval_seconds=5)
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(
            result["metrics"]["symbol_error"]["reason_code"],
            "IB_COUNTER_RESET_OR_WRAP",
        )

    def test_invalid_value_is_unknown(self):
        before = counter_sample(0)
        after = counter_sample(0)
        after["VL15_dropped"] = "not-a-number"
        result = evaluate_ib_counter_samples(before, after, interval_seconds=5)
        self.assertEqual(result["status"], "UNKNOWN")

    def test_invalid_interval_is_unknown_even_when_values_are_comparable(self):
        result = evaluate_ib_counter_samples(
            counter_sample(0), counter_sample(0), interval_seconds=0
        )
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertFalse(result["interval_valid"])
        self.assertIn("IB_COUNTER_INTERVAL_INVALID", result["reason_codes"])

    def test_threshold_is_policy_configurable(self):
        rules = {
            "symbol_error": CounterRule(
                category="ERROR_OR_LINK_EVENT",
                required=True,
                max_delta=2,
                violation_status="FAIL",
            )
        }
        result = evaluate_ib_counter_samples(
            {"symbol_error": 10},
            {"symbol_error": 12},
            interval_seconds=5,
            rules=rules,
        )
        self.assertEqual(result["status"], "PASS")
        result = evaluate_ib_counter_samples(
            {"symbol_error": 10},
            {"symbol_error": 13},
            interval_seconds=5,
            rules=rules,
        )
        self.assertEqual(result["status"], "FAIL")

    def test_stable_required_counter_at_common_pma_max_is_unknown(self):
        before = counter_sample(0)
        before["symbol_error"] = 0xFFFF
        result = evaluate_ib_counter_samples(before, dict(before), interval_seconds=5)
        self.assertEqual(result["status"], "UNKNOWN")
        metric = result["metrics"]["symbol_error"]
        self.assertEqual(metric["reason_code"], "POSSIBLE_COUNTER_SATURATION")
        self.assertEqual(metric["counter_source"], "sysfs")
        self.assertIsNone(metric["counter_width_bits"])

if __name__ == "__main__":
    unittest.main()
