# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import tempfile
import unittest
from pathlib import Path

from hcu_envcheck.rdma_policy import load_roce_policy, policy_requires_roce


class RdmaPolicyFileTests(unittest.TestCase):
    def test_valid_policy_is_preserved_as_json_values(self):
        policy = {
            "protocol": "roce-v2",
            "versions": ["v2"],
            "allowed_prefixes": ["10.20.0.0/16"],
            "vlan_ids": [120],
            "lossless_priorities": [3],
        }
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "policy.json"
            path.write_text(json.dumps(policy), encoding="utf-8")
            self.assertEqual(load_roce_policy(path), policy)
        self.assertTrue(policy_requires_roce(policy))

    def test_unknown_key_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "policy.json"
            path.write_text('{"invented": true}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unknown RoCE policy keys"):
                load_roce_policy(path)

    def test_non_object_and_invalid_json_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            array_path = Path(temp) / "array.json"
            array_path.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "one JSON object"):
                load_roce_policy(array_path)
            bad_path = Path(temp) / "bad.json"
            bad_path.write_text("{", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not valid JSON"):
                load_roce_policy(bad_path)

    def test_missing_file_and_empty_policy_semantics(self):
        with self.assertRaisesRegex(ValueError, "not found"):
            load_roce_policy(Path("does-not-exist.json"))
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "empty.json"
            path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "empty JSON object"):
                load_roce_policy(path)
        self.assertFalse(policy_requires_roce(None))
        self.assertFalse(policy_requires_roce({}))


if __name__ == "__main__":
    unittest.main()
