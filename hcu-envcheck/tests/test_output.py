# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from hcu_envcheck.output import (
    atomic_write_text_exclusive,
    claim_labeled_run_directory,
    claim_output_directory,
    require_new_output_path,
    run_directory_label,
    validate_output_layout,
)


class OutputSafetyTests(unittest.TestCase):
    def test_claim_output_directory_refuses_existing_empty_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            existing = Path(temporary) / "existing"
            existing.mkdir()
            with self.assertRaisesRegex(ValueError, "already exists"):
                claim_output_directory(existing)

    def test_claim_output_directory_creates_private_unique_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "new" / "run"
            self.assertEqual(claim_output_directory(output), output)
            self.assertTrue(output.is_dir())

    def test_labeled_run_directory_reuses_root_without_overwriting_old_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            output_root = Path(temporary) / "out2"
            output_root.mkdir()
            old_summary = output_root / "cluster-summary.md"
            old_summary.write_text("old result\n", encoding="utf-8")

            run_dir = claim_labeled_run_directory(
                output_root, "container_platform_resource",
                timestamp=datetime(
                    2026,
                    7,
                    29,
                    12,
                    34,
                    56,
                    tzinfo=timezone.utc,
                ),
            )

            self.assertEqual(
                run_dir,
                output_root / "container_platform_resource_20260729_123456",
            )
            self.assertTrue(run_dir.is_dir())
            self.assertEqual(old_summary.read_text(encoding="utf-8"), "old result\n")

    def test_same_second_rerun_appends_suffix_instead_of_overwriting(self):
        with tempfile.TemporaryDirectory() as temporary:
            output_root = Path(temporary)
            moment = datetime(2026, 7, 29, 12, 34, 56, tzinfo=timezone.utc)
            first = claim_labeled_run_directory(output_root, "conda_rccl", timestamp=moment)
            second = claim_labeled_run_directory(output_root, "conda_rccl", timestamp=moment)
            third = claim_labeled_run_directory(output_root, "conda_rccl", timestamp=moment)
            self.assertEqual(first.name, "conda_rccl_20260729_123456")
            self.assertEqual(second.name, "conda_rccl_20260729_123456_1")
            self.assertEqual(third.name, "conda_rccl_20260729_123456_2")
            self.assertTrue(first.is_dir() and second.is_dir() and third.is_dir())

    def test_labeled_run_directory_creates_missing_output_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            output_root = Path(temporary) / "nested" / "results"

            run_dir = claim_labeled_run_directory(output_root, "lconda_script")

            self.assertEqual(run_dir.parent, output_root)
            self.assertRegex(run_dir.name, r"^lconda_script_\d{8}_\d{6}$")

    def test_run_directory_label_maps_scenarios_and_sanitizes_operations(self):
        self.assertEqual(run_directory_label("per-node-container", "rccl"), "container_rccl")
        self.assertEqual(run_directory_label("shared-conda", "platform,resource"), "conda_platform_resource")
        self.assertEqual(run_directory_label("node-local-conda", "ib-write-bw"), "lconda_ib_write_bw")
        with self.assertRaisesRegex(ValueError, "unknown scenario"):
            run_directory_label("bogus-scenario", "rccl")

    def test_require_new_output_path_rejects_existing_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            output.write_text("old", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "already exists"):
                require_new_output_path(output, label="output file")

    def test_atomic_write_is_complete_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            atomic_write_text_exclusive(output, "first\n")
            self.assertEqual(output.read_text(encoding="utf-8"), "first\n")
            with self.assertRaisesRegex(ValueError, "already exists"):
                atomic_write_text_exclusive(output, "second\n")
            self.assertEqual(output.read_text(encoding="utf-8"), "first\n")

    def test_output_and_evidence_paths_cannot_overlap(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            with self.assertRaisesRegex(ValueError, "non-nested"):
                validate_output_layout(output, output / "evidence")


if __name__ == "__main__":
    unittest.main()
