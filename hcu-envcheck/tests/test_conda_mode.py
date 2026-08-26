# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import unittest

from hcu_envcheck.conda_mode import (
    CondaRuntimeObservation,
    CondaStorageObservation,
    ConfigurationError,
    EnvironmentMode,
    StorageScope,
    build_conda_probe_commands,
    group_runtime_observations,
    parse_mountinfo,
    plan_conda_collection,
    plan_deep_runtime_representatives,
    validate_environment_selection,
)


def observation(
    node: str,
    *,
    fs_type: str = "nfs4",
    mount_source: str = "10.0.0.20:/conda",
    fingerprint: str = "artifact-a",
    shared_backend: bool | None = None,
) -> CondaStorageObservation:
    return CondaStorageObservation(
        node=node,
        prefix="/share/conda/envs/train",
        prefix_exists=True,
        python_executable=True,
        realpath="/share/conda/envs/train",
        mount_source=mount_source,
        fs_type=fs_type,
        identity_fingerprint=fingerprint,
        shared_backend=shared_backend,
    )


class EnvironmentSelectionTests(unittest.TestCase):
    def test_host_python_is_an_explicit_third_mode(self):
        selection = validate_environment_selection(env_mode="host-python")
        self.assertEqual(selection.env_mode.value, "host-python")
        self.assertEqual(
            selection.to_dict(),
            {
                "env_mode": "host-python",
                "conda_prefix": None,
                "conda_storage": None,
                "image": None,
            },
        )

    def test_host_python_forbids_conda_or_docker_options(self):
        with self.assertRaises(ConfigurationError):
            validate_environment_selection(
                env_mode="host-python", image="repo/train:1"
            )
    def test_conda_requires_explicit_storage(self):
        with self.assertRaisesRegex(ConfigurationError, "conda_storage is required"):
            validate_environment_selection(
                env_mode="conda",
                conda_prefix="/share/conda/envs/train",
            )

    def test_conda_selection_is_normalised(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda//envs/train",
            conda_storage="shared",
        )
        self.assertEqual(selection.env_mode, EnvironmentMode.CONDA)
        self.assertEqual(selection.conda_prefix, "/share/conda/envs/train")
        self.assertEqual(selection.to_dict()["conda_storage"], "shared")

    def test_conda_forbids_image(self):
        with self.assertRaisesRegex(ConfigurationError, "image is forbidden"):
            validate_environment_selection(
                env_mode="conda",
                conda_prefix="/share/conda/envs/train",
                conda_storage="shared",
                image="registry/train:tag",
            )

    def test_docker_forbids_all_conda_options(self):
        with self.assertRaisesRegex(ConfigurationError, "forbidden"):
            validate_environment_selection(
                env_mode="docker",
                image="registry/train:tag",
                conda_storage="node-local",
            )

    def test_docker_requires_image(self):
        with self.assertRaisesRegex(ConfigurationError, "image must not be empty"):
            validate_environment_selection(env_mode="docker")

    def test_prefix_must_be_safe_absolute_path(self):
        for prefix in ("relative/env", "/", "/share/../etc", "/share/env\nother"):
            with self.subTest(prefix=prefix):
                with self.assertRaises(ConfigurationError):
                    validate_environment_selection(
                        env_mode="conda",
                        conda_prefix=prefix,
                        conda_storage="node-local",
                    )


class MountInfoTests(unittest.TestCase):
    def test_deepest_mount_and_escaped_path_are_parsed(self):
        mountinfo = "\n".join(
            [
                "20 1 8:1 / / rw - ext4 /dev/sda1 rw",
                "21 20 0:44 / /share rw - nfs4 10.0.0.20:/share rw",
                r"22 21 0:45 / /share/conda\040env rw - nfs4 10.0.0.21:/conda\040env rw",
            ]
        )
        identity = parse_mountinfo(mountinfo, "/share/conda env/train")
        self.assertIsNotNone(identity)
        assert identity is not None
        self.assertEqual(identity.mount_point, "/share/conda env")
        self.assertEqual(identity.mount_source, "10.0.0.21:/conda env")
        self.assertEqual(identity.fs_type, "nfs4")


class CollectionPlanTests(unittest.TestCase):
    def test_shared_artifact_is_probed_once_but_runtime_targets_every_node(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node003", "node001", "node002"],
            observations=[observation("node001"), observation("node002"), observation("node003")],
        )
        self.assertEqual(plan.observed_storage_mode, "shared")
        self.assertEqual(len(plan.storage_cohorts), 1)
        self.assertEqual(plan.artifact_probe_nodes, ("node001",))
        self.assertEqual(plan.runtime_target_nodes, ("node001", "node002", "node003"))
        self.assertEqual(plan.runtime_probe_nodes, plan.runtime_target_nodes)
        self.assertEqual(plan.to_dict()["coverage"]["artifact_metadata"]["unit"], "storage_cohort")

    def test_node_local_never_deduplicates_identical_clones(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="node-local",
        )
        local = [
            observation(
                node,
                fs_type="ext4",
                mount_source="/dev/nvme0n1p3",
                fingerprint="same-clone",
            )
            for node in ("node001", "node002")
        ]
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002"],
            observations=local,
        )
        self.assertEqual(plan.observed_storage_mode, "node-local")
        self.assertEqual(len(plan.storage_cohorts), 2)
        self.assertEqual(plan.artifact_probe_nodes, ("node001", "node002"))

    def test_node_local_declaration_reports_an_observed_shared_mount(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="node-local",
        )
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002"],
            observations=[observation("node001"), observation("node002")],
        )
        self.assertEqual(plan.observed_storage_mode, "shared")
        self.assertEqual(plan.artifact_probe_nodes, ("node001",))
        self.assertIn(
            "CONDA_STORAGE_MODE_MISMATCH",
            {finding.reason_code for finding in plan.findings},
        )

    def test_shared_declaration_detects_local_shadow_and_split(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        observations = [
            observation("node001"),
            observation("node002"),
            observation(
                "node003",
                fs_type="ext4",
                mount_source="/dev/nvme0n1p3",
                fingerprint="artifact-b",
            ),
        ]
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002", "node003"],
            observations=observations,
        )
        self.assertEqual(plan.observed_storage_mode, "split")
        self.assertEqual(plan.artifact_probe_nodes, ("node001", "node003"))
        reasons = {finding.reason_code for finding in plan.findings}
        self.assertIn("CONDA_STORAGE_MODE_MISMATCH", reasons)
        self.assertIn("SHARED_ENV_SPLIT", reasons)

    def test_shared_fingerprint_difference_creates_two_artifact_cohorts(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002"],
            observations=[
                observation("node001", fingerprint="artifact-a"),
                observation("node002", fingerprint="artifact-b"),
            ],
        )
        self.assertEqual(plan.observed_storage_mode, "split")
        self.assertEqual(plan.artifact_probe_nodes, ("node001", "node002"))
        self.assertIn("SHARED_ENV_SPLIT", {finding.reason_code for finding in plan.findings})

    def test_missing_node_keeps_denominator_and_blocks_runtime_probe(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002"],
            observations=[observation("node001")],
        )
        self.assertEqual(plan.observed_storage_mode, "unknown")
        self.assertEqual(plan.runtime_target_nodes, ("node001", "node002"))
        self.assertEqual(plan.runtime_probe_nodes, ("node001",))
        self.assertEqual(plan.to_dict()["coverage"]["storage_identity"], {"covered": 1, "expected": 2})

    def test_failed_storage_collection_is_not_counted_as_identity_coverage(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        timed_out = CondaStorageObservation(
            node="node002",
            prefix="/share/conda/envs/train",
            prefix_exists=False,
            python_executable=False,
            collection_status="TIMEOUT",
            reason_code="SSH_TIMEOUT",
        )
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002"],
            observations=[observation("node001"), timed_out],
        )
        self.assertEqual(plan.observed_storage_mode, "unknown")
        self.assertEqual(plan.storage_observed_nodes, ("node001",))
        self.assertEqual(plan.runtime_probe_nodes, ("node001",))

    def test_unknown_filesystem_is_not_assumed_shared(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002"],
            observations=[
                observation("node001", fs_type="vendorfs"),
                observation("node002", fs_type="vendorfs"),
            ],
        )
        self.assertEqual(plan.observed_storage_mode, "unknown")
        self.assertEqual(len(plan.storage_cohorts), 2)
        self.assertTrue(all(cohort.storage_scope is StorageScope.UNKNOWN for cohort in plan.storage_cohorts))

    def test_explicit_shared_backend_supports_vendor_filesystem(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002"],
            observations=[
                observation("node001", fs_type="vendorfs", shared_backend=True),
                observation("node002", fs_type="vendorfs", shared_backend=True),
            ],
        )
        self.assertEqual(plan.observed_storage_mode, "shared")
        self.assertEqual(plan.artifact_probe_nodes, ("node001",))

    def test_deep_runtime_is_selected_by_artifact_and_host_cohort(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        plan = plan_conda_collection(
            selection,
            expected_nodes=["node001", "node002", "node003"],
            observations=[observation("node001"), observation("node002"), observation("node003")],
        )
        representatives = plan_deep_runtime_representatives(
            plan,
            host_cohort_by_node={
                "node001": "host-a",
                "node002": "host-a",
                "node003": "host-b",
            },
        )
        self.assertEqual(representatives, ("node001", "node003"))


class RuntimeGroupingTests(unittest.TestCase):
    def test_only_exact_normalized_results_are_folded(self):
        groups = group_runtime_observations(
            [
                CondaRuntimeObservation(
                    "node001",
                    "PASS",
                    "RUNTIME_OK",
                    {"python": "3.10.12", "torch": "2.10.0"},
                ),
                CondaRuntimeObservation(
                    "node002",
                    "PASS",
                    "RUNTIME_OK",
                    {"torch": "2.10.0", "python": "3.10.12"},
                ),
                CondaRuntimeObservation(
                    "node003",
                    "PASS",
                    "RUNTIME_OK",
                    {"python": "3.10.12", "torch": "2.9.1"},
                ),
                CondaRuntimeObservation(
                    "node004",
                    "FAIL",
                    "NATIVE_ABI_SYMBOL_MISMATCH",
                    {"python": "3.10.12", "torch": None},
                ),
            ]
        )
        self.assertEqual(len(groups), 3)
        folded = next(group for group in groups if len(group.nodes) == 2)
        self.assertEqual(folded.nodes, ("node001", "node002"))


class ProbeCommandTests(unittest.TestCase):
    def test_commands_use_direct_prefix_python_without_activation(self):
        selection = validate_environment_selection(
            env_mode="conda",
            conda_prefix="/share/conda/envs/train",
            conda_storage="shared",
        )
        commands = build_conda_probe_commands(selection)
        self.assertEqual(
            {command.name for command in commands},
            {"prefix_identity", "artifact_metadata", "torch_runtime"},
        )
        for command in commands:
            self.assertEqual(command.argv[0], "/share/conda/envs/train/bin/python")
            self.assertNotIn("source", command.argv)
            self.assertNotIn("conda", command.argv[:1])
            self.assertIn(("PYTHONDONTWRITEBYTECODE", "1"), command.environment)
        artifact = next(command for command in commands if command.name == "artifact_metadata")
        self.assertEqual(artifact.evidence_scope, "storage-cohort")


if __name__ == "__main__":
    unittest.main()
