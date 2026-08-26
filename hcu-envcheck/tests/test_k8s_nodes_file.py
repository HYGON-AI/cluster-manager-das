# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from hcu_envcheck.cli import build_parser, main


class K8sNodesFileCliTests(unittest.TestCase):
    def _args(self, *node_source: str) -> list[str]:
        return [
            "k8s-cluster",
            *node_source,
            "--namespace",
            "training",
            "--image",
            "registry.example.com/training:tag",
            "--expected-devices",
            "8",
            "--output-dir",
            "new-output",
        ]

    def test_nodes_file_is_accepted_as_node_source(self):
        args = build_parser().parse_args(
            self._args("--nodes-file", "k8s-nodes.txt")
        )
        self.assertIsNone(args.node)
        self.assertEqual(args.nodes_file, Path("k8s-nodes.txt"))

    def test_repeated_node_remains_backward_compatible(self):
        args = build_parser().parse_args(
            self._args("--node", "compute001", "--node", "compute002")
        )
        self.assertEqual(args.node, ["compute001", "compute002"])
        self.assertIsNone(args.nodes_file)

    def test_node_sources_are_required_and_mutually_exclusive(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as missing:
            build_parser().parse_args(self._args())
        self.assertEqual(missing.exception.code, 3)

        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as both:
            build_parser().parse_args(
                self._args(
                    "--node",
                    "compute001",
                    "--nodes-file",
                    "k8s-nodes.txt",
                )
            )
        self.assertEqual(both.exception.code, 3)

    def test_nodes_file_is_parsed_before_kubectl_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            nodes_file = Path(temporary) / "nodes.txt"
            nodes_file.write_text(
                "# K8s training nodes\n"
                "compute[001-003]\n"
                "compute002\n",
                encoding="utf-8",
            )
            report = {
                "status": "READY",
                "node_result_groups": [],
                "consistency_findings": [],
            }
            with (
                patch(
                    "hcu_envcheck.cli.run_k8s_cluster_preflight",
                    return_value=(report, Path("result.json"), Path("summary.md")),
                ) as run,
                redirect_stdout(io.StringIO()),
            ):
                returncode = main(
                    self._args("--nodes-file", str(nodes_file))
                )

        self.assertEqual(returncode, 0)
        self.assertEqual(
            run.call_args.kwargs["nodes"],
            ["compute001", "compute002", "compute003"],
        )

    def test_invalid_nodes_file_is_tool_error_without_kubectl(self):
        with tempfile.TemporaryDirectory() as temporary:
            empty_file = Path(temporary) / "empty.txt"
            empty_file.write_text("# no nodes\n", encoding="utf-8")
            invalid_file = Path(temporary) / "invalid.txt"
            invalid_file.write_text("compute001;touch bad\n", encoding="utf-8")
            missing_file = Path(temporary) / "missing.txt"

            for nodes_file in (empty_file, invalid_file, missing_file):
                with self.subTest(nodes_file=nodes_file):
                    with (
                        patch("hcu_envcheck.cli.run_k8s_cluster_preflight") as run,
                        redirect_stdout(io.StringIO()) as stdout,
                    ):
                        returncode = main(
                            self._args("--nodes-file", str(nodes_file))
                        )
                    self.assertEqual(returncode, 3)
                    self.assertFalse(run.called)
                    self.assertIn("RESULT        TOOL_ERROR", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
