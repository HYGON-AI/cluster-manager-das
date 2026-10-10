import io
import json
import pickle
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from trace_analyzer.fr_attribution import (
    CollectiveAnalyzer,
    _RestrictedTraceUnpickler,
    build_rank_candidates,
    _find_progress_outliers,
    _topology_group_type,
    _write_detailed_json_result,
)
from trace_analyzer.fr_support import (
    _fr_pattern_for_path,
    fr_fields_for_dataflow_record,
    fr_result_from_mcp_module_response,
)


class ProgressOutlierTests(unittest.TestCase):
    def test_consensus_has_no_outliers(self):
        outliers, summary = _find_progress_outliers(
            range(4), {0: 10, 1: 10, 2: 10, 3: 10}
        )
        self.assertEqual(outliers, set())
        self.assertEqual(summary["baseline"], 10)
        self.assertEqual(summary["strategy"], "consensus")

    def test_strict_majority_finds_rank_behind(self):
        outliers, summary = _find_progress_outliers(
            range(4), {0: 159, 1: 159, 2: 159, 3: 158}
        )
        self.assertEqual(outliers, {3})
        self.assertEqual(summary["baseline"], 159)
        self.assertEqual(summary["strategy"], "strict_majority")

    def test_strict_majority_finds_rank_ahead(self):
        outliers, summary = _find_progress_outliers(
            range(4), {0: 4, 1: 3, 2: 3, 3: 3}
        )
        self.assertEqual(outliers, {0})
        self.assertEqual(summary["baseline"], 3)

    def test_split_without_majority_is_ambiguous(self):
        outliers, summary = _find_progress_outliers(
            range(4), {0: 10, 1: 10, 2: 11, 3: 11}
        )
        self.assertEqual(outliers, set())
        self.assertIsNone(summary["baseline"])
        self.assertEqual(summary["ambiguous_ranks"], [0, 1, 2, 3])
        self.assertEqual(summary["strategy"], "ambiguous_no_majority")


class TraceLoadingTests(unittest.TestCase):
    def test_restricted_unpickler_loads_plain_data(self):
        payload = {"entries": [{"rank": 4}], "version": "2.4"}
        loaded = _RestrictedTraceUnpickler(
            io.BytesIO(pickle.dumps(payload))
        ).load()
        self.assertEqual(loaded, payload)

    def test_restricted_unpickler_rejects_python_objects(self):
        data = pickle.dumps(ValueError("do not construct me"))
        with self.assertRaises(pickle.UnpicklingError):
            _RestrictedTraceUnpickler(io.BytesIO(data)).load()


class TraceSummaryTests(unittest.TestCase):
    def test_entry_count_outliers_use_strict_majority(self):
        analyzer = object.__new__(CollectiveAnalyzer)
        analyzer.trace_metadata = {
            str(rank): {
                "entry_count": 29 if rank != 4 else 6,
                "scheduled_collective_count": 29 if rank != 4 else 6,
                "state_counts": {"scheduled": 29 if rank != 4 else 6},
                "operation_counts": {"nccl:all_reduce": 29 if rank != 4 else 6},
                "version": "2.4",
                "comm_lib_version": "test",
            }
            for rank in range(8)
        }
        summary = analyzer._build_trace_summary()
        self.assertEqual(summary["entry_count_mode"], 29)
        self.assertEqual(summary["entry_count_outlier_ranks"], [4])
        self.assertEqual(summary["total_entry_count"], 209)


class RankAssessmentTests(unittest.TestCase):
    def test_ahead_rank_is_primary_and_other_ranks_are_downstream(self):
        rows = [
            "1 | DATA_PARALLEL_GROUP_WITH_CP,0 | nccl:all_gather | 8 | Float | 4",
            "3 | DATA_PARALLEL_GROUP_WITH_CP,0 | nccl:all_gather | 8 | Float | 5",
            "5 | DATA_PARALLEL_GROUP_WITH_CP,0 | nccl:all_gather | 8 | Float | 6",
            "7 | DATA_PARALLEL_GROUP_WITH_CP,0 | nccl:all_gather | 8 | Float | 7",
            "17 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 1 | Float | 4",
            "70 | TENSOR_MODEL_PARALLEL_GROUP,0 | nccl:broadcast | 1x4096 | Long | 4",
        ]
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                + "\n".join(rows)
            ),
            "hanging_rank_list": [4, 5, 6, 7],
            "progress_anomalies": [
                {"rank": 4, "pgid": "17", "direction": "ahead"},
                {"rank": 4, "pgid": "70", "direction": "behind"},
                {"rank": 5, "pgid": "3", "direction": "behind"},
                {"rank": 6, "pgid": "5", "direction": "behind"},
                {"rank": 7, "pgid": "7", "direction": "behind"},
            ],
        }

        assessment = build_rank_candidates(payload)

        self.assertEqual(assessment["primary_suspect_ranks"], [4])
        self.assertEqual(assessment["affected_ranks"], [4, 5, 6, 7])
        self.assertEqual(
            assessment["downstream_affected_ranks"], [5, 6, 7]
        )
        self.assertEqual(
            assessment["结论"]["首要故障嫌疑 Rank"], [4]
        )
        rank4 = assessment["candidate_ranks"][0]
        self.assertIn("relative_score_ratio", rank4)
        self.assertNotIn("confidence", rank4)
        self.assertEqual(rank4["confidence_level"], "high")
        self.assertEqual(rank4["confidence_level_zh"], "高")

    def test_behind_only_candidates_are_not_forced_into_root_cause(self):
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                "1 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 1 | Float | 4\n"
                "2 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 1 | Float | 5\n"
            ),
            "hanging_rank_list": [4, 5],
            "progress_anomalies": [
                {"rank": 4, "pgid": "1", "direction": "behind"},
                {"rank": 5, "pgid": "2", "direction": "behind"},
            ],
        }

        assessment = build_rank_candidates(payload)

        self.assertEqual(assessment["primary_suspect_ranks"], [])
        self.assertEqual(assessment["affected_ranks"], [4, 5])
        self.assertEqual(assessment["downstream_affected_ranks"], [])
        self.assertEqual(
            assessment["classification_basis"],
            "insufficient_causal_evidence",
        )

    def test_weaker_ahead_candidate_is_not_mislabeled_as_downstream(self):
        rows = [
            "1 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 1 | Float | 4",
            "2 | TENSOR_MODEL_PARALLEL_GROUP,0 | nccl:broadcast | 8 | Long | 4",
            "3 | EXPERT_DATA_PARALLEL_GROUP,0 | nccl:all_gather | 8 | Float | 4",
            "4 | TENSOR_MODEL_PARALLEL_GROUP,0 | nccl:reduce_scatter | 8 | Float | 14",
        ]
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                + "\n".join(rows)
            ),
            "hanging_rank_list": [4, 14],
            "progress_anomalies": [
                {"rank": 4, "pgid": "1", "direction": "ahead"},
                {"rank": 14, "pgid": "4", "direction": "ahead"},
            ],
        }

        assessment = build_rank_candidates(payload)

        self.assertEqual(assessment["primary_suspect_ranks"], [4])
        self.assertEqual(assessment["downstream_affected_ranks"], [])
        self.assertEqual(assessment["unresolved_affected_ranks"], [14])

    def test_missing_pg_status_can_identify_crashed_rank(self):
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                "1 | TENSOR_MODEL_PARALLEL_GROUP,0 | nccl:all_reduce | 8 | Float | 4\n"
                "2 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 8 | Float | 5\n"
            ),
            "hanging_rank_list": [4, 5],
            "progress_anomalies": [
                {
                    "rank": 4,
                    "pgid": "1",
                    "direction": "missing_status",
                    "topology_expected_ranks": [4, 5],
                },
                {
                    "rank": 5,
                    "pgid": "2",
                    "direction": "behind",
                    "last_enqueued": 9,
                    "baseline_enqueued": 10,
                    "topology_expected_ranks": [4, 5],
                },
            ],
        }

        assessment = build_rank_candidates(payload)

        self.assertEqual(assessment["primary_suspect_ranks"], [4])
        self.assertEqual(assessment["downstream_affected_ranks"], [5])
        self.assertIn(
            "missing_process_group_status",
            assessment["candidate_ranks"][0]["diagnostic_signals"],
        )

    def test_direct_collective_absence_is_not_hidden_without_ahead_signal(self):
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                "1 | PIPELINE_MODEL_PARALLEL_GROUP,0 | nccl:recv | 8 | Float | 4\n"
                "2 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 8 | Float | 5\n"
            ),
            "hanging_rank_list": [4, 5],
            "progress_anomalies": [
                {
                    "rank": 5,
                    "pgid": "2",
                    "direction": "behind",
                    "last_enqueued": 9,
                    "baseline_enqueued": 10,
                }
            ],
        }

        assessment = build_rank_candidates(payload)

        self.assertEqual(assessment["primary_suspect_ranks"], [4])
        self.assertEqual(assessment["affected_ranks"], [4, 5])
        self.assertEqual(assessment["downstream_affected_ranks"], [])
        self.assertEqual(assessment["unresolved_affected_ranks"], [5])
        self.assertIn(
            "missing_collective_participation",
            assessment["candidate_ranks"][0]["diagnostic_signals"],
        )

    def test_dominant_repeated_behind_pattern_can_be_primary(self):
        rows = [
            "1 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 8 | Float | 4",
            "2 | TENSOR_MODEL_PARALLEL_GROUP,0 | nccl:broadcast | 8 | Long | 4",
            "3 | EXPERT_DATA_PARALLEL_GROUP,0 | nccl:all_gather | 8 | Float | 4",
            "4 | CONTEXT_PARALLEL_GROUP,0 | nccl:reduce_scatter | 8 | Float | 4",
            "5 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 8 | Float | 5",
        ]
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                + "\n".join(rows)
            ),
            "hanging_rank_list": [4, 5],
            "progress_anomalies": [
                {
                    "rank": 4,
                    "pgid": str(pgid),
                    "direction": "behind",
                    "last_enqueued": 8,
                    "baseline_enqueued": 10,
                    "topology_expected_ranks": [4, 5],
                }
                for pgid in range(1, 5)
            ]
            + [
                {
                    "rank": 5,
                    "pgid": "5",
                    "direction": "behind",
                    "last_enqueued": 9,
                    "baseline_enqueued": 10,
                    "topology_expected_ranks": [4, 5],
                }
            ],
        }

        assessment = build_rank_candidates(payload)

        self.assertEqual(assessment["primary_suspect_ranks"], [4])
        self.assertEqual(
            assessment["classification_basis"],
            "dominant_repeated_progress_anomaly",
        )
        self.assertEqual(assessment["downstream_affected_ranks"], [])
        self.assertEqual(assessment["unresolved_affected_ranks"], [5])
        self.assertEqual(
            assessment["candidate_ranks"][1]["classification_role"],
            "unresolved_affected",
        )

    def test_trace_entry_outlier_is_an_additional_signal(self):
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                "1 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 8 | Float | 4\n"
            ),
            "hanging_rank_list": [4],
            "progress_anomalies": [
                {"rank": 4, "pgid": "1", "direction": "behind"}
            ],
            "trace_summary": {"entry_count_outlier_ranks": [4]},
        }

        assessment = build_rank_candidates(payload)

        self.assertIn(
            "trace_entry_count_outlier",
            assessment["candidate_ranks"][0]["diagnostic_signals"],
        )

    def test_trace_outlier_is_reported_when_collective_table_is_empty(self):
        payload = {
            "analysis_text": "",
            "hanging_rank_list": [],
            "progress_anomalies": [],
            "trace_summary": {"entry_count_outlier_ranks": [4]},
        }

        assessment = build_rank_candidates(payload)

        self.assertEqual(assessment["primary_suspect_ranks"], [4])
        self.assertEqual(assessment["affected_ranks"], [4])
        self.assertIn(
            "trace_entry_count_outlier",
            assessment["candidate_ranks"][0]["diagnostic_signals"],
        )

    def test_affected_ranks_are_sorted_but_primary_keeps_evidence_order(self):
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                "1 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 8 | Float | 1\n"
                "2 | TENSOR_MODEL_PARALLEL_GROUP,0 | nccl:broadcast | 8 | Long | 1\n"
                "3 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 8 | Float | 0\n"
            ),
            "hanging_rank_list": [0, 1],
            "progress_anomalies": [
                {
                    "rank": 1,
                    "pgid": "1",
                    "direction": "behind",
                    "last_enqueued": 8,
                    "baseline_enqueued": 10,
                    "topology_expected_ranks": [0, 1],
                },
                {
                    "rank": 1,
                    "pgid": "2",
                    "direction": "behind",
                    "last_enqueued": 8,
                    "baseline_enqueued": 10,
                    "topology_expected_ranks": [0, 1],
                },
                {
                    "rank": 0,
                    "pgid": "3",
                    "direction": "behind",
                    "last_enqueued": 9,
                    "baseline_enqueued": 10,
                    "topology_expected_ranks": [0, 1],
                },
            ],
            "trace_summary": {"entry_count_outlier_ranks": [1]},
        }

        assessment = build_rank_candidates(payload)

        self.assertEqual(assessment["primary_suspect_ranks"], [1])
        self.assertEqual(assessment["affected_ranks"], [0, 1])
        self.assertEqual(assessment["downstream_affected_ranks"], [])
        self.assertEqual(assessment["unresolved_affected_ranks"], [0])
        rank0 = next(
            item
            for item in assessment["candidate_ranks"]
            if item["rank"] == 0
        )
        self.assertEqual(rank0["confidence_level"], "low")


class TopologyIntegrationTests(unittest.TestCase):
    def _analyzer_with_tp_groups(self, groups):
        analyzer = object.__new__(CollectiveAnalyzer)
        analyzer._reset_run_state()
        analyzer.topology_payload = {"world_size": 64, "groups": {"tp": groups}}
        normalized = [tuple(group) for group in groups]
        analyzer.topology_groups = {"tp": normalized}
        analyzer.topology_rank_index = {
            "tp": {
                rank: (group_index, group)
                for group_index, group in enumerate(normalized)
                for rank in group
            }
        }
        return analyzer

    def test_process_group_description_maps_to_topology_type(self):
        self.assertEqual(_topology_group_type("TENSOR_MODEL_PARALLEL_GROUP"), "tp")
        self.assertEqual(_topology_group_type("DATA_PARALLEL_GROUP_WITH_CP"), "dp-cp")
        self.assertEqual(_topology_group_type("EXPERT_DATA_PARALLEL_GROUP"), "edp")

    def test_topology_excludes_rank_from_colliding_numeric_pg(self):
        analyzer = self._analyzer_with_tp_groups(
            [(12, 13, 14, 15), (40, 41, 42, 43)]
        )
        collectives = [SimpleNamespace(file_id=str(rank)) for rank in range(40, 44)]

        partitions = analyzer._partition_collectives_by_topology(
            ("107", "TENSOR_MODEL_PARALLEL_GROUP", 0),
            collectives,
        )

        self.assertEqual(len(partitions), 1)
        _, match = partitions[0]
        self.assertEqual(match["expected_ranks"], (40, 41, 42, 43))
        self.assertNotIn(14, match["expected_ranks"])

    def test_topology_splits_same_numeric_pg_across_real_groups(self):
        analyzer = self._analyzer_with_tp_groups([(0, 1), (4, 5)])
        collectives = [
            SimpleNamespace(file_id=str(rank)) for rank in (0, 1, 4, 5)
        ]

        partitions = analyzer._partition_collectives_by_topology(
            ("9", "TENSOR_MODEL_PARALLEL_GROUP", 0),
            collectives,
        )

        self.assertEqual(
            [match["expected_ranks"] for _, match in partitions],
            [(0, 1), (4, 5)],
        )
        self.assertEqual(analyzer.topology_stats["split_numeric_pg_groups"], 1)

    def test_topology_text_is_converted_to_sibling_json(self):
        analyzer = object.__new__(CollectiveAnalyzer)
        analyzer._reset_run_state()
        with tempfile.TemporaryDirectory() as directory:
            topo_path = Path(directory) / "topo.txt"
            topo_path.write_text(
                "tp_group: [[0, 1], [2, 3]]\n",
                encoding="utf-8",
            )
            analyzer._load_topology(str(topo_path))
            json_path = topo_path.with_suffix(".json")
            self.assertTrue(json_path.exists())
            self.assertEqual(analyzer.topology_payload["world_size"], 4)
            self.assertEqual(analyzer.topology_groups["tp"][1], (2, 3))

    def test_topology_world_size_must_cover_trace_ranks(self):
        analyzer = object.__new__(CollectiveAnalyzer)
        analyzer._reset_run_state()
        analyzer.topology_payload = {"world_size": 32, "groups": {}}
        analyzer.collectives_by_file = {"0": [], "63": []}
        with self.assertRaisesRegex(ValueError, "does not cover trace ranks: 63"):
            analyzer._validate_topology_trace_ranks()

    def test_end_to_end_topology_removes_numeric_pg_collision(self):
        def entry(*, pg_id, process_group, seq, op):
            return {
                "record_id": seq,
                "collective_seq_id": seq,
                "p2p_seq_id": -1,
                "pg_id": pg_id,
                "op_id": seq,
                "profiling_name": op,
                "state": "scheduled",
                "time_created_ns": seq,
                "time_discovered_started_ns": seq,
                "time_discovered_completed_ns": seq,
                "process_group": process_group,
                "input_sizes": [[1]],
                "output_sizes": [[1]],
                "input_dtypes": ["Float"],
                "output_dtypes": ["Float"],
            }

        with tempfile.TemporaryDirectory() as directory:
            trace_dir = Path(directory) / "trace"
            trace_dir.mkdir()
            for rank in range(8):
                pg_config = {}
                if rank in (0, 2, 4, 6):
                    pg_config["9"] = {
                        "name": "9",
                        "desc": "DATA_PARALLEL_GROUP",
                        "ranks": "[0, 2, 4, 6]",
                    }
                if rank == 1:
                    pg_config["107"] = {
                        "name": "107",
                        "desc": "PIPELINE_MODEL_PARALLEL_GROUP",
                        "ranks": "[1]",
                    }
                if rank in (4, 5, 6, 7):
                    pg_config["107"] = {
                        "name": "107",
                        "desc": "TENSOR_MODEL_PARALLEL_GROUP",
                        "ranks": "[4, 5, 6, 7]",
                    }

                entries = []
                if rank == 0:
                    entries.append(
                        entry(
                            pg_id=2,
                            process_group=["9", "DATA_PARALLEL_GROUP"],
                            seq=4,
                            op="nccl:all_reduce",
                        )
                    )
                if rank in (4, 5, 6, 7):
                    entries.append(
                        entry(
                            pg_id=5,
                            process_group=["107", "TENSOR_MODEL_PARALLEL_GROUP"],
                            seq=169,
                            op="nccl:_reduce_scatter_base",
                        )
                    )

                pg_status = {
                    "2": {
                        "last_enqueued_collective": 4 if rank == 0 else 3,
                        "last_started_collective": -1,
                        "last_completed_collective": 3,
                    },
                    "5": {
                        "last_enqueued_collective": 319 if rank == 1 else 169,
                        "last_started_collective": -1,
                        "last_completed_collective": 160,
                    },
                }
                payload = {
                    "version": "test",
                    "comm_lib_version": "test",
                    "entries": entries,
                    "pg_config": pg_config,
                    "pg_status": pg_status,
                }
                with (trace_dir / f"trace_rank_{rank}").open("wb") as handle:
                    pickle.dump(payload, handle)

            topo_path = Path(directory) / "topo.txt"
            topo_path.write_text(
                "world_size: 8\n"
                "tp_group: [[0, 1, 2, 3], [4, 5, 6, 7]]\n",
                encoding="utf-8",
            )
            base_args = {
                "fr_path": str(trace_dir),
                "pattern": "trace_rank_*",
                "emit_stdout": True,
                "stdout_format": "json",
            }

            plain_analyzer = CollectiveAnalyzer(base_args)
            plain_payload, _ = plain_analyzer.run_sync(base_args)
            self.assertEqual(set(plain_payload["candidate_rank_list"]), {0, 1})

            topology_args = dict(base_args, topo_file=str(topo_path))
            topology_analyzer = CollectiveAnalyzer(topology_args)
            topology_payload, _ = topology_analyzer.run_sync(topology_args)

            self.assertEqual(topology_payload["candidate_rank_list"], [0])
            self.assertIn("结论", topology_payload)
            self.assertNotIn("中文结论", topology_payload)
            self.assertNotIn("结论", topology_payload["rank_assessment"])
            self.assertTrue(topo_path.with_suffix(".json").exists())
            self.assertGreater(
                topology_payload["topology_analysis"]["matched_collective_groups"],
                0,
            )


class StructuredResultTests(unittest.TestCase):
    def test_json_stdout_writes_full_result_and_returns_conclusion_only(self):
        payload = {
            "analysis_text": "large detail",
            "candidate_rank_list": [4, 5],
            "结论": {
                "首要故障嫌疑 Rank": [4],
                "全部受影响 Rank": [4, 5],
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "trace_analyzer_result.json"

            terminal_payload = _write_detailed_json_result(
                payload, output_path
            )

            persisted = json.loads(
                output_path.read_text(encoding="utf-8")
            )
            self.assertEqual(persisted, payload)
            self.assertEqual(
                terminal_payload,
                {
                    "结论": payload["结论"],
                    "详细结果文件": str(output_path),
                },
            )
            self.assertNotIn("analysis_text", terminal_payload)

    def test_candidate_keeps_progress_evidence(self):
        payload = {
            "analysis_text": (
                "PGID | Process Group Desc | Op Type | Size | Dtype | Missing Ranks\n"
                "9 | DATA_PARALLEL_GROUP,0 | nccl:all_reduce | 1 | Float | 4\n"
            ),
            "hanging_rank_list": [4],
            "progress_anomalies": [
                {
                    "rank": 4,
                    "pgid": "9",
                    "direction": "ahead",
                    "last_enqueued": 4,
                    "baseline_enqueued": 3,
                }
            ],
        }
        assessment = build_rank_candidates(payload)
        self.assertEqual(assessment["rank_list"], [4])
        self.assertEqual(
            assessment["candidate_ranks"][0]["progress_anomalies"][0]["direction"],
            "ahead",
        )

    def test_mcp_wrapper_preserves_structured_diagnostics(self):
        result = fr_result_from_mcp_module_response(
            {
                "result": {
                    "analysis_text": "table",
                    "hanging_ranks": "hanging ranks: [4]",
                    "hanging_rank_list": [4],
                    "candidate_rank_list": [4, 5],
                    "rank_assessment": {"recommendation": "ambiguous"},
                    "trace_summary": {"rank_count": 64},
                    "progress_anomalies": [{"rank": 4, "direction": "behind"}],
                    "topology_analysis": {"enabled": True, "world_size": 64},
                }
            }
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.hanging_rank_list, [4])
        self.assertEqual(result.candidate_rank_list, [4, 5])
        self.assertEqual(result.trace_summary["rank_count"], 64)
        self.assertEqual(result.progress_anomalies[0]["direction"], "behind")
        self.assertEqual(result.topology_analysis["world_size"], 64)
        fields = fr_fields_for_dataflow_record(fr_analysis=result)
        self.assertEqual(fields["l_hanging_ranks"], [4])

    def test_trace_rank_pattern_is_auto_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "trace_rank_0").touch()
            self.assertEqual(_fr_pattern_for_path(directory), "trace_rank_*")


if __name__ == "__main__":
    unittest.main()
