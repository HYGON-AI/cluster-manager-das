# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import unittest

from hcu_envcheck.rccl_output import parse_rccl_tests_output


def row(
    size,
    *,
    out_time="10.0",
    out_algbw="12.0",
    out_busbw="11.0",
    out_wrong="0",
    in_time="9.0",
    in_algbw="13.0",
    in_busbw="12.0",
    in_wrong="0",
):
    return (
        f"{size} {size // 4} float sum -1 "
        f"{out_time} {out_algbw} {out_busbw} {out_wrong} "
        f"{in_time} {in_algbw} {in_busbw} {in_wrong}"
    )


def output(*rows, minimum=8, maximum=8, factor=2, extra=""):
    table = "\n".join(rows)
    return f"""# nThread 1 nGpus 1 minBytes {minimum} maxBytes {maximum} step: {factor}(factor)
# Using devices
{extra}
# size count type redop root time algbw busbw #wrong time algbw busbw #wrong
{table}
# Out of bounds values : 0
# Avg bus bandwidth    : 12.5
"""


def issue_codes(result):
    return {issue.code for issue in result.issues}


class RcclOutputParserTests(unittest.TestCase):
    def test_realistic_complete_output_is_accepted(self):
        text = output(
            row(8),
            row(16),
            row(32),
            minimum=8,
            maximum=32,
            extra="""#   Rank  0 Pid 100 on node-a device  0 [0000:36:00.0] BW
#   Rank  1 Pid 200 on node-b device  0 [0000:77:00.0] BW
node-a:100:101 [0] NCCL INFO Using network IBext_v8
node-b:200:201 [0] NCCL INFO Using network IBext_v8
node-a:100:101 [0] NCCL INFO ncclCommInitRank rank 0 nranks 2
node-b:200:201 [0] NCCL INFO ncclCommInitRank rank 1 nRanks 02
node-a:100:101 [0] NCCL INFO Channel 00 via NET/IBext_v8/0/GDRDMA
node-b:200:201 [0] NCCL INFO GPU Direct RDMA Enabled""",
        )

        result = parse_rccl_tests_output(
            text,
            expected_nranks=2,
            expected_devices_per_node={"node-a": 1, "node-b": 1},
        )

        self.assertTrue(result.valid, result.issues)
        self.assertEqual(result.expected_sizes, (8, 16, 32))
        self.assertEqual(result.observed_sizes, (8, 16, 32))
        self.assertEqual(result.nranks_values, (2,))
        self.assertEqual([item.rank for item in result.devices], [0, 1])
        evidence = {item.hostname: item for item in result.host_transports}
        self.assertEqual(evidence["node-a"].transport, "IBEXT")
        self.assertEqual(evidence["node-a"].gdr_state, "ENABLED")
        self.assertEqual(evidence["node-b"].gdr_state, "ENABLED")
        json.dumps(result.to_dict())

    def test_interleaved_nccl_logs_and_ok_summary_suffix_are_supported(self):
        text = """# nThread 1 nGpus 1 minBytes 8 maxBytes 8 step: 2(factor)
# size count type redop root time algbw busbw #wrong time algbw busbw #wrong
node-a:10:11 [0] NCCL INFO ncclCommInitRank rank 0 nranks 1 - Init COMPLETE
8 2 float sum -1 10 1 1 0 10 1 1 0
node-a:10:11 [0] NCCL INFO comm rank 0 nranks 1 - Destroy COMPLETE
# Errors with asterisks indicate errors that have exceeded the maximum threshold.
# Out of bounds values : 0 OK
# Avg bus bandwidth : 1.0
"""
        result = parse_rccl_tests_output(text)

        self.assertTrue(result.valid, result.issues)
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.summary.out_of_bounds_values, (0,))

    def test_summary_only_is_parsed_but_not_accepted_as_results(self):
        text = """# Out of bounds values : 0
# Avg bus bandwidth    : 12.1968
"""
        result = parse_rccl_tests_output(
            text, min_bytes=8, max_bytes=8, step_factor=2
        )

        self.assertFalse(result.valid)
        self.assertEqual(result.summary.out_of_bounds_values, (0,))
        self.assertEqual(result.summary.average_bus_bandwidths, (12.1968,))
        self.assertIn("TABLE_ROWS_MISSING", issue_codes(result))
        self.assertIn("EXPECTED_SIZE_MISSING", issue_codes(result))

    def test_missing_generated_size_is_rejected(self):
        result = parse_rccl_tests_output(
            output(row(8), row(32), minimum=8, maximum=32)
        )

        self.assertFalse(result.valid)
        self.assertIn("EXPECTED_SIZE_MISSING", issue_codes(result))
        self.assertTrue(any("16" in item.message for item in result.issues))

    def test_nan_and_zero_layout_metrics_are_rejected(self):
        result = parse_rccl_tests_output(
            output(row(8, out_time="nan", in_algbw="0"))
        )

        self.assertFalse(result.valid)
        self.assertIn("LAYOUT_METRIC_NOT_FINITE", issue_codes(result))
        self.assertIn("LAYOUT_METRIC_NOT_POSITIVE", issue_codes(result))

    def test_nonzero_and_starred_wrong_values_are_rejected(self):
        result = parse_rccl_tests_output(
            output(row(8, out_wrong="2*", in_wrong="0*"))
        )

        self.assertFalse(result.valid)
        self.assertIn("WRONG_VALUE_NONZERO", issue_codes(result))
        self.assertIn("WRONG_VALUE_STARRED", issue_codes(result))

    def test_conflicting_world_sizes_are_rejected(self):
        text = output(
            row(8),
            extra="""# Rank 0 Pid 10 on node-a device 0 [0000:01:00.0]
# Rank 1 Pid 20 on node-b device 0 [0000:02:00.0]
node-a:10:11 [0] NCCL INFO rank 0 nranks 2
node-b:20:21 [0] NCCL INFO rank 1 nranks=4""",
        )
        result = parse_rccl_tests_output(text, expected_nranks=2)

        self.assertEqual(result.nranks_values, (2, 4))
        self.assertIn("NRANKS_MISMATCH", issue_codes(result))

    def test_missing_rank_assignment_is_rejected(self):
        text = output(
            row(8),
            extra="""# Rank 0 Pid 10 on node-a device 0 [0000:01:00.0]
node-a:10:11 [0] NCCL INFO rank 0 nranks 2""",
        )
        result = parse_rccl_tests_output(text, expected_nranks=2)

        self.assertIn("DEVICE_RANK_MISSING", issue_codes(result))
        self.assertTrue(any("rank 1" in item.message for item in result.issues))

    def test_duplicate_rank_and_device_are_rejected(self):
        text = output(
            row(8),
            extra="""# Rank 0 Pid 10 on node-a device 0 [0000:01:00.0]
# Rank 0 Pid 11 on node-a device 0 [0000:01:00.0]
node-a:10:12 [0] NCCL INFO rank 0 nranks 1""",
        )
        result = parse_rccl_tests_output(text, expected_nranks=1)

        codes = issue_codes(result)
        self.assertIn("DEVICE_RANK_DUPLICATE", codes)
        self.assertIn("DEVICE_INDEX_DUPLICATE", codes)
        self.assertIn("DEVICE_BDF_DUPLICATE", codes)

    def test_per_node_device_count_mapping_is_exact(self):
        text = output(
            row(8),
            extra="""# Rank 0 Pid 10 on node-a device 0 [0000:01:00.0]
node-a:10:12 [0] NCCL INFO rank 0 nranks 1""",
        )
        result = parse_rccl_tests_output(
            text,
            expected_nranks=1,
            expected_devices_per_node={"node-a": 1, "node-b": 1},
        )

        self.assertIn("DEVICE_COUNT_PER_NODE_MISMATCH", issue_codes(result))
        self.assertTrue(any("node-b" in item.message for item in result.issues))

    def test_socket_transport_is_extracted_without_being_silently_called_ib(self):
        text = output(
            row(8),
            extra="node-a:10:11 [0] NCCL INFO Using network Socket_v8",
        )
        result = parse_rccl_tests_output(text)

        self.assertTrue(result.valid, result.issues)
        evidence = {item.hostname: item for item in result.host_transports}
        self.assertEqual(evidence["node-a"].transport, "SOCKET")
        self.assertEqual(evidence["node-a"].using_networks, ("Socket_v8",))

    def test_selected_gdrrdma_outranks_candidate_hca_disabled_probe(self):
        text = output(
            row(8),
            extra="""node-a:10:11 [0] NCCL INFO GPU Direct RDMA Disabled for HCA 0
node-a:10:11 [0] NCCL INFO Channel 00 via NET/IBext_v8/0/GDRDMA""",
        )
        result = parse_rccl_tests_output(text)

        self.assertTrue(result.valid, result.issues)
        self.assertEqual(result.host_transports[0].gdr_state, "ENABLED")
        self.assertEqual(result.host_transports[0].gdr_probe_disabled_marker_count, 1)

    def test_contradictory_final_gdr_evidence_is_a_conflict(self):
        text = output(
            row(8),
            extra="""node-a:10:11 [0] NCCL INFO GPU Direct RDMA Disabled
node-a:10:11 [0] NCCL INFO Channel 00 via NET/IBext_v8/0/GDRDMA""",
        )
        result = parse_rccl_tests_output(text)

        self.assertIn("GDR_EVIDENCE_CONFLICT", issue_codes(result))
        self.assertEqual(result.host_transports[0].gdr_state, "CONFLICT")

    def test_candidate_hca_disabled_without_selected_gdrrdma_stays_disabled(self):
        result = parse_rccl_tests_output(
            output(
                row(8),
                extra=(
                    "node-a:10:11 [0] NCCL INFO NET/IBext_v8 : "
                    "GPU Direct RDMA Disabled for HCA 0 'shca_0'"
                ),
            )
        )

        self.assertTrue(result.valid, result.issues)
        self.assertEqual(result.host_transports[0].gdr_state, "DISABLED")

    def test_plain_ib_selected_network_is_rdma_evidence(self):
        result = parse_rccl_tests_output(
            output(row(8), extra="node-a:10:11 [0] NCCL INFO Using network IB")
        )

        self.assertTrue(result.valid, result.issues)
        self.assertEqual(result.host_transports[0].transport, "IBEXT")

    def test_gdrcopy_disabled_message_is_not_data_path_gdr_disabled(self):
        text = output(
            row(8),
            extra=(
                "node-a:10:11 [0] NCCL INFO Disabled GDRCopy equivalent memory "
                "allocation on gfx936 due to GPU architecture"
            ),
        )
        result = parse_rccl_tests_output(text)


        self.assertTrue(result.valid, result.issues)
        self.assertEqual(result.host_transports[0].gdr_state, "UNKNOWN")
    def test_missing_out_of_bounds_summary_is_rejected(self):
        text = output(row(8)).replace("# Out of bounds values : 0\n", "")
        result = parse_rccl_tests_output(text)

        self.assertFalse(result.valid)
        self.assertIn(
            "SUMMARY_OUT_OF_BOUNDS_MISSING",
            issue_codes(result),
        )


if __name__ == "__main__":
    unittest.main()
