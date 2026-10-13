from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from topo_to_json import (
    TopologyFormatError,
    convert_topology_file,
    parse_topology_text,
)


SAMPLE_TOPOLOGY = """\
tp_group: [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14, 15], [16, 17, 18, 19], [20, 21, 22, 23], [24, 25, 26, 27], [28, 29, 30, 31]]
pp_group: [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28], [29], [30], [31]]
dp_group: [[0, 4, 8, 12, 16, 20, 24, 28], [1, 5, 9, 13, 17, 21, 25, 29], [2, 6, 10, 14, 18, 22, 26, 30], [3, 7, 11, 15, 19, 23, 27, 31]]
ep_group: [[0, 2], [1, 3], [4, 6], [5, 7], [8, 10], [9, 11], [12, 14], [13, 15], [16, 18], [17, 19], [20, 22], [21, 23], [24, 26], [25, 27], [28, 30], [29, 31]]
etp_group: [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10, 11], [12, 13], [14, 15], [16, 17], [18, 19], [20, 21], [22, 23], [24, 25], [26, 27], [28, 29], [30, 31]]
edp_group: [[0, 4, 8, 12, 16, 20, 24, 28], [1, 5, 9, 13, 17, 21, 25, 29], [2, 6, 10, 14, 18, 22, 26, 30], [3, 7, 11, 15, 19, 23, 27, 31]]
cp_group: [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28], [29], [30], [31]]
tp-cp_group: [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14, 15], [16, 17, 18, 19], [20, 21, 22, 23], [24, 25, 26, 27], [28, 29, 30, 31]]
embd-pp_group: [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28], [29], [30], [31]]
pos_embd-pp_group: [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28], [29], [30], [31]]
tp-ep_group: [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14, 15], [16, 17, 18, 19], [20, 21, 22, 23], [24, 25, 26, 27], [28, 29, 30, 31]]
tp-dp-cp_group: [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]]
tp-pp_group: [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11], [12, 13, 14, 15], [16, 17, 18, 19], [20, 21, 22, 23], [24, 25, 26, 27], [28, 29, 30, 31]]
dp-cp_group: [[0, 4, 8, 12, 16, 20, 24, 28], [1, 5, 9, 13, 17, 21, 25, 29], [2, 6, 10, 14, 18, 22, 26, 30], [3, 7, 11, 15, 19, 23, 27, 31]]
"""


class TopologyParserTests(unittest.TestCase):
    def test_user_sample_is_converted(self) -> None:
        payload = parse_topology_text(SAMPLE_TOPOLOGY)

        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["world_size"], 32)
        self.assertEqual(
            payload["parallel_sizes"],
            {"tp": 4, "pp": 1, "cp": 1, "ep": 2, "etp": 2},
        )
        self.assertEqual(payload["groups"]["tp"][0], [0, 1, 2, 3])
        self.assertEqual(
            payload["groups"]["dp"][0],
            [0, 4, 8, 12, 16, 20, 24, 28],
        )
        self.assertIn("tp-cp", payload["groups"])
        self.assertIn("pos_embd-pp", payload["groups"])
        self.assertNotIn("tp_group", payload["groups"])

    def test_multiline_group_and_declared_sizes(self) -> None:
        payload = parse_topology_text(
            """\
world_size: 4
tp_size: 2
tp_group: [
  [0, 1],
  [2, 3]
]
dp_group: [[0, 2], [1, 3]]
"""
        )

        self.assertEqual(payload["world_size"], 4)
        self.assertEqual(payload["parallel_sizes"]["tp"], 2)
        self.assertEqual(payload["parallel_sizes"]["pp"], 1)

    def test_world_size_must_match_rank_coverage(self) -> None:
        with self.assertRaisesRegex(TopologyFormatError, "缺少"):
            parse_topology_text(
                "tp_group: [[0, 1], [2, 3]]\n",
                world_size_override=8,
            )

    def test_duplicate_rank_is_rejected(self) -> None:
        with self.assertRaisesRegex(TopologyFormatError, "重复包含 rank 1"):
            parse_topology_text("tp_group: [[0, 1], [1, 2]]\n")

    def test_non_literal_input_is_rejected(self) -> None:
        with self.assertRaisesRegex(TopologyFormatError, "二维整数列表"):
            parse_topology_text(
                "tp_group: __import__('os').system('echo unsafe')\n"
            )

    def test_file_conversion_writes_valid_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "topo.txt"
            output_path = Path(directory) / "topo.json"
            input_path.write_text(SAMPLE_TOPOLOGY, encoding="utf-8")

            payload = convert_topology_file(input_path, output_path)
            written = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(written, payload)
        self.assertEqual(written["world_size"], 32)


if __name__ == "__main__":
    unittest.main()
