# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path

from stack_analyzer.coordinator import load_topology, snapshots_to_json
from stack_analyzer.models import ProcessRole, StackFrame, StackSnapshot


def test_load_topology_normalizes_groups(monkeypatch):
    payload = {
        "pp_groups": [[0, 2], [1, 3]],
        "tp_groups": [[0, 1]],
        "dp_groups": [],
    }
    monkeypatch.setattr(
        "pathlib.Path.read_text",
        lambda _path, encoding=None: json.dumps(payload),
    )

    topology = load_topology(Path("topology.json"))

    assert topology.pp_groups == [frozenset({0, 2}), frozenset({1, 3})]
    assert topology.tp_groups == [frozenset({0, 1})]
    assert topology.dp_groups == []


def test_snapshots_to_json_preserves_capture_contract():
    snapshot = StackSnapshot(
        machine_id="node-a",
        rank=3,
        pid=42,
        role=ProcessRole.TRAINER,
        frames=(StackFrame("forward", "/workspace/model.py", 17),),
        raw_text="raw stack",
    )

    assert snapshots_to_json([snapshot]) == [
        {
            "machine_id": "node-a",
            "rank": 3,
            "pid": 42,
            "role": "trainer",
            "frames": [
                {
                    "function": "forward",
                    "file": "/workspace/model.py",
                    "line": 17,
                }
            ],
            "raw_text": "raw stack",
        }
    ]
