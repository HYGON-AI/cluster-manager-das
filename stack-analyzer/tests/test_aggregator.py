# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import tempfile
from pathlib import Path

from stack_analyzer.aggregator import StackAggregator
from examples.pp_hang_scenario import build_demo_snapshots, build_demo_topology


def test_pp_hang_finds_outlier_pp_group():
    snapshots = build_demo_snapshots()
    topology = build_demo_topology()
    rank_to_machine = {s.rank: s.machine_id for s in snapshots}

    result = StackAggregator().aggregate(
        snapshots,
        topology=topology,
        rank_to_machine=rank_to_machine,
    )

    assert result.outlier_machines == {
        "machine-3",
        "machine-7",
        "machine-11",
        "machine-15",
    }
    assert result.isolation_group is not None
    label, ranks = result.isolation_group
    assert label == "PP"
    assert ranks == frozenset({3, 7, 11, 15})
    assert result.machines_to_evict == result.outlier_machines


def test_dominant_group_is_healthy_majority():
    snapshots = build_demo_snapshots()
    result = StackAggregator().aggregate(snapshots)
    assert len(result.dominant_group.snapshots) == 12
    assert "all_reduce" in result.dominant_signature


def test_analyze_json_roundtrip():
    snapshots = build_demo_snapshots()
    payload = [
        {
            "machine_id": s.machine_id,
            "rank": s.rank,
            "pid": s.pid,
            "role": s.role.value,
            "frames": [
                {"function": f.function, "file": f.file, "line": f.line}
                for f in s.frames
            ],
        }
        for s in snapshots
    ]
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "stacks.json"
        path.write_text(json.dumps(payload), encoding="utf-8")

        from main import _load_snapshots_from_json

        loaded = _load_snapshots_from_json(path)
        assert len(loaded) == 16
