# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import tempfile
from pathlib import Path

from stack_analyzer.ansible_collector import (
    parse_ansible_adhoc_output,
    stacks_from_payload,
)
from stack_analyzer.hostfile import (
    infer_topology,
    parse_hostfile,
    rank_to_machine,
    world_size,
    write_ansible_inventory,
)


def test_parse_hostfile():
    with tempfile.TemporaryDirectory() as tmp_dir:
        hostfile = Path(tmp_dir) / "hosts"
        hostfile.write_text(
            "node0 slots=4\nnode1 slots=4 ansible_host=10.0.0.2\n",
            encoding="utf-8",
        )
        hosts = parse_hostfile(hostfile)
    assert len(hosts) == 2
    assert hosts[0].hostname == "node0"
    assert hosts[0].ranks == [0, 1, 2, 3]
    assert hosts[1].rank_start == 4
    assert hosts[1].ansible_host == "10.0.0.2"
    assert world_size(hosts) == 8


def test_infer_pp_topology_megatron_default():
    hostfile = Path(__file__).resolve().parent.parent / "examples" / "hostfile.example"
    hosts = parse_hostfile(hostfile)
    topo = infer_topology(hosts, pp_size=4, tp_size=1, dp_size=4)
    pp_with_12 = [g for g in topo.pp_groups if 12 in g][0]
    assert pp_with_12 == frozenset({0, 4, 8, 12})
    dp_last = [g for g in topo.dp_groups if 15 in g][0]
    assert dp_last == frozenset({12, 13, 14, 15})


def test_infer_pp_topology_tp_pp_dp_mapping():
    hostfile = Path(__file__).resolve().parent.parent / "examples" / "hostfile.example"
    hosts = parse_hostfile(hostfile)
    topo = infer_topology(
        hosts,
        pp_size=4,
        tp_size=1,
        dp_size=4,
        use_tp_pp_dp_mapping=True,
    )
    pp_last = [g for g in topo.pp_groups if 12 in g][0]
    assert pp_last == frozenset({12, 13, 14, 15})


def test_parse_ansible_output():
    sample = """
worker00 | CHANGED | rc=0 >>
[{"machine_id": "worker00", "rank": 0, "pid": 100, "role": "trainer", "frames": [{"function": "all_reduce", "file": "", "line": 0}], "raw_text": ""}]
worker01 | CHANGED | rc=0 >>
[{"machine_id": "worker01", "rank": 4, "pid": 200, "role": "trainer", "frames": [{"function": "isend", "file": "", "line": 0}], "raw_text": ""}]
""".strip()
    parsed = parse_ansible_adhoc_output(sample)
    assert set(parsed) == {"worker00", "worker01"}
    snaps = stacks_from_payload(parsed["worker00"], fallback_host="worker00", rank_start=0)
    assert snaps[0].rank == 0
    assert snaps[0].frames[0].function == "all_reduce"


def test_inventory_contains_rank_vars():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        hostfile = tmp_path / "hosts"
        hostfile.write_text("a slots=2\nb slots=2\n", encoding="utf-8")
        hosts = parse_hostfile(hostfile)
        inv = write_ansible_inventory(
            hosts, default_user="root", output_path=tmp_path / "inv.ini"
        )
        text = inv.read_text(encoding="utf-8")
    assert "rank_start=0" in text
    assert "rank_start=2" in text
    assert "ansible_user=root" in text


def test_rank_to_machine():
    hostfile = Path(__file__).resolve().parent.parent / "examples" / "hostfile.example"
    hosts = parse_hostfile(hostfile)
    mapping = rank_to_machine(hosts)
    assert mapping[0] == "worker00"
    assert mapping[15] == "worker03"
