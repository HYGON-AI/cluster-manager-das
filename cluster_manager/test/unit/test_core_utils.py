# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import pytest

from cluster_manager.event.event_bus import Event, EventBus
from cluster_manager.monitor.log_event_sink import (
    EventBusLogSink,
    FeishuLogSink,
    build_k8s_fault_report,
    create_log_event_sink,
    is_fault_event,
)
from cluster_manager.parallel.topology_validator import ParallelTopologyValidator
from cluster_manager.utils.string_utils import (
    compress_nodes,
    match_failed_nodes,
    node_expr_parser,
    split_outside_brackets,
)


def test_event_bus_preserves_publish_order_and_empty_semantics():
    """Consumers receive queued events in publication order and then no event.

    The empty queue sentinel is intentionally ``None``.
    """
    bus = EventBus()
    first = Event("first", {"value": 1})
    second = Event("second")

    bus.publish(first)
    bus.publish(second)

    assert bus.get_event_nowait() is first
    assert bus.get_event_nowait() is second
    assert bus.get_event_nowait() is None


def test_node_expression_parser_expands_multiple_ranges():
    assert split_outside_brackets("rack[01-02],rack[04,06]") == [
        "rack[01-02]",
        "rack[04,06]",
    ]
    assert node_expr_parser("r[01-02]n[1,3]") == [
        "r01n1",
        "r01n3",
        "r02n1",
        "r02n3",
    ]


def test_failed_node_matching_and_compression():
    output = (
        "REASON USER TIMESTAMP STATE NODELIST\n"
        "maintenance root 2026-01-01 down node[01-02]\n"
    )

    assert match_failed_nodes(output, ["node01", "node03"]) == {
        "node01": "maintenance"
    }
    assert compress_nodes(["node01", "node02", "node04"]) == "node[01-02,04]"


def test_parallel_topology_world_size_and_moe_validation():
    assert ParallelTopologyValidator.get_world_size(
        {"required_nodes_num": 4, "slots_per_node": 8}
    ) == 32
    ParallelTopologyValidator.validate_moe({"num_experts": 8}, ep=4)
    with pytest.raises(RuntimeError, match="not divisible"):
        ParallelTopologyValidator.validate_moe({"num_experts": 7}, ep=4)


def test_log_event_sink_classifies_root_cause_for_k8s():
    event = Event(
        "LOG_MONITOR",
        {
            "type": "exit",
            "data": {
                "type": "root_cause",
                "host": "trainer-2.training.svc",
                "rank": 16,
                "local_rank": 0,
                "exit_code": 1,
            },
        },
    )
    report = build_k8s_fault_report(
        event,
        {
            "FT_POD_NAME": "trainer-0",
            "FT_POD_NAMESPACE": "training",
            "FT_JOB_NAME": "job-a",
        },
    )

    assert is_fault_event(event)
    assert report["type"] == "TrainingRootCause"
    assert report["faultClass"] == "root_cause"
    assert report["confidence"] == 80
    assert report["podName"] == "trainer-2"
    assert report["action"]["taintNode"] is True


def test_log_event_sink_factory_selects_expected_adapter():
    bus = EventBus()
    notify = object()

    assert isinstance(create_log_event_sink("cluster", event_bus=bus), EventBusLogSink)
    assert isinstance(create_log_event_sink("standalone", notify=notify), FeishuLogSink)
