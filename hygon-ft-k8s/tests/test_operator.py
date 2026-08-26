# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import threading
import sys
from types import SimpleNamespace
from unittest import mock


class ApiException(Exception):
    def __init__(self, status=None):
        super().__init__(f"status={status}")
        self.status = status


kubernetes = mock.Mock()
kubernetes.client = mock.Mock()
kubernetes.config = mock.Mock()
kubernetes.watch = mock.Mock()
kubernetes.client.ApiException = ApiException
sys.modules.setdefault("kubernetes", kubernetes)
sys.modules.setdefault("kubernetes.client", kubernetes.client)

from hygon_ft.operator import controller as controller_module
from hygon_ft.operator.controller import (
    FaultController,
    fault_event_restart_readiness,
    select_root_fault,
)


def fault(name, fault_class, observed_at, node="node-a"):
    return {
        "metadata": {"name": name, "namespace": "hygon-ft"},
        "spec": {
            "jobName": "train-job",
            "nodeName": node,
            "faultClass": fault_class,
            "observedAt": observed_at,
            "action": {"taintNode": True, "deletePod": False, "deletePods": False},
        },
    }


def bare_controller():
    controller = object.__new__(FaultController)
    controller.namespace = "hygon-ft"
    controller.ranks_per_pod = 8
    controller.core_api = mock.Mock()
    controller.custom_api = mock.Mock()
    controller._leader_lock = threading.Lock()
    controller._is_leader = True
    controller._aggregation_lock = threading.Lock()
    controller._pending_faults = {}
    controller._aggregation_timers = {}
    controller.delete_pod_after_taint = True
    return controller


def processing_fault(fault_type="TrainingRootCause", workload_kind="PyTorchJob"):
    return {
        "metadata": {"name": "fault-1", "namespace": "hygon-ft"},
        "spec": {
            "type": fault_type,
            "nodeName": "node-a",
            "podNamespace": "default",
            "podName": "train-worker-0",
            "jobName": "train-job",
            "workloadKind": workload_kind,
            "action": {
                "taintNode": True,
                "deletePod": False,
                "deletePods": False,
            },
        },
    }


def test_select_root_fault_uses_priority_before_arrival_time():
    communication = fault("comm", "communication", "2026-07-15T09:00:00Z")
    root_cause = fault("root", "root_cause", "2026-07-15T09:00:02Z")
    explicit = fault("node", "explicit_node", "2026-07-15T09:00:04Z")

    assert select_root_fault([communication, root_cause, explicit]) is explicit


def test_select_root_fault_uses_earliest_event_for_equal_priority():
    later = fault("later", "root_cause", "2026-07-15T09:00:02Z")
    earlier = fault("earlier", "root_cause", "2026-07-15T09:00:01Z")

    assert select_root_fault([later, earlier]) is earlier


def test_reporter_resolves_root_cause_host_to_pod_node():
    controller = bare_controller()
    controller.core_api.read_namespaced_pod.return_value = SimpleNamespace(
        spec=SimpleNamespace(node_name="node38")
    )
    controller.custom_api.create_namespaced_custom_object.return_value = {
        "metadata": {"name": "launcher-root"}
    }

    name = controller.create_fault_event_from_report(
        {
            "type": "TrainingRootCause",
            "source": "log-monitor",
            "podNamespace": "default",
            "podName": "reporting-pod",
            "host": "megatron-train-job-worker-0.megatron-headless.default.svc.cluster.local",
            "jobName": "megatron-train-job",
            "workloadKind": "PyTorchJob",
            "rank": 9,
            "localRank": 1,
            "exitCode": 1,
            "faultClass": "root_cause",
            "confidence": 80,
            "action": {"taintNode": True, "deletePods": True},
        }
    )

    assert name == "launcher-root"
    body = controller.custom_api.create_namespaced_custom_object.call_args.args[-1]
    assert body["spec"]["podName"] == "megatron-train-job-worker-0"
    assert body["spec"]["nodeName"] == "node38"
    assert body["spec"]["workloadKind"] == "PyTorchJob"
    assert body["spec"]["rank"] == 9
    controller.core_api.read_namespaced_pod.assert_called_once_with(
        "megatron-train-job-worker-0", "default"
    )


def test_rank_mapping_uses_eight_ranks_per_pod():
    controller = bare_controller()
    master = SimpleNamespace(
        metadata=SimpleNamespace(
            name="megatron-train-job-master-0",
            labels={"ft.hygon.io/replica-role": "master"},
        ),
        spec=SimpleNamespace(node_name="node37"),
    )
    worker = SimpleNamespace(
        metadata=SimpleNamespace(
            name="megatron-train-job-worker-0",
            labels={"ft.hygon.io/replica-role": "worker"},
        ),
        spec=SimpleNamespace(node_name="node38"),
    )
    controller.core_api.list_namespaced_pod.return_value = SimpleNamespace(
        items=[master, worker]
    )

    assert controller.resolve_pod_from_rank("default", "megatron-train-job", 9) == (
        "megatron-train-job-worker-0",
        "node38",
    )


def test_job_aggregation_processes_only_highest_priority_event():
    controller = bare_controller()
    communication = fault("comm", "communication", "2026-07-15T09:00:00Z")
    root_cause = fault("root", "root_cause", "2026-07-15T09:00:02Z")
    explicit = fault("node", "explicit_node", "2026-07-15T09:00:04Z")
    group_key = "hygon-ft/train-job"
    controller._pending_faults[group_key] = {
        "comm": communication,
        "root": root_cause,
        "node": explicit,
    }
    controller._aggregation_timers[group_key] = mock.Mock()
    controller._process_fault = mock.Mock()
    controller.patch_status = mock.Mock()

    controller._resolve_fault_group(group_key)

    controller._process_fault.assert_called_once_with(explicit)
    suppressed = {call.args[0] for call in controller.patch_status.call_args_list}
    assert suppressed == {"comm", "root"}


def test_training_root_cause_restarts_all_pytorchjob_pods_after_taint_succeeds():
    controller = bare_controller()
    controller.taint_node = mock.Mock(
        return_value={"type": "TaintNode", "nodeName": "node-a", "success": True}
    )
    controller.delete_ft_job_pods = mock.Mock(
        return_value=[
            {"type": "DeletePod", "podName": "train-master-0", "success": True},
            {"type": "DeletePod", "podName": "train-worker-0", "success": True},
        ]
    )
    controller.patch_status = mock.Mock()
    controller.send_alert = mock.Mock()

    controller._process_fault(processing_fault())

    controller.taint_node.assert_called_once_with("node-a", "TrainingRootCause")
    controller.delete_ft_job_pods.assert_called_once_with("default", "train-job")


def test_training_root_cause_without_workload_kind_restarts_detected_pytorchjob():
    controller = bare_controller()
    controller.taint_node = mock.Mock(
        return_value={"type": "TaintNode", "nodeName": "node-a", "success": True}
    )
    controller.custom_api.get_namespaced_custom_object.return_value = {
        "kind": "PyTorchJob",
        "metadata": {"name": "train-job"},
    }
    controller.delete_ft_job_pods = mock.Mock(
        return_value=[{"type": "DeletePod", "podName": "train-worker-0", "success": True}]
    )
    controller.patch_status = mock.Mock()
    controller.send_alert = mock.Mock()

    controller._process_fault(processing_fault(workload_kind=""))

    controller.custom_api.get_namespaced_custom_object.assert_called_once_with(
        "kubeflow.org",
        "v1",
        "default",
        "pytorchjobs",
        "train-job",
    )
    controller.delete_ft_job_pods.assert_called_once_with("default", "train-job")


def test_node_fault_restarts_jobs_on_fault_node():
    controller = bare_controller()
    controller.taint_node = mock.Mock(
        return_value={"type": "TaintNode", "nodeName": "node-a", "success": True}
    )
    controller.core_api.list_pod_for_all_namespaces.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(
                metadata=SimpleNamespace(
                    namespace="default",
                    name="train-worker-0",
                    labels={"ft.hygon.io/job-name": "train-job"},
                ),
                status=SimpleNamespace(phase="Running"),
            )
        ]
    )
    controller.delete_ft_job_pods = mock.Mock(
        return_value=[{"type": "DeletePod", "podName": "train-master-0", "success": True}]
    )
    controller.patch_status = mock.Mock()
    controller.send_alert = mock.Mock()

    fault_event = {
        "metadata": {"name": "fault-1", "namespace": "hygon-ft"},
        "spec": {
            "type": "NodeHealthCheckFailed",
            "nodeName": "node-a",
            "action": {"taintNode": True, "deletePod": False, "deletePods": False},
        },
    }

    controller._process_fault(fault_event)

    controller.core_api.list_pod_for_all_namespaces.assert_called_once_with(
        field_selector="spec.nodeName=node-a",
        label_selector="ft.hygon.io/enabled=true",
    )
    controller.delete_ft_job_pods.assert_called_once_with("default", "train-job")


def test_training_root_cause_does_not_delete_pod_when_taint_fails():
    controller = bare_controller()
    controller.taint_node = mock.Mock(
        return_value={
            "type": "TaintNode",
            "nodeName": "node-a",
            "success": False,
            "message": "forbidden",
        }
    )
    controller.delete_ft_job_pods = mock.Mock()
    controller.patch_status = mock.Mock()
    controller.send_alert = mock.Mock()

    controller._process_fault(processing_fault())

    controller.delete_ft_job_pods.assert_not_called()
    actions = controller.patch_status.call_args.args[1]
    assert any(
        action.get("type") == "DeletePod"
        and action.get("success") is False
        and "taint failed" in action.get("message", "")
        for action in actions
    )


def test_generic_taint_does_not_implicitly_delete_pod():
    controller = bare_controller()
    controller.taint_node = mock.Mock(
        return_value={"type": "TaintNode", "nodeName": "node-a", "success": True}
    )
    controller.delete_ft_job_pods = mock.Mock()
    controller.patch_status = mock.Mock()
    controller.send_alert = mock.Mock()

    controller._process_fault(processing_fault("TrainingFault"))

    controller.delete_ft_job_pods.assert_not_called()


def test_explicit_training_node_fault_restarts_all_pytorchjob_pods():
    controller = bare_controller()
    controller.taint_node = mock.Mock(
        return_value={"type": "TaintNode", "nodeName": "node-a", "success": True}
    )
    controller.delete_ft_job_pods = mock.Mock(
        return_value=[
            {"type": "DeletePod", "podName": "train-master-0", "success": True},
            {"type": "DeletePod", "podName": "train-worker-0", "success": True},
        ]
    )
    controller.patch_status = mock.Mock()
    controller.send_alert = mock.Mock()

    controller._process_fault(processing_fault("TrainingNodeFault"))

    controller.delete_ft_job_pods.assert_called_once_with("default", "train-job")


def test_volcano_root_cause_leaves_restart_to_volcano_policy():
    controller = bare_controller()
    controller.taint_node = mock.Mock(
        return_value={"type": "TaintNode", "nodeName": "node-a", "success": True}
    )
    controller.delete_ft_job_pods = mock.Mock()
    controller.delete_named_pod = mock.Mock()
    controller.patch_status = mock.Mock()
    controller.send_alert = mock.Mock()

    controller._process_fault(processing_fault(workload_kind="VolcanoJob"))

    controller.delete_ft_job_pods.assert_not_called()
    controller.delete_named_pod.assert_not_called()


def test_delete_ft_job_pods_deletes_every_active_replica():
    controller = bare_controller()
    controller.delete_grace_seconds = 0
    controller.core_api.list_namespaced_pod.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(
                metadata=SimpleNamespace(name="train-master-0"),
                status=SimpleNamespace(phase="Running"),
            ),
            SimpleNamespace(
                metadata=SimpleNamespace(name="train-worker-0"),
                status=SimpleNamespace(phase="Running"),
            ),
            SimpleNamespace(
                metadata=SimpleNamespace(name="train-worker-old"),
                status=SimpleNamespace(phase="Failed"),
            ),
        ]
    )

    actions = controller.delete_ft_job_pods("default", "train-job")

    controller.core_api.list_namespaced_pod.assert_called_once_with(
        "default",
        label_selector="ft.hygon.io/enabled=true,ft.hygon.io/job-name=train-job",
    )
    deleted = [
        call.args[:2]
        for call in controller.core_api.delete_namespaced_pod.call_args_list
    ]
    assert deleted == [
        ("train-master-0", "default"),
        ("train-worker-0", "default"),
    ]
    assert all(action["success"] is True for action in actions)


def test_restart_readiness_waits_for_processing():
    processed, ready, message = fault_event_restart_readiness(fault("pending", "root_cause", "2026-07-15T09:00:00Z"))

    assert processed is False
    assert ready is False
    assert "pending" in message


def test_restart_readiness_requires_successful_taint():
    event = fault("root", "root_cause", "2026-07-15T09:00:00Z")
    event["status"] = {
        "processed": True,
        "actions": [{"type": "TaintNode", "success": True, "nodeName": "node-a"}],
    }

    assert fault_event_restart_readiness(event)[:2] == (True, True)


def test_restart_readiness_rejects_failed_taint():
    event = fault("root", "root_cause", "2026-07-15T09:00:00Z")
    event["status"] = {
        "processed": True,
        "actions": [{"type": "TaintNode", "success": False, "message": "forbidden"}],
    }

    processed, ready, message = fault_event_restart_readiness(event)
    assert processed is True
    assert ready is False
    assert "failed" in message


def test_restart_readiness_rejects_implicit_delete_failure():
    event = fault("root", "root_cause", "2026-07-15T09:00:00Z")
    event["status"] = {
        "processed": True,
        "actions": [
            {"type": "TaintNode", "success": True, "nodeName": "node-a"},
            {"type": "DeletePod", "success": False, "message": "forbidden"},
        ],
    }

    processed, ready, message = fault_event_restart_readiness(event)
    assert processed is True
    assert ready is False
    assert "forbidden" in message


def test_restart_readiness_accepts_aggregated_event():
    event = fault("comm", "communication", "2026-07-15T09:00:00Z")
    event["status"] = {
        "processed": True,
        "actions": [{"type": "Suppressed", "success": True, "message": "aggregated under root"}],
    }

    assert fault_event_restart_readiness(event)[:2] == (True, True)


def test_reconcile_is_idempotent_for_processed_event():
    controller = bare_controller()
    controller._process_fault = mock.Mock()
    event = processing_fault()
    event["status"] = {"processed": True}

    controller.reconcile(event)

    controller._process_fault.assert_not_called()


def test_reconcile_duplicate_event_uses_one_timer_and_latest_payload(monkeypatch):
    controller = bare_controller()
    controller.aggregate_window_seconds = 5
    timer = mock.Mock()
    monkeypatch.setattr(
        "hygon_ft.operator.controller.threading.Timer", mock.Mock(return_value=timer)
    )
    first = fault("same", "communication", "2026-07-15T09:00:00Z")
    latest = fault("same", "root_cause", "2026-07-15T09:00:01Z")

    controller.reconcile(first)
    controller.reconcile(latest)

    assert len(controller._aggregation_timers) == 1
    assert controller._pending_faults["hygon-ft/train-job"]["same"] is latest
    timer.start.assert_called_once_with()


def test_taint_node_is_idempotent_when_owned_taint_already_exists():
    controller = bare_controller()
    controller.taint_key = "ft.hygon.io/node-unhealthy"
    controller.taint_effect = "NoSchedule"
    existing = SimpleNamespace(
        key=controller.taint_key,
        effect=controller.taint_effect,
        value="old-fault",
        time_added=None,
    )
    controller.core_api.read_node.return_value = SimpleNamespace(
        spec=SimpleNamespace(taints=[existing])
    )

    action = controller.taint_node("node-a", "new-fault")

    assert action["success"] is True
    controller.core_api.patch_node.assert_not_called()


def test_taint_node_api_failure_returns_failed_action_without_deleting():
    controller = bare_controller()
    controller.taint_key = "ft.hygon.io/node-unhealthy"
    controller.taint_effect = "NoSchedule"
    controller.core_api.read_node.side_effect = RuntimeError("api unavailable")

    action = controller.taint_node("node-a", "fault")

    assert action["success"] is False
    assert "api unavailable" in action["message"]
    controller.core_api.patch_node.assert_not_called()


def test_delete_named_pod_treats_not_found_as_idempotent_success():
    controller = bare_controller()
    controller.delete_grace_seconds = 0
    error = controller_module.ApiException("not found")
    error.status = 404
    controller.core_api.delete_namespaced_pod.side_effect = error

    action = controller.delete_named_pod("default", "worker-0", "node-a")

    assert action["success"] is True
    assert action["message"] == "pod already deleted"


def test_delete_named_pod_forbidden_is_reported_not_raised():
    controller = bare_controller()
    controller.delete_grace_seconds = 0
    error = controller_module.ApiException("forbidden")
    error.status = 403
    controller.core_api.delete_namespaced_pod.side_effect = error

    action = controller.delete_named_pod("default", "worker-0", "node-a")

    assert action["success"] is False
    assert "forbidden" in action["message"]


def test_list_pod_api_failure_does_not_attempt_broad_delete():
    controller = bare_controller()
    controller.core_api.list_pod_for_all_namespaces.side_effect = RuntimeError("timeout")

    actions = controller.delete_ft_pods_on_node("node-a")

    assert actions == [
        {"type": "ListPods", "nodeName": "node-a", "success": False, "message": "timeout"}
    ]
    controller.core_api.delete_namespaced_pod.assert_not_called()


def test_job_pod_listing_failure_does_not_attempt_delete():
    controller = bare_controller()
    controller.core_api.list_namespaced_pod.side_effect = RuntimeError("forbidden")

    actions = controller.delete_ft_job_pods("default", "train-job")

    assert actions[0]["success"] is False
    assert "list job pods failed" in actions[0]["message"]
    controller.core_api.delete_namespaced_pod.assert_not_called()


def test_node_not_ready_event_is_emitted_once_and_reset_after_recovery(monkeypatch):
    controller = bare_controller()
    controller.node_not_ready_grace_seconds = 10
    controller._node_not_ready_since = {"node-a": 100.0}
    controller._node_not_ready_event_sent = set()
    controller.create_node_not_ready_fault_event = mock.Mock()
    monkeypatch.setattr("hygon_ft.operator.controller.time.time", lambda: 111.0)
    unhealthy = SimpleNamespace(
        metadata=SimpleNamespace(name="node-a"),
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Ready", status="False", reason="KubeletDown")]
        ),
    )

    controller.handle_node_condition(unhealthy)
    controller.handle_node_condition(unhealthy)
    controller.create_node_not_ready_fault_event.assert_called_once_with(
        "node-a", "False", "KubeletDown", 11
    )

    healthy = SimpleNamespace(
        metadata=SimpleNamespace(name="node-a"),
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Ready", status="True", reason="Ready")]
        ),
    )
    controller.handle_node_condition(healthy)
    assert "node-a" not in controller._node_not_ready_since
    assert "node-a" not in controller._node_not_ready_event_sent


def test_existing_node_not_ready_fault_event_is_not_created_again():
    controller = bare_controller()
    controller._node_not_ready_since = {"node-a": 100.0}
    controller.custom_api.get_namespaced_custom_object.return_value = {"metadata": {"name": "exists"}}

    controller.create_node_not_ready_fault_event("node-a", "False", "KubeletDown", 30)

    controller.custom_api.create_namespaced_custom_object.assert_not_called()


def test_missing_fault_target_is_safely_marked_processed():
    controller = bare_controller()
    controller.patch_status = mock.Mock()
    controller.send_alert = mock.Mock()
    event = {"metadata": {"name": "invalid"}, "spec": {"type": "TrainingRootCause"}}

    controller._process_fault(event)

    actions = controller.patch_status.call_args.args[1]
    assert actions == [{"type": "Skip", "message": "spec.nodeName and spec.podName are both empty"}]
    controller.core_api.delete_namespaced_pod.assert_not_called()


def test_restart_detection_fails_safe_for_kubernetes_api_error():
    controller = bare_controller()
    error = controller_module.ApiException("server unavailable")
    error.status = 500
    controller.custom_api.get_namespaced_custom_object.side_effect = error

    assert controller.should_restart_entire_job("default", "train-job", "") is True


def test_restart_detection_does_not_treat_missing_pytorchjob_as_job_restart():
    controller = bare_controller()
    error = controller_module.ApiException("not found")
    error.status = 404
    controller.custom_api.get_namespaced_custom_object.side_effect = error

    assert controller.should_restart_entire_job("default", "train-job", "") is False


def test_delete_jobs_for_node_deduplicates_job_and_skips_terminal_pods():
    controller = bare_controller()
    controller.core_api.list_pod_for_all_namespaces.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(
                metadata=SimpleNamespace(namespace="default", labels={"ft.hygon.io/job-name": "train"}),
                status=SimpleNamespace(phase="Running"),
            ),
            SimpleNamespace(
                metadata=SimpleNamespace(namespace="default", labels={"ft.hygon.io/job-name": "train"}),
                status=SimpleNamespace(phase="Pending"),
            ),
            SimpleNamespace(
                metadata=SimpleNamespace(namespace="default", labels={"ft.hygon.io/job-name": "old"}),
                status=SimpleNamespace(phase="Succeeded"),
            ),
        ]
    )
    controller.delete_ft_job_pods = mock.Mock(return_value=[{"type": "DeletePod", "success": True}])

    actions = controller.delete_ft_jobs_for_node("node-a")

    controller.delete_ft_job_pods.assert_called_once_with("default", "train")
    assert actions == [{"type": "DeletePod", "success": True}]


def test_job_delete_continues_after_one_forbidden_pod_and_ignores_not_found():
    controller = bare_controller()
    controller.delete_grace_seconds = 0
    controller.core_api.list_namespaced_pod.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(metadata=SimpleNamespace(name="worker-0"), status=SimpleNamespace(phase="Running")),
            SimpleNamespace(metadata=SimpleNamespace(name="worker-1"), status=SimpleNamespace(phase="Running")),
            SimpleNamespace(metadata=SimpleNamespace(name="worker-2"), status=SimpleNamespace(phase="Running")),
        ]
    )
    forbidden = controller_module.ApiException("forbidden")
    forbidden.status = 403
    missing = controller_module.ApiException("not found")
    missing.status = 404
    controller.core_api.delete_namespaced_pod.side_effect = [forbidden, missing, None]

    actions = controller.delete_ft_job_pods("default", "train")

    assert controller.core_api.delete_namespaced_pod.call_count == 3
    assert any(action["success"] is False and action["podName"] == "worker-0" for action in actions)
    assert any(action["success"] is True and action["podName"] == "worker-2" for action in actions)
    assert not any(action.get("podName") == "worker-1" for action in actions)
