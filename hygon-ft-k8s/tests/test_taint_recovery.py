# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace
from unittest import mock

from hygon_ft.nodehealth import taint_recovery
from hygon_ft.nodehealth.taint_recovery import (
    DEFAULT_SCRIPT_PATH,
    RecoveryConfig,
    parse_nhc_recovery_result,
)


def node(name, ready=True, taints=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Ready", status="True" if ready else "False")]
        ),
        spec=SimpleNamespace(taints=taints or []),
    )


def test_recovery_uses_configmap_adapter_by_default():
    assert DEFAULT_SCRIPT_PATH == "/opt/hygon-ft/nhc/run_nhc.sh"
    assert RecoveryConfig().script_path == DEFAULT_SCRIPT_PATH


def test_recovery_accepts_successful_host_run_nhc_result():
    result = parse_nhc_recovery_result(
        "[nhc-adapter] backend=host:/usr/local/bin/run_nhc command_exit=0\n"
        "[CHECK RESULT]: PASSED\n",
        "Succeeded",
    )

    assert result == {"result": "PASS", "nhc_result": "PASSED"}


def test_recovery_rejects_reported_node_failure():
    result = parse_nhc_recovery_result("[CHECK RESULT]: disk_failed\n", "Failed")

    assert result["result"] == "FAIL"
    assert result["failed_items"] == ["run_nhc"]


def test_recovery_rejects_missing_result_marker():
    result = parse_nhc_recovery_result("run_nhc: command not found\n", "Failed")

    assert result["result"] == "FAIL"
    assert result["failed_items"] == ["checker_output"]


def test_choose_normal_nodes_excludes_target_unready_and_tainted(monkeypatch):
    owned = SimpleNamespace(key="ft.hygon.io/node-unhealthy", effect="NoSchedule")
    api = mock.Mock()
    api.list_node.return_value.items = [
        node("target"), node("ready-a"), node("unready", ready=False),
        node("tainted", taints=[owned]), node("ready-b"),
    ]
    monkeypatch.setattr(taint_recovery.random, "shuffle", lambda values: None)
    assert taint_recovery.choose_normal_nodes(
        api, "target", "ft.hygon.io/node-unhealthy", "NoSchedule", 2
    ) == ["ready-a", "ready-b"]


def test_remove_owned_taint_preserves_foreign_taints_and_is_idempotent(monkeypatch):
    owned = SimpleNamespace(
        key="ft.hygon.io/node-unhealthy", effect="NoSchedule", value="fault", time_added=None
    )
    foreign = SimpleNamespace(key="dedicated", effect="NoExecute", value="train", time_added=None)
    api = mock.Mock()
    api.read_node.side_effect = [node("node-a", taints=[owned, foreign]), node("node-a", taints=[foreign])]
    cfg = RecoveryConfig(remove_taint=True)

    first = taint_recovery.remove_owned_taint(api, "node-a", cfg)
    second = taint_recovery.remove_owned_taint(api, "node-a", cfg)

    assert first["removed"] is True
    assert second["removed"] is False
    api.patch_node.assert_called_once_with(
        "node-a", {"spec": {"taints": [{"key": "dedicated", "effect": "NoExecute", "value": "train"}]}}
    )


def test_run_recovery_failure_still_cleans_up_check_pod(monkeypatch):
    api = mock.Mock()
    api.create_namespaced_pod.return_value = None
    monkeypatch.setattr(taint_recovery, "build_check_pod", lambda *args: object())
    monkeypatch.setattr(taint_recovery, "build_recovery_pod_name", lambda *args: "check-pod")
    monkeypatch.setattr(
        taint_recovery, "wait_for_pod_phase", mock.Mock(side_effect=TimeoutError("pod timeout"))
    )
    monkeypatch.setattr(
        taint_recovery,
        "client",
        SimpleNamespace(V1DeleteOptions=lambda **kwargs: kwargs),
    )

    result = taint_recovery.run_pod_recovery_checks(
        api, "node-a", ["node-b", "node-c"], RecoveryConfig(), 1
    )

    assert result["result"] == "FAIL"
    assert result["failed_items"] == ["checker_pod"]
    assert "pod timeout" in result["detail"]
    api.delete_namespaced_pod.assert_called_once()


def test_check_recovery_fails_closed_when_kubernetes_is_unavailable(monkeypatch):
    monkeypatch.setattr(taint_recovery, "load_kube", mock.Mock(side_effect=RuntimeError("no api")))
    result = taint_recovery.check_taint_recovery("node-a")
    assert result["result"] == "FAIL"
    assert result["failed_items"] == ["kubernetes"]


def test_check_recovery_requires_two_safe_reference_nodes(monkeypatch):
    api = mock.Mock()
    api.read_node.return_value = node("node-a")
    monkeypatch.setattr(taint_recovery, "load_kube", mock.Mock())
    monkeypatch.setattr(taint_recovery, "client", SimpleNamespace(CoreV1Api=lambda: api))
    monkeypatch.setattr(taint_recovery, "choose_normal_nodes", lambda *args: ["node-b"])

    result = taint_recovery.check_taint_recovery("node-a")

    assert result["result"] == "FAIL"
    assert result["failed_items"] == ["ib_write_bw"]


def test_check_recovery_stops_on_first_failed_round_without_untaint(monkeypatch):
    api = mock.Mock()
    api.read_node.return_value = node("node-a")
    monkeypatch.setattr(taint_recovery, "load_kube", mock.Mock())
    monkeypatch.setattr(taint_recovery, "client", SimpleNamespace(CoreV1Api=lambda: api))
    monkeypatch.setattr(taint_recovery, "init_detail_log", mock.Mock())
    check = mock.Mock(return_value={"result": "FAIL", "failed_items": ["run_nhc"]})
    monkeypatch.setattr(taint_recovery, "run_pod_recovery_checks", check)
    untaint = mock.Mock()
    monkeypatch.setattr(taint_recovery, "remove_owned_taint", untaint)

    result = taint_recovery.check_taint_recovery(
        "node-a", ["node-b", "node-c"], RecoveryConfig(check_times=3, remove_taint=True)
    )

    assert result["result"] == "FAIL"
    check.assert_called_once()
    untaint.assert_not_called()


def test_pod_logs_fall_back_to_previous_container_instance(monkeypatch):
    class FakeApiError(Exception):
        pass

    api = mock.Mock()
    api.read_namespaced_pod_log.side_effect = [FakeApiError("current missing"), "previous logs"]
    monkeypatch.setattr(taint_recovery, "ApiException", FakeApiError)

    assert taint_recovery.get_pod_logs(api, "hygon-ft", "checker") == "previous logs"
    assert [call.kwargs["previous"] for call in api.read_namespaced_pod_log.call_args_list] == [False, True]


def test_pod_log_api_failures_are_returned_as_diagnostics(monkeypatch):
    class FakeApiError(Exception):
        pass

    api = mock.Mock()
    api.read_namespaced_pod_log.side_effect = FakeApiError("forbidden")
    monkeypatch.setattr(taint_recovery, "ApiException", FakeApiError)

    logs = taint_recovery.get_pod_logs(api, "hygon-ft", "checker")

    assert "failed to read pod logs" in logs
    assert "previous=False" in logs and "previous=True" in logs


def test_run_recovery_success_records_phase_and_deletes_pod(monkeypatch):
    api = mock.Mock()
    finished = SimpleNamespace(status=SimpleNamespace(phase="Succeeded"))
    waits = mock.Mock(side_effect=[SimpleNamespace(status=SimpleNamespace(phase="Running")), finished])
    monkeypatch.setattr(taint_recovery, "build_check_pod", lambda *args: object())
    monkeypatch.setattr(taint_recovery, "build_recovery_pod_name", lambda *args: "check-pod")
    monkeypatch.setattr(taint_recovery, "wait_for_pod_phase", waits)
    monkeypatch.setattr(taint_recovery, "get_pod_logs", lambda *args: "[CHECK RESULT]: PASSED\n")
    monkeypatch.setattr(taint_recovery, "append_detail_log_message", mock.Mock())
    monkeypatch.setattr(
        taint_recovery, "client", SimpleNamespace(V1DeleteOptions=lambda **kwargs: kwargs)
    )

    result = taint_recovery.run_pod_recovery_checks(
        api, "node-a", ["node-b", "node-c"], RecoveryConfig(), 2
    )

    assert result["result"] == "PASS"
    assert result["pod_phase"] == "Succeeded"
    assert result["round"] == 2
    assert result["normal_nodes"] == ["node-b", "node-c"]
    api.delete_namespaced_pod.assert_called_once()


def test_check_recovery_untaints_only_after_every_round_passes(monkeypatch):
    api = mock.Mock()
    api.read_node.return_value = node("node-a")
    monkeypatch.setattr(taint_recovery, "load_kube", mock.Mock())
    monkeypatch.setattr(taint_recovery, "client", SimpleNamespace(CoreV1Api=lambda: api))
    monkeypatch.setattr(taint_recovery, "init_detail_log", mock.Mock())
    monkeypatch.setattr(taint_recovery, "append_detail_log_message", mock.Mock())
    check = mock.Mock(side_effect=[{"result": "PASS", "round": 1}, {"result": "PASS", "round": 2}])
    monkeypatch.setattr(taint_recovery, "run_pod_recovery_checks", check)
    untaint = mock.Mock(return_value={"removed": True})
    monkeypatch.setattr(taint_recovery, "remove_owned_taint", untaint)
    monkeypatch.setattr(taint_recovery.time, "sleep", mock.Mock())
    cfg = RecoveryConfig(
        check_times=2,
        check_interval_seconds=1,
        remove_taint=True,
        detail_log_file="/tmp/detail.log",
    )

    result = taint_recovery.check_taint_recovery("node-a", ["node-b", "node-c"], cfg)

    assert result["result"] == "PASS"
    assert result["rounds"] == 2
    assert result["untaint"] == {"removed": True}
    assert check.call_count == 2
    untaint.assert_called_once_with(api, "node-a", cfg)


def test_unready_target_never_launches_recovery_pod(monkeypatch):
    api = mock.Mock()
    api.read_node.return_value = node("node-a", ready=False)
    monkeypatch.setattr(taint_recovery, "load_kube", mock.Mock())
    monkeypatch.setattr(taint_recovery, "client", SimpleNamespace(CoreV1Api=lambda: api))
    run_check = mock.Mock()
    monkeypatch.setattr(taint_recovery, "run_pod_recovery_checks", run_check)

    result = taint_recovery.check_taint_recovery("node-a", ["node-b", "node-c"])

    assert result["failed_items"] == ["Node Ready"]
    run_check.assert_not_called()
