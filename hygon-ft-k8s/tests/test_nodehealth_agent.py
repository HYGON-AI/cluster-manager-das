# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import sys
from types import SimpleNamespace
from unittest import mock

import pytest


class ApiException(Exception):
    pass


kubernetes = mock.Mock()
kubernetes.client = mock.Mock()
kubernetes.config = mock.Mock()
kubernetes.watch = mock.Mock()
kubernetes.client.ApiException = ApiException
sys.modules.setdefault("kubernetes", kubernetes)
sys.modules.setdefault("kubernetes.client", kubernetes.client)

from hygon_ft.nodehealth import agent


def test_trim_keeps_tail_and_timestamp_is_utc():
    assert agent.trim("abcdef", 4) == "cdef"
    assert agent.trim("abc", 4) == "abc"
    assert agent.now_iso().endswith("Z")


def test_load_kube_falls_back_to_local_config():
    class ConfigException(Exception):
        pass

    fake_config = mock.Mock()
    fake_config.ConfigException = ConfigException
    fake_config.load_incluster_config.side_effect = ConfigException()

    with mock.patch.object(agent, "config", fake_config):
        agent.load_kube()

    fake_config.load_incluster_config.assert_called_once_with()
    fake_config.load_kube_config.assert_called_once_with()


def test_create_fault_event_builds_expected_contract(monkeypatch):
    api = mock.Mock()
    monkeypatch.setenv("FT_TAINT_NODE_ON_NHC_FAIL", "true")
    monkeypatch.setenv("FT_DELETE_PODS_ON_NHC_FAIL", "false")
    monkeypatch.setattr(agent.time, "time", lambda: 1234)

    agent.create_fault_event(
        api,
        namespace="training",
        node_name="NODE_01",
        reason="nhc_failed",
        stdout="probe output",
        stderr="probe error",
        exit_code=2,
        observed_at="2026-01-01T00:00:00.000Z",
    )

    args = api.create_namespaced_custom_object.call_args.args
    assert args[:4] == (agent.GROUP, agent.VERSION, "training", agent.PLURAL)
    body = args[4]
    assert body["metadata"]["name"] == "nhc-node-01-1234"
    assert body["spec"]["nodeName"] == "NODE_01"
    assert body["spec"]["observedAt"] == "2026-01-01T00:00:00.000Z"
    assert body["spec"]["action"] == {"taintNode": True, "deletePods": False}
    assert "exit=2" in body["spec"]["message"]


def run_one_agent_iteration(monkeypatch, result=None, error=None, **env):
    custom_api = mock.Mock()
    monkeypatch.setattr(agent, "load_kube", mock.Mock())
    monkeypatch.setattr(agent.client, "CustomObjectsApi", lambda: custom_api)
    monkeypatch.setattr(agent, "now_iso", lambda: "2026-08-13T00:00:00.000Z")
    monkeypatch.setattr(agent.time, "time", lambda: 1000.0)
    monkeypatch.setattr(agent.time, "sleep", mock.Mock(side_effect=StopIteration))
    monkeypatch.setenv("NODE_NAME", "node-a")
    monkeypatch.setenv("NHC_FAILURE_SUPPRESS_SECONDS", "0")
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))
    runner = mock.Mock(return_value=result, side_effect=error)
    monkeypatch.setattr(agent.subprocess, "run", runner)
    with pytest.raises(StopIteration):
        agent.main()
    return custom_api


def test_agent_creates_fault_only_for_actionable_nhc_failure(monkeypatch):
    api = run_one_agent_iteration(
        monkeypatch,
        SimpleNamespace(returncode=2, stdout="disk failed", stderr=""),
    )
    body = api.create_namespaced_custom_object.call_args.args[-1]
    assert body["spec"]["reason"] == "run_nhc_failed"
    assert body["spec"]["observedAt"] == "2026-08-13T00:00:00.000Z"


@pytest.mark.parametrize(
    "result",
    [
        SimpleNamespace(returncode=0, stdout="healthy", stderr=""),
        SimpleNamespace(returncode=3, stdout="[NHC REPORT ONLY] warning", stderr=""),
        SimpleNamespace(returncode=1, stdout="probe setup failed", stderr=""),
    ],
)
def test_agent_does_not_create_fault_for_non_actionable_results(monkeypatch, result):
    api = run_one_agent_iteration(monkeypatch, result)
    api.create_namespaced_custom_object.assert_not_called()


def test_agent_timeout_is_safe_by_default(monkeypatch):
    timeout = agent.subprocess.TimeoutExpired("run_nhc", 5, output="partial", stderr="slow")
    api = run_one_agent_iteration(monkeypatch, error=timeout)
    api.create_namespaced_custom_object.assert_not_called()


def test_agent_can_promote_timeout_to_fault(monkeypatch):
    timeout = agent.subprocess.TimeoutExpired("run_nhc", 5, output="partial", stderr="slow")
    api = run_one_agent_iteration(monkeypatch, error=timeout, NHC_TIMEOUT_IS_FAILURE="true")
    body = api.create_namespaced_custom_object.call_args.args[-1]
    assert body["spec"]["reason"] == "run_nhc_timeout"
    assert "exit=124" in body["spec"]["message"]


def test_agent_kubernetes_api_failure_does_not_escape_loop(monkeypatch):
    api = run_one_agent_iteration(monkeypatch, error=ApiException("forbidden"))
    api.create_namespaced_custom_object.assert_not_called()
