# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
from unittest.mock import patch

from stack_analyzer.kubernetes_collector import KubernetesConfig, KubernetesStackCollector


def completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    import subprocess
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_kubernetes_collector_selects_running_pods():
    pods = {
        "items": [
            {
                "metadata": {"name": "trainer-0"},
                "status": {"phase": "Running"},
                "spec": {"containers": [{"name": "trainer"}]},
            },
            {
                "metadata": {"name": "trainer-1"},
                "status": {"phase": "Pending"},
                "spec": {"containers": [{"name": "trainer"}]},
            },
        ]
    }
    payload = json.dumps([
        {"machine_id": "trainer-0/trainer", "rank": 0, "pid": 7,
         "role": "trainer", "frames": [], "raw_text": ""}
    ])
    config = KubernetesConfig(selector="job-name=train", parallelism=1)
    collector = KubernetesStackCollector(config)
    with patch("shutil.which", return_value="kubectl"), patch.object(
        collector, "_kubectl", side_effect=[completed(json.dumps(pods)), completed(payload)]
    ) as kubectl:
        snapshots = collector.collect()

    assert len(snapshots) == 1
    assert snapshots[0].rank == 0
    assert kubectl.call_args_list[0].args[:4] == ("get", "pods", "-n", "default")
    assert "job-name=train" in kubectl.call_args_list[0].args
    assert "--nonblocking" in kubectl.call_args_list[1].args
    assert "--ranked-only" in kubectl.call_args_list[1].args


def test_kubernetes_collector_container_filter():
    config = KubernetesConfig(container="trainer")
    collector = KubernetesStackCollector(config)
    pods = {"items": [{
        "metadata": {"name": "pod-0"}, "status": {"phase": "Running"},
        "spec": {"containers": [{"name": "sidecar"}, {"name": "trainer"}]},
    }]}
    with patch.object(collector, "_kubectl", return_value=completed(json.dumps(pods))):
        assert collector._targets() == [("pod-0", "trainer")]
