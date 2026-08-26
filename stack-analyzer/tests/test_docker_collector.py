# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

from stack_analyzer.docker_collector import DockerSSHConfig, DockerSSHStackCollector


def completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_docker_collector_ssh_same_port():
    payload = json.dumps(
        [
            {
                "machine_id": "container0",
                "rank": 0,
                "pid": 7,
                "role": "trainer",
                "frames": [],
                "raw_text": "",
            }
        ]
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        hostfile = Path(tmp_dir) / "docker-hostfile"
        hostfile.write_text(
            "container0 slots=4 ansible_host=10.0.0.10\n"
            "container1 slots=4 ansible_host=10.0.0.11\n",
            encoding="utf-8",
        )
        config = DockerSSHConfig(ssh_user="root", ssh_port=2222, parallelism=1)
        collector = DockerSSHStackCollector(config)
        with patch("shutil.which", return_value="ssh"), patch(
            "subprocess.run", return_value=completed(payload)
        ) as run:
            snapshots = collector.collect(hostfile)

    assert len(snapshots) == 2
    first_cmd = run.call_args_list[0].args[0]
    assert first_cmd[:3] == ["ssh", "-p", "2222"]
    assert "root@10.0.0.10" in first_cmd
    assert "--rank-start" in first_cmd
    assert "--nonblocking" in first_cmd
    assert "--ranked-only" in first_cmd


def test_docker_collector_uses_hostfile_user():
    payload = json.dumps(
        [
            {
                "machine_id": "container0",
                "rank": 0,
                "pid": 7,
                "role": "trainer",
                "frames": [],
                "raw_text": "",
            }
        ]
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        hostfile = Path(tmp_dir) / "docker-hostfile"
        hostfile.write_text(
            "container0 slots=1 ansible_user=example-user\n", encoding="utf-8"
        )
        collector = DockerSSHStackCollector(DockerSSHConfig(parallelism=1))
        with patch("shutil.which", return_value="ssh"), patch(
            "subprocess.run", return_value=completed(payload)
        ) as run:
            collector.collect(hostfile)

    cmd = run.call_args.args[0]
    assert "example-user@container0" in cmd
