# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import base64
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hygon_ft.webhook.server import (
    build_admission_response,
    build_patch,
    mutate_pytorchjob,
    mutate_volcano_job,
)


LAUNCHER = "/shared/hcu_cluster_manager/hygon-ft-k8s/runtime/ft-launcher"


def volcano_job():
    def task(name, replicas=1):
        return {
            "name": name,
            "replicas": replicas,
            "template": {
                "metadata": {"labels": {"app": "training"}},
                "spec": {
                    "containers": [
                        {
                            "name": "trainer",
                            "image": "training:test",
                            "command": ["bash", "-lc"],
                            "args": [f"echo {name}"],
                        }
                    ]
                },
            },
        }

    return {
        "apiVersion": "batch.volcano.sh/v1alpha1",
        "kind": "Job",
        "metadata": {
            "name": "training-job",
            "labels": {"ft.hygon.io/enabled": "true"},
            "annotations": {
                "ft.hygon.io/enabled": "true",
                "ft.hygon.io/launcher-path": LAUNCHER,
                "ft.hygon.io/launcher-interpreter": "/bin/bash",
                "ft.hygon.io/log-dir": "/shared/workspace/hygon-ft",
                "ft.hygon.io/log-monitor-roles": "all",
                "ft.hygon.io/log-monitor-command": "log-monitor --log-file ${FT_LOG_FILE}",
            },
        },
        "spec": {"tasks": [task("master"), task("worker", replicas=3)]},
    }


def env_map(container):
    return {item["name"]: item for item in container.get("env", [])}


def test_pytorchjob_injects_workload_kind():
    job = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "PyTorchJob",
        "metadata": {
            "name": "training-job",
            "annotations": {
                "ft.hygon.io/enabled": "true",
                "ft.hygon.io/launcher-path": LAUNCHER,
            },
        },
        "spec": {
            "pytorchReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "pytorch",
                                    "image": "training:test",
                                    "command": ["bash", "-lc"],
                                    "args": ["echo master"],
                                }
                            ]
                        }
                    },
                },
                "Worker": {
                    "replicas": 2,
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "pytorch",
                                    "image": "training:test",
                                    "command": ["bash", "-lc"],
                                    "args": ["echo worker"],
                                }
                            ]
                        }
                    },
                },
            }
        },
    }

    mutated = mutate_pytorchjob(job)

    for replica in mutated["spec"]["pytorchReplicaSpecs"].values():
        container = replica["template"]["spec"]["containers"][0]
        assert env_map(container)["FT_WORKLOAD_KIND"]["value"] == "PyTorchJob"


def test_mutate_volcano_job_injects_each_task():
    original = volcano_job()
    mutated = mutate_volcano_job(original)

    assert "ft.hygon.io/injected" not in original["metadata"]["annotations"]
    assert mutated["metadata"]["annotations"]["ft.hygon.io/injected"] == "true"

    master, worker = mutated["spec"]["tasks"]
    for role, task in (("master", master), ("worker", worker)):
        template = task["template"]
        container = template["spec"]["containers"][0]
        env = env_map(container)
        assert template["metadata"]["labels"]["ft.hygon.io/enabled"] == "true"
        assert template["metadata"]["labels"]["ft.hygon.io/replica-role"] == role
        assert container["command"] == ["/bin/bash", LAUNCHER]
        assert container["args"] == ["--", "bash", "-lc", f"echo {role}"]
        assert env["FT_LOG_DIR"]["value"] == "/shared/workspace/hygon-ft"
        assert env["FT_WORKLOAD_KIND"]["value"] == "VolcanoJob"
        assert env["FT_POD_UID"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid"
        assert "FT_LOG_FILE" not in env
        assert "initContainers" not in template["spec"]

    assert env_map(master["template"]["spec"]["containers"][0])["FT_EXTERNAL_MONITOR_CMD"]
    master_env = env_map(master["template"]["spec"]["containers"][0])
    assert master_env["LOG_MONITOR_ENABLE_NO_UPDATE"]["value"] == "false"
    assert master_env["LOG_MONITOR_LAST_NODE_ONLY"]["value"] == "true"
    assert master_env["FT_REPLICA_COUNT"]["value"] == "1"
    worker_env = env_map(worker["template"]["spec"]["containers"][0])
    assert worker_env["FT_EXTERNAL_MONITOR_CMD"]
    assert worker_env["LOG_MONITOR_ENABLE_NO_UPDATE"]["value"] == "true"
    assert worker_env["LOG_MONITOR_LAST_NODE_ONLY"]["value"] == "true"
    assert worker_env["FT_REPLICA_COUNT"]["value"] == "3"
    assert worker_env["ENABLE_REGULAR_NOTIFY"]["value"] == "false"


def test_volcano_patch_contains_annotations_and_tasks():
    original = volcano_job()
    patch = build_patch(original, mutate_volcano_job(original))
    assert [operation["path"] for operation in patch] == ["/metadata/annotations", "/spec/tasks"]


def test_volcano_mutation_is_idempotent():
    once = mutate_volcano_job(volcano_job())
    assert mutate_volcano_job(once) == once


def test_admission_endpoint_returns_volcano_json_patch():
    review = {
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "request": {"uid": "test-uid", "object": volcano_job()},
    }
    body = build_admission_response(review)["response"]
    assert body["allowed"] is True
    assert body["patchType"] == "JSONPatch"
    patch = json.loads(base64.b64decode(body["patch"]))
    assert any(operation["path"] == "/spec/tasks" for operation in patch)


def test_disabled_workload_is_allowed_without_mutation():
    job = volcano_job()
    job["metadata"]["annotations"]["ft.hygon.io/enabled"] = "false"
    job["metadata"]["labels"]["ft.hygon.io/enabled"] = "false"
    review = {
        "apiVersion": "admission.k8s.io/v1",
        "request": {"uid": "disabled", "object": job},
    }

    response = build_admission_response(review)["response"]

    assert response == {"uid": "disabled", "allowed": True}


def test_already_injected_workload_is_allowed_without_second_patch():
    injected = mutate_volcano_job(volcano_job())
    review = {
        "apiVersion": "admission.k8s.io/v1",
        "request": {"uid": "again", "object": injected},
    }

    response = build_admission_response(review)["response"]

    assert response == {"uid": "again", "allowed": True}


def test_malformed_replica_count_is_explicitly_denied():
    job = volcano_job()
    job["spec"]["tasks"][0]["replicas"] = "not-an-integer"
    review = {
        "apiVersion": "admission.k8s.io/v1",
        "request": {"uid": "bad", "object": job},
    }

    response = build_admission_response(review)["response"]

    assert response["allowed"] is False
    assert response["uid"] == "bad"
    assert "mutation failed" in response["status"]["message"]
    assert "patch" not in response


def test_unknown_resource_is_allowed_without_patch():
    review = {
        "apiVersion": "admission.k8s.io/v1",
        "request": {
            "uid": "unknown",
            "object": {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {}},
        },
    }
    assert build_admission_response(review)["response"] == {
        "uid": "unknown",
        "allowed": True,
    }
