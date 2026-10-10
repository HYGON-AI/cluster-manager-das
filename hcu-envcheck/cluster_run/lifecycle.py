# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Explicit, host-side Docker lifecycle operations for a node set.

Image acquisition is a separate all-node phase.  A failed pull/load never
removes an existing container on any node.  This module does not run env.sh.
"""

from __future__ import annotations

import re
import shlex
import tempfile
from pathlib import Path
from typing import Sequence

from hcu_envcheck.baremetal import BaremetalClusterExecutor, BaremetalExecutionConfig
from . import container_ssh


_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")


def _execute(nodes: Sequence[str], command: list[str], *, transport: str, concurrency: int, run_dir: Path, name: str) -> dict:
    execution = BaremetalClusterExecutor(
        nodes,
        BaremetalExecutionConfig(output_root=run_dir / name, transport=transport,
                                 concurrency=concurrency, command_timeout_seconds=600),
    ).execute(name, command)
    return {node: execution.nodes.get(node) for node in nodes}


def _failed(results: dict) -> list[dict[str, str]]:
    issues = []
    for node, result in results.items():
        if result is None or not result.success:
            detail = (result.stderr if result else "no remote result") or (result.error_kind if result else "no remote result")
            issues.append({"node": node, "code": "REMOTE_COMMAND_FAILED", "message": str(detail).strip()[:500]})
    return issues


def run_container_lifecycle(
    *, operation: str, nodes: Sequence[str], container_name: str,
    image: str | None, image_tar: str | None, volumes: Sequence[str],
    docker_args: Sequence[str], container_command: str | None,
    transport: str, concurrency: int, confirm: bool, dry_run: bool,
    port: int | None = None, log=None,
) -> dict:
    """Create, recreate or delete the exact named container on all nodes.

    ``--yes`` is required for operations that remove an existing container.
    Creation never adopts or overwrites an existing container.
    """
    if operation not in {"container-create", "container-recreate", "container-delete"}:
        raise ValueError(f"unsupported container operation: {operation}")
    if not _NAME.fullmatch(container_name):
        raise ValueError("--container must be one safe Docker name")
    if operation != "container-delete" and not image:
        raise ValueError(f"{operation} requires -i/--image")
    if operation in {"container-recreate", "container-delete"} and not confirm and not dry_run:
        raise ValueError(f"{operation} is destructive; pass --yes explicitly")
    if image_tar and (not image or not image_tar.startswith("/")):
        raise ValueError("--image-tar requires --image and an absolute path visible on every node")
    if operation == "container-delete" and (image or image_tar or volumes or docker_args or container_command):
        raise ValueError("container-delete accepts only --container, --yes and common node/transport options")
    for volume in volumes:
        if not volume or "\x00" in volume:
            raise ValueError("invalid --volume")
    for arg in docker_args:
        if not arg or "\x00" in arg:
            raise ValueError("invalid --docker-arg")
    if any("\x00" in value for value in (image or "", container_command or "")):
        raise ValueError("NUL is not allowed in Docker arguments")
    if port is not None:
        if operation == "container-delete":
            raise ValueError("--port is only valid for container-create/recreate")
        docker_args = container_ssh.validate_options(port, nodes, volumes, docker_args)

    if dry_run:
        return {"status": "DRY_RUN", "operation": operation, "nodes": list(nodes), "issues": [], "ssh_port": port}

    with tempfile.TemporaryDirectory(prefix="hcu-container-lifecycle-") as temp:
        root = Path(temp)
        # Inspect first.  Missing containers are expected for create, but not
        # for delete.  Transport failures are never treated as absence.
        inspection = _execute(nodes, ["bash", "-lc", f"docker info >/dev/null 2>&1 || exit 125; docker inspect --type container {shlex.quote(container_name)} >/dev/null 2>&1; rc=$?; "
                                      "if [ \"$rc\" = 0 ]; then echo PRESENT; "
                                      "elif [ \"$rc\" = 1 ]; then echo ABSENT; else exit \"$rc\"; fi"],
                              transport=transport, concurrency=concurrency, run_dir=root, name="inspect")
        issues = _failed(inspection)
        # Login shells on some clusters print a MOTD before the command's
        # output.  The final non-empty line is our explicit inspect marker.
        marker = {node: (result.stdout.strip().splitlines()[-1].strip()
                         if result and result.success and result.stdout.strip() else "")
                  for node, result in inspection.items()}
        present = {node for node in nodes if marker[node] == "PRESENT"}
        absent = {node for node in nodes if marker[node] == "ABSENT"}
        unknown = set(nodes) - present - absent - {issue["node"] for issue in issues}
        issues += [{"node": node, "code": "CONTAINER_INSPECT_INVALID", "message": "unexpected Docker inspect output"} for node in unknown]
        if operation == "container-create":
            issues += [{"node": node, "code": "CONTAINER_ALREADY_EXISTS", "message": "container exists; use container-recreate --yes explicitly"} for node in present]
        if operation == "container-delete":
            issues += [{"node": node, "code": "CONTAINER_MISSING", "message": "container does not exist"} for node in absent]
        if issues:
            return {"status": "FAIL", "operation": operation, "nodes": list(nodes), "issues": issues}

        if operation != "container-delete":
            qimage = shlex.quote(str(image))
            if image_tar:
                # An explicit tar is authoritative, even when a stale tag is
                # already present locally on some nodes.
                acquisition = (f"test -f {shlex.quote(image_tar)} && "
                               f"docker load -i {shlex.quote(image_tar)} >/dev/null && "
                               f"docker image inspect {qimage} >/dev/null 2>&1")
            else:
                acquisition = f"docker image inspect {qimage} >/dev/null 2>&1 || docker pull {qimage} >/dev/null"
            acquired = _execute(nodes, ["bash", "-lc", acquisition], transport=transport,
                                concurrency=concurrency, run_dir=root, name="acquire-image")
            issues = _failed(acquired)
            if issues:
                for issue in issues:
                    issue["code"] = "IMAGE_ACQUISITION_FAILED"
                    issue["message"] += "; if pull failed on offline nodes, supply --image-tar /shared/image.tar"
                return {"status": "FAIL", "operation": operation, "nodes": list(nodes), "issues": issues}

            identities = _execute(
                nodes,
                ["docker", "image", "inspect", "--format", "{{.Id}}", str(image)],
                transport=transport, concurrency=concurrency,
                run_dir=root, name="verify-image-identity",
            )
            issues = _failed(identities)
            image_ids = {
                node: result.stdout.strip().splitlines()[-1].strip()
                for node, result in identities.items()
                if result and result.success and result.stdout.strip()
            }
            for node in nodes:
                if node not in image_ids or not image_ids[node].startswith("sha256:"):
                    issues.append({"node": node, "code": "IMAGE_ID_MISSING",
                                   "message": "cannot determine immutable image ID before container change"})
            if len(set(image_ids.values())) > 1:
                issues.append({"node": "cluster", "code": "IMAGE_ID_INCONSISTENT",
                               "message": "same image tag resolves to different IDs; align images before changing containers"})
            if issues:
                return {"status": "FAIL", "operation": operation, "nodes": list(nodes), "issues": issues}
            expected_image_id = next(iter(image_ids.values()))

        if port is not None:
            return _create_with_ssh(
                operation=operation, nodes=nodes, container_name=container_name,
                image=str(image), expected_image_id=expected_image_id, volumes=volumes,
                docker_args=docker_args, container_command=container_command,
                port=port, transport=transport, concurrency=concurrency, root=root, log=log,
            )

        run = ["docker", "run", "-dit", "--name", container_name]
        for volume in volumes:
            run.extend(["-v", volume])
        run.extend(docker_args)
        if image:
            run.append(image)
        if container_command:
            run.extend(shlex.split(container_command))
        run_command = shlex.join(run) + " >/dev/null"
        if operation == "container-create":
            body = run_command
        elif operation == "container-recreate":
            body = f"docker rm -f {shlex.quote(container_name)} >/dev/null 2>&1 || true; {run_command}"
        else:
            body = f"docker rm -f {shlex.quote(container_name)} >/dev/null"
        changed = _execute(nodes, ["bash", "-lc", body], transport=transport,
                           concurrency=concurrency, run_dir=root, name="change-container")
        issues = _failed(changed)
        if not issues:
            if operation == "container-delete":
                verification = f"! docker inspect --type container {shlex.quote(container_name)} >/dev/null 2>&1"
            else:
                expected = shlex.quote(f"true|{image}|{expected_image_id}")
                verification = (f"test \"$(docker inspect --type container -f '{{{{.State.Running}}}}|{{{{.Config.Image}}}}|{{{{.Image}}}}' "
                                f"{shlex.quote(container_name)})\" = {expected}")
            verified = _execute(nodes, ["bash", "-lc", verification], transport=transport,
                                concurrency=concurrency, run_dir=root, name="verify-container")
            issues = _failed(verified)
            for issue in issues:
                issue["code"] = "CONTAINER_VERIFY_FAILED"
        return {"status": "PASS" if not issues else "FAIL", "operation": operation,
                "nodes": list(nodes), "issues": issues}


def _create_with_ssh(*, operation, nodes, container_name, image, expected_image_id,
                     volumes, docker_args, container_command, port, transport, concurrency, root, log):
    """All-node validation, creation, public-key exchange, actual SSH verification.

    A post-create failure is NOT rolled back by deleting containers. Return the
    precise phase so an operator can inspect/fix partial changes explicitly.
    """
    report = {"status": "FAIL", "operation": operation, "nodes": list(nodes),
              "issues": [], "ssh_port": port, "ssh_user": "root", "containers_changed": False}

    def phase(name, command, code):
        if log:
            log("INFO", f"container SSH phase={name} nodes={len(nodes)} port={port}")
        results = _execute(nodes, command, transport=transport, concurrency=concurrency,
                           run_dir=root, name=name)
        issues = _failed(results)
        for issue in issues:
            issue["code"] = code
        report["issues"].extend(issues)
        return results

    phase("ssh-port-check", container_ssh.port_check(container_name, port), "CONTAINER_SSH_PORT_UNAVAILABLE")
    if report["issues"]:
        return report
    phase("ssh-image-check", ["docker", "run", "--rm", "--network=none", "--entrypoint=/bin/bash",
                              expected_image_id, "-c", container_ssh.bootstrap(port, check_only=True)],
          "CONTAINER_SSH_DEPENDENCY_FAILED")
    if report["issues"]:
        return report
    metadata = phase("ssh-image-command", ["docker", "image", "inspect", "--format",
                     "[{{json .Config.Entrypoint}},{{json .Config.Cmd}}]", expected_image_id],
                     "CONTAINER_IMAGE_COMMAND_FAILED")
    if report["issues"]:
        return report
    commands = {}
    for node, result in metadata.items():
        try:
            commands[node] = container_ssh.image_command(result.stdout, container_command)
        except (ValueError, IndexError, TypeError) as exc:
            report["issues"].append({"node": node, "code": "CONTAINER_IMAGE_COMMAND_FAILED", "message": str(exc)})
    if report["issues"]:
        return report
    if any(command != commands[nodes[0]] for command in commands.values()):
        report["issues"].append({"node": "cluster", "code": "CONTAINER_IMAGE_COMMAND_FAILED",
                                 "message": "image Entrypoint/Cmd differs across nodes"})
        return report
    run = ["docker", "run", "-dit", "--name", container_name,
           "--label", f"{container_ssh.LABEL}={port}"]
    for volume in volumes:
        run += ["-v", volume]
    run += [*docker_args, "--entrypoint=/bin/bash", image,
            "-c", container_ssh.bootstrap(port), "hcu-container-ssh", *commands[nodes[0]]]
    body = shlex.join(run) + " >/dev/null"
    if operation == "container-recreate":
        # Do not hide failed removal (permissions/running daemon errors).
        body = (f"if docker inspect --type container {shlex.quote(container_name)} >/dev/null 2>&1; then "
                f"docker rm -f {shlex.quote(container_name)} >/dev/null || exit $?; fi; " + body)
    report["containers_changed"] = True  # change attempted; remote failures can be partial
    phase("ssh-create-container", ["bash", "-lc", body], "CONTAINER_CREATE_FAILED")
    if report["issues"]:
        return report
    expected = shlex.quote(f"true|{image}|{expected_image_id}")
    ready = (f"test \"$(docker inspect -f '{{{{.State.Running}}}}|{{{{.Config.Image}}}}|{{{{.Image}}}}' "
             f"{shlex.quote(container_name)})\" = {expected} && " + shlex.join([
                 "docker", "exec", container_name, "bash", "-c",
                 f"for ((i=0;i<30;i++)); do [[ -f {container_ssh.STATE}/ready ]] && break; sleep 1; done; "
                 f"test -f {container_ssh.STATE}/ready || {{ echo 'sshd startup failed; inspect docker logs and {container_ssh.STATE}/sshd.log' >&2; exit 1; }}; "
                 f"printf HCU_USER_KEY=; cat {container_ssh.STATE}/id_ed25519.pub; "
                 f"printf HCU_HOST_KEY=; cat {container_ssh.STATE}/host_ed25519.pub"]))
    evidence = phase("ssh-collect-public-keys", ["bash", "-lc", ready], "CONTAINER_SSH_START_FAILED")
    if report["issues"]:
        return report
    keys = {}
    for node, result in evidence.items():
        try:
            keys[node] = container_ssh.parse_public_keys(result.stdout)
        except (ValueError, TypeError) as exc:
            report["issues"].append({"node": node, "code": "CONTAINER_SSH_KEY_INVALID", "message": str(exc)})
    if report["issues"]:
        return report
    if any(len({key[index] for key in keys.values()}) != len(nodes) for index in (0, 1)):
        report["issues"].append({"node": "cluster", "code": "CONTAINER_SSH_KEY_REUSED",
                                 "message": "containers must have unique user and host keys; do not bake managed SSH state into the image"})
        return report
    for index, command in enumerate(container_ssh.upload_commands(container_name, container_ssh.trust_files(keys, port))):
        phase(f"ssh-public-key-batch-{index:04d}", command, "CONTAINER_SSH_TRUST_FAILED")
        if report["issues"]:
            return report
    phase("ssh-install-trust", container_ssh.install_trust(container_name), "CONTAINER_SSH_TRUST_FAILED")
    if report["issues"]:
        return report
    from .preflight import verify_container_mpi_peers
    groups, topology = container_ssh.verification_groups(nodes)
    if log:
        log("INFO", f"container SSH phase=verify-peers topology={topology} nodes={len(nodes)} port={port}")
    report["issues"].extend(verify_container_mpi_peers(
        nodes=nodes, groups=groups, container_name=container_name, port=port,
        transport=transport, concurrency=concurrency, run_dir=root))
    report["ssh_verification"] = topology
    report["status"] = "FAIL" if report["issues"] else "PASS"
    return report
