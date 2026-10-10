# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Explicit creation-time SSH bootstrap; never used by read-only checks.

Only public keys cross the controller. Each container owns its private keys.
Host networking is intentional: node:port must identify that node's container.
"""
from __future__ import annotations

import base64
import json
import posixpath
import re
import shlex
from typing import Sequence

STATE = "/var/lib/hcu-cluster-ssh"
LABEL = "hcu.cluster.ssh-port"
_NODE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")


def validate_options(port: int, nodes: Sequence[str], volumes: Sequence[str], docker_args: Sequence[str]) -> list[str]:
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ValueError("--port must be between 1 and 65535")
    if not nodes or any(len(node) > 253 or not _NODE.fullmatch(node) for node in nodes):
        raise ValueError("--port requires plain node hostnames/IPv4 addresses (no user@host or SSH patterns)")
    # These locations are written only INSIDE the new container. Never allow
    # an explicit bind/volume to turn them into host-side SSH configuration.
    protected = ("/root", STATE, "/run", "/etc", "/var/run")
    for volume in volumes:
        fields = volume.split(":")
        target = posixpath.normpath(fields[1] if len(fields) > 1 else fields[0])
        if not target.startswith("/") or any(
            target == p or target.startswith(p + "/") or p.startswith(target.rstrip("/") + "/")
            for p in protected
        ):
            raise ValueError("--port cannot mount over SSH state, /root, /etc or runtime directories")
    output = []
    args = iter(docker_args)
    for arg in args:
        flag, sep, value = arg.partition("=")
        if flag in {"--network", "--net"}:
            value = value if sep else next(args, "")
            if value != "host":
                raise ValueError("--port requires --network=host; bridge/published ports are not supported")
            continue
        # Keep the managed entrypoint, private PID namespace and writable key
        # directory under our control. Mounts must use the validated --volume.
        if flag in {"--entrypoint", "--user", "-u", "--pid", "--mount", "--volume", "-v",
                    "--volumes-from", "--tmpfs", "--read-only", "--publish", "-p", "-P", "--publish-all",
                    "--name", "--rm", "--label", "-l", "--label-file"} or (
                        arg.startswith(("-u", "-v", "-p", "-l")) and not arg.startswith("--")
                    ):
            raise ValueError(f"--port cannot be combined with --docker-arg={arg}; use dedicated options")
        output.append(arg)
    return ["--network=host", *output]


def sshd_config(port: int) -> str:
    return f"""Port {port}
HostKey {STATE}/host_ed25519
PidFile {STATE}/sshd.pid
AuthorizedKeysFile {STATE}/authorized_keys
PermitRootLogin prohibit-password
PubkeyAuthentication yes
AuthenticationMethods publickey
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitEmptyPasswords no
UsePAM yes
AllowUsers root
StrictModes yes
AllowAgentForwarding no
AllowTcpForwarding no
X11Forwarding no
PermitTunnel no
Subsystem sftp internal-sftp
"""


def bootstrap(port: int, *, check_only: bool = False) -> str:
    """Runs at EVERY container start, before the original image argv.

    An isolated image check exercises dependencies/config BEFORE removing old
    containers. PAM account policy is retained; no password is set or unlocked.
    """
    tail = 'exit 0' if check_only else f"""
# Preserve keys/trust across docker restart; recreate generates new keys.
/usr/sbin/sshd -f {STATE}/sshd_config -E {STATE}/sshd.log
touch {STATE}/ready
if (( $# )); then exec "$@"; else exec sleep infinity; fi
"""
    image_guard = f"[[ ! -e {STATE} ]] || fail 'image contains managed SSH state; rebuild it without private keys'" if check_only else ""
    return f"""set -euo pipefail
umask 077
fail() {{ printf 'SSH_BOOTSTRAP: %s\\n' "$*" >&2; exit 1; }}
[[ $(id -u) == 0 ]] || fail 'image USER must be root for --port'
(( BASH_VERSINFO[0] >= 4 )) || fail 'Bash 4+ is required'
for tool in ssh ssh-keygen getent cut readlink base64 cat chmod mkdir mv touch sleep; do
  command -v "$tool" >/dev/null || fail "missing $tool; prepare the offline image first"
done
[[ -x /usr/sbin/sshd && -r /etc/pam.d/sshd ]] || fail 'OpenSSH server/PAM configuration is missing'
[[ $(getent passwd root | cut -d: -f6) == /root ]] || fail 'root home must be /root'
{image_guard}
for path in /root /root/.ssh /var /var/lib {STATE} /run /run/sshd; do
  [[ ! -L $path ]] || fail "refusing symlink: $path"
done
mkdir -p {STATE} /run/sshd /root/.ssh
chmod 700 {STATE} /root/.ssh
for path in /root/.ssh/config /root/.ssh/config.hcu-original /root/.ssh/config.hcu-new; do
  [[ ! -L $path ]] || fail "symlink SSH config: $path"
done
for key in id_ed25519 host_ed25519; do
  [[ ! -L {STATE}/$key && ! -L {STATE}/$key.pub ]] || fail 'symlink key'
  if [[ ! -e {STATE}/$key ]]; then
    ssh-keygen -q -t ed25519 -N '' -C hcu-container-ssh -f {STATE}/$key
  fi
  [[ -f {STATE}/$key && -f {STATE}/$key.pub ]] || fail 'incomplete SSH key pair'
done
for file in sshd_config sshd.log ready authorized_keys known_hosts; do
  [[ ! -L {STATE}/$file ]] || fail "symlink state: $file"
done
printf '%s' {shlex.quote(sshd_config(port))} > {STATE}/sshd_config
touch {STATE}/authorized_keys {STATE}/known_hosts
/usr/sbin/sshd -t -f {STATE}/sshd_config
{tail}
"""


def port_check(container: str, port: int) -> list[str]:
    # A recreate may reuse only a listener demonstrably owned by the old
    # managed container. An unrelated/unattributable listener fails closed.
    owned = f"""set -euo pipefail
declare -A sockets=()
hex=$(printf '%04X' {port})
for table in /proc/net/tcp /proc/net/tcp6; do
  [[ -r $table ]] || continue
  while read -ra fields; do
    if [[ ${{fields[1]-}} == *:$hex && ${{fields[3]-}} == 0A ]]; then
      sockets[${{fields[9]}}]=missing
    fi
  done < "$table"
done
(( ${{#sockets[@]}} )) || exit 1
for fd in /proc/[0-9]*/fd/*; do
  target=$(readlink "$fd" 2>/dev/null || true)
  if [[ $target =~ ^socket:\\[([0-9]+)\\]$ ]]; then
    inode=${{BASH_REMATCH[1]}}
    if [[ -n ${{sockets[$inode]+present}} ]]; then sockets[$inode]=owned; fi
  fi
done
for state in "${{sockets[@]}}"; do [[ $state == owned ]] || exit 1; done
"""
    return ["bash", "-lc", f"""set -euo pipefail
command -v ss >/dev/null || {{ echo 'host ss command missing' >&2; exit 1; }}
listeners=$(ss -H -ltn 'sport = :{port}')
if [[ -n "$listeners" ]]; then
  managed=$(docker inspect -f '{{{{index .Config.Labels "{LABEL}"}}}}|{{{{.HostConfig.PidMode}}}}' {shlex.quote(container)} 2>/dev/null || true)
  if [[ $managed != '{port}|' ]] || ! {shlex.join(['docker', 'exec', container, 'bash', '-c', owned])}; then
    echo 'port {port} is occupied or ownership cannot be verified; choose another --port' >&2
    exit 1
  fi
fi
"""]


def image_command(stdout: str, override: str | None) -> list[str]:
    # Only Entrypoint and Cmd are read, not image Env (which can hold secrets).
    line = stdout.strip().splitlines()[-1]
    data = json.loads(line)
    if not isinstance(data, list) or len(data) != 2:
        raise ValueError("invalid image command metadata")
    entry, cmd = [part or [] for part in data]
    if any(not isinstance(part, list) or any(not isinstance(x, str) or "\x00" in x for x in part)
           for part in (entry, cmd)):
        raise ValueError("invalid image Entrypoint/Cmd")
    return entry + (shlex.split(override) if override is not None else cmd)


def parse_public_keys(stdout: str) -> tuple[str, str]:
    values = {}
    for line in stdout.splitlines():
        if line.startswith(("HCU_USER_KEY=", "HCU_HOST_KEY=")):
            name, key = line.split("=", 1)
            fields = key.split()
            if name in values or len(fields) < 2 or fields[0] != "ssh-ed25519":
                raise ValueError("invalid/duplicate SSH public key evidence")
            blob = base64.b64decode(fields[1], validate=True)
            if len(blob) != 51 or blob[:19] != b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20":
                raise ValueError("invalid ed25519 public key")
            values[name] = " ".join(fields[:2])
    if set(values) != {"HCU_USER_KEY", "HCU_HOST_KEY"}:
        raise ValueError("missing SSH public key evidence")
    return values["HCU_USER_KEY"], values["HCU_HOST_KEY"]


def trust_files(keys: dict[str, tuple[str, str]], port: int) -> dict[str, str]:
    options = (f"  User root\n  Port {port}\n  IdentityFile {STATE}/id_ed25519\n"
               f"  UserKnownHostsFile {STATE}/known_hosts\n  GlobalKnownHostsFile /dev/null\n"
               "  StrictHostKeyChecking yes\n  IdentitiesOnly yes\n  BatchMode yes\n")
    # Per-host blocks keep unrelated SSH destinations and existing configs intact.
    names = list(keys)
    client = "".join(f"Host {' '.join(names[index:index + 8])}\n{options}"
                     for index in range(0, len(names), 8)) + "Host *\n"
    return {
        "authorized_keys": "".join(key[0] + "\n" for key in keys.values()),
        "known_hosts": "".join(f"[{node}]:{port} {key[1]}\n" if port != 22 else f"{node} {key[1]}\n"
                               for node, key in keys.items()),
        "client_config": client,
    }


def upload_commands(container: str, files: dict[str, str]):
    """Chunk public data: avoids ARG_MAX/Windows limits at thousand-node scale."""
    for name, content in files.items():
        for index, start in enumerate(range(0, len(content), 4096)):
            payload = base64.b64encode(content[start:start + 4096].encode()).decode()
            redirect = ">" if index == 0 else ">>"
            yield ["docker", "exec", container, "bash", "-c",
                   f"set -e; umask 077; printf %s {payload} | base64 -d {redirect} {STATE}/{name}.new"]


def install_trust(container: str) -> list[str]:
    return ["docker", "exec", container, "bash", "-c", f"""set -euo pipefail
umask 077
[[ ! -L /root/.ssh/config && ! -L /root/.ssh/config.hcu-original ]] || exit 1
if [[ ! -e /root/.ssh/config.hcu-original ]]; then
  if [[ -f /root/.ssh/config ]]; then
    cat /root/.ssh/config > /root/.ssh/config.hcu-original
  else : > /root/.ssh/config.hcu-original; fi
fi
cat {STATE}/client_config.new /root/.ssh/config.hcu-original > /root/.ssh/config.hcu-new
chmod 600 /root/.ssh/config.hcu-new {STATE}/authorized_keys.new {STATE}/known_hosts.new
mv /root/.ssh/config.hcu-new /root/.ssh/config
mv {STATE}/known_hosts.new {STATE}/known_hosts
mv {STATE}/authorized_keys.new {STATE}/authorized_keys
"""]


def verification_groups(nodes: Sequence[str]) -> tuple[list[tuple[str, ...]], str]:
    if len(nodes) == 1:
        return [(nodes[0], nodes[0])], "self"
    if len(nodes) <= 32:
        return [(node, *(other for other in nodes if other != node)) for node in nodes], "all-to-all"
    # Do not launch O(N^2) SSH probes at ten-thousand-card scale. Every node
    # authenticates as both source and destination; active preflight still
    # verifies the real group leader -> every member routes before each run.
    groups = [tuple(nodes)]
    groups += [(node, nodes[0], nodes[(index + 1) % len(nodes)]) for index, node in enumerate(nodes[1:], 1)]
    return groups, "leader-star+ring"
