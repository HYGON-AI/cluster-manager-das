# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""A bounded terminal region, with append-only snapshots for pipes and files."""
from __future__ import annotations

import sys
import time
import unicodedata
from pathlib import Path

from .log_store import log_size, log_tail
from .facts import STATE_LAYOUT
from .model import TERMINAL, age, clean, direct_group_logs, number


def width(text):
    return sum(0 if unicodedata.combining(c) else 2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)


def truncate(text, maximum):
    text = clean(text)
    if width(text) <= maximum:
        return text
    output, used = "", 0
    for char in text:
        size = width(char)
        if used + size > maximum - 1:
            break
        output += char
        used += size
    return output + "…"


class Display:
    def __init__(self, mode="plain", columns=80):
        self.mode, self.columns = mode, max(36, columns)
        self.height = 0
        self.last_update = 0.0
        self.last_stage = None
        self.logs = None

    def show(self, snapshot, force=False):
        info = snapshot["control"]
        current = info.get("current_stage", "")
        final = snapshot["final"]
        now = time.monotonic()
        interval = 1 if self.mode == "tty" else 15
        if not force and current == self.last_stage and now - self.last_update < interval:
            return
        self.last_update, self.last_stage = now, current
        if self.mode == "quiet" and not final:
            return
        name = {"rccl": "RCCL", "deepep": "DeepEP", "byte-flux": "byte-flux",
                "check_deep": "check_deep", "ib-write-bw": "IB write BW"}.get(
                    info.get("operation"), info.get("operation", "Cluster"))
        state = ("已结束 · " + snapshot["status"]) if final else ("预检中" if current in {"", "0"} else "运行中")
        output = [f"  HCU / {name}    {state} · {age(info.get('started_epoch'))}",
                  f"  {info.get('node_count', '?')} 节点 · {info.get('group_count', '?')} 组", ""]
        full_paths = set()
        if final:
            if info.get("status") == "PRECHECK_FAILED":
                preflight = snapshot.get("preflight", [])
                passed = sum(r["status"] in {"PASS", "WARN", "NA"} for r in preflight)
                output += [f"  环境预检    通过 {passed}    失败 {len(preflight) - passed}"]
            else:
                output += ["  测试项                    通过  告警  失败  未完  N/A"]
                for stage in snapshot["stages"]:
                    label = truncate(stage.get("label") or stage["item"], 23)
                    output.append("  " + label + " " * max(1, 25 - width(label)) +
                                  " ".join(f"{stage[k]:4}" for k in ("passed", "warned", "failed", "incomplete", "na")))
            cleanup_records = snapshot.get("preflight", []) if info.get("status") == "PRECHECK_FAILED" else snapshot["results"]
            cleanup_unit = "预检组" if info.get("status") == "PRECHECK_FAILED" else "组项"
            confirmed = sum(r["cleanup"] == "CONFIRMED" for r in cleanup_records)
            unsafe = sum(r["cleanup"] == "UNCONFIRMED" for r in cleanup_records)
            output += ["", f"  覆盖  {snapshot['completed']} / {snapshot['planned']} 组项进入终态 · 有效结论 {snapshot.get('valid', 0)}",
                       f"  清理  {confirmed} {cleanup_unit}已确认 · {unsafe} {cleanup_unit}未确认"]
            if snapshot["failures"]:
                output += ["", f"  异常 · {len(snapshot['failures'])}条"]
                for item in snapshot["failures"][:3]:
                    evidence = f"             {self.evidence_label(snapshot, item['evidence'])}"
                    full_paths.add(evidence)
                    output += [f"  {item['group']}  {item['item']} · {item['status']}",
                               f"             {item['detail'] or item['reason_code']}",
                               evidence]
                if len(snapshot["failures"]) > 3:
                    output += [f"  另有 {len(snapshot['failures'])-3} 条，见失败明细"]
            output += ["", "  总报告    summary.md", "  失败明细  failures.tsv"]
        else:
            stages = snapshot["stages"]
            stage = next((s for s in stages if s["stage_id"] == current), None)
            if stage is None and 0 < (count := int(number(current, 0) or 0)) <= len(stages):
                # The controller reports current_stage as a 1-based count, not a stage id.
                stage = stages[count - 1]
            if stage:
                current = stage["stage_id"]
            selected = [r for r in snapshot["results"] if r["stage_id"] == current]
            if stage:
                index = next(i for i, s in enumerate(stages, 1) if s["stage_id"] == current)
                output += [f"  当前  {index:02} / {len(stages):02} · {stage.get('label') or stage['item']}"]
                ratio = stage["completed"] / stage["planned"] if stage["planned"] else 0
                filled = round(20 * ratio)
                output += [f"  {'█' * filled}{'░' * (20-filled)}  {ratio:.0%}    {stage['completed']} / {stage['planned']} 组", "",
                           f"  通过 {stage['passed']}    失败 {stage['failed']}    运行 {stage['running']}    等待 {stage['pending']}"]
                if stage["warned"] or stage["na"] or any(r["status"] in {"BLOCKED", "CANCELLED"} for r in selected):
                    output += [f"  告警 {stage['warned']}    不适用 {stage['na']}    跳过 {sum(r['status'] in {'BLOCKED', 'CANCELLED'} for r in selected)}"]
                running = [r for r in selected if r["status"] == "RUNNING"]
                if running:
                    oldest = min(running, key=lambda r: number(r["started_epoch"], time.time()))
                    output += [f"  最久  {oldest['group']} · {age(oldest['started_epoch'])}"]
                    output += self.live_log(oldest)
            else:
                output += ["  正在检查节点、报告环境及测试计划"]
            anomalies = [f for f in snapshot["failures"] if f["stage_id"] == current]
            previous = [f for f in snapshot["failures"] if f["stage_id"] != current]
            output += [""]
            if anomalies:
                output += [f"  本项异常 · {len(anomalies)}条", "  " + "─" * min(self.columns - 4, 54)]
                for item in anomalies[:3]:
                    output += [f"  {item['group']}  {item['item']} · {item['status']}", f"             {item['detail'] or item['reason_code']}"]
                if len(anomalies) > 3:
                    output += [f"  另有 {len(anomalies)-3} 条，见报告"]
            elif stage:
                output += ["  本项暂无异常"]
            if previous:
                output += [f"  前序异常 {len(previous)} 条，见报告"]
            item_now = next((i for i, s in enumerate(stages, 1) if s["stage_id"] == current), 0)
            running_groups = sum(r["status"] == "RUNNING" for r in selected)
            output += ["", f"  总进度  测试项 {item_now:02} / {len(stages):02} · 并发组 {running_groups} / {info.get('group_count', '?')}",
                       "  报告    summary.md"]
            # The logs path is only useful complete; keep it out of the width clamp.
            self.logs = f"  日志    {self.logs_label(snapshot)}"
        output = [line if line in full_paths else truncate(line, self.columns - 1) for line in output]
        if self.logs:
            output.append(self.logs)
            self.logs = None
        if self.mode == "tty":
            # Only erase the region we own; never clear the screen or its scrollback.
            if self.height:
                sys.stderr.write(f"\x1b[{self.height}F")
            height = max(self.height, len(output))
            for index in range(height):
                line = output[index] if index < len(output) else ""
                color = "\x1b[2m" if "报告" in line or "明细" in line else "\x1b[31m" if any(x in line for x in ("FAIL", "不达标", "执行失败", "未确认")) else "\x1b[36m" if index == 0 else ""
                sys.stderr.write("\x1b[2K" + color + line + ("\x1b[0m" if color else "") + "\n")
            self.height = height
        else:
            sys.stderr.write("\n".join(output) + "\n\n")
        sys.stderr.flush()

    def live_log(self, record):
        # Collective launchers run mpirun on the group's first node and stream the
        # aggregated output into that node's log; group.log only carries worker
        # bookkeeping between phases, so it is the fallback, not the source.
        span = min(self.columns - 4, 54)
        head = f"  ── {record['group']} 实时日志 "
        task_dir = Path(record.get("raw_evidence") or record["evidence"]).parent
        master = (record.get("nodes", "").split(",") + [""])[0]
        source = None
        candidates = ([task_dir / f"{master}.log"] if master else []) + [task_dir / "group.log"]
        state = next((path for path in task_dir.parents if path.name == ".state"), None)
        if state is not None and direct_group_logs(state, task_dir):
            candidates.insert(0, state.parent / f"{record['group']}.log")
        for candidate in candidates:
            if log_size(candidate) is not None:
                source = candidate
                break
        tail = []
        if source is not None:
            try:
                text = log_tail(source)
                tail = [line for line in map(clean, text.splitlines()) if line.strip()][-6:]
            except OSError:
                tail = []
        if not tail:
            tail = ["等待日志输出…"]
        return [head + "─" * max(0, span - (width(head) - 2)),
                *[f"  {line}" for line in tail],
                "  " + "─" * span]

    @staticmethod
    def evidence_label(snapshot, evidence):
        if not evidence or Path(evidence).is_absolute() or evidence.startswith(("/", "\\")) or evidence[1:2] == ":":
            return evidence
        return Display.root_label(snapshot).rstrip("/\\") + "/" + evidence

    @staticmethod
    def root_label(snapshot):
        return (snapshot["control"].get("public_run_dir") or
                snapshot["control"].get("entry_run_dir") or snapshot.get("run_dir", "<run>"))

    @staticmethod
    def logs_label(snapshot):
        # Keep the complete public path copyable even in the bounded panel.
        root = Display.root_label(snapshot).rstrip("/\\")
        if snapshot["control"].get("state_layout") == STATE_LAYOUT:
            # Compact facts own committed ranges; neither legacy index trees
            # nor a pre-existing logs/ directory determine the public location.
            # This also identifies the path before the first collector starts.
            return root + "/group-*.log"
        local = Path(snapshot.get("run_dir") or root)
        if local.name == ".state":
            local = local.parent
        archived = any(path.is_file() for path in (local / ".state" / "log-archive").glob("group-*.tsv"))
        current = any(path.is_file() for path in local.glob("group-*.log"))
        suffix = "/logs/group-*.log" if not (archived or current) and (local / "logs").is_dir() else "/group-*.log"
        return root + suffix
