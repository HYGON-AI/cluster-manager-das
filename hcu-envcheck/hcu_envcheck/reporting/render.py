# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Atomic machine-readable reports and human-readable evidence tables."""
from __future__ import annotations

import json
from pathlib import Path

from cluster_run.consistency import render_consistency_markdown

from .model import PROTOCOL_VERSION, atomic, clean, number, public_root, table_text
from .export import _deployment_stamp
from .parsers import METRIC_COLUMNS
from .facts import STATE_LAYOUT, file_signature

FAILURE_FIELDS = ["stage_id", "item", "group", "nodes", "status", "reason_code", "detail", "evidence"]
# Keep iteration in machine-readable historical facts; new plans execute each item once.
RESULT_FIELDS = ["stage_id", "item", "iteration", "group", "nodes", "status", "exit_code", "cleanup", "reason_code", "detail", "evidence"]
RCCL_FIELDS = ["test", "status", *METRIC_COLUMNS, "peak_busbw_gbps", "baseline_busbw_gbps", "margin_pct",
               "threshold_busbw_gbps", "delta_pct", "returncode", "reason",
               "avg_busbw_gbps", "table_rows", "planned_sizes", "missing_sizes", "invalid_rows",
               "wrong_out", "wrong_in", "end_evidence",
               "actual_np", "actual_nodes", "devices_per_node", "rank_duplicates", "header_ngpus"]

# T24: write-out increment. Group detail files (per-stage tables, per-group
# RCCL reports) are re-rendered on every snapshot but only hit the disk when
# their content actually changed, so an unchanged terminal result costs no
# rewrite and unexecuted details are never fabricated early. The cache lives
# for the reporting process only; report-only starts a fresh process and
# rebuilds everything from disk once. atomic() is still used for every real
# write, keeping the rename semantics, idempotence and failure diagnostics.
_written: dict[Path, tuple[str, tuple]] = {}


def write_if_changed(path: Path, text: str):
    if _written.get(path) == (text, file_signature(path)):
        return
    atomic(path, text)
    _written[path] = text, file_signature(path)


def table_if_changed(path: Path, fields: list, records: list):
    write_if_changed(path, table_text(fields, records))


def md(value):
    return clean(value).replace("\\", "\\\\").replace("|", "\\|").replace("<", "&lt;").replace(">", "&gt;").replace("`", "\\`").replace("[", "\\[").replace("]", "\\]")


def gbps(value) -> str:
    return f"{value:.2f}" if isinstance(value, (int, float)) else ""


def rccl_baseline_table(results: list[dict]) -> tuple[list[str], list[list]]:
    """Baselines depend only on the group's card count; collapse rows while every group agrees."""
    items = list(dict.fromkeys(r["item"] for r in results))
    uniform = all(len({(r["metrics"].get("baseline_busbw_gbps"), r["metrics"].get("threshold_busbw_gbps"))
                       for r in results if r["item"] == item}) <= 1 for item in items)
    fields = ["测试项", "基准", "下限"] if uniform else ["测试项", "分组", "基准", "下限"]
    rows, seen = [], set()
    for result in results:
        key = (result["item"],) if uniform else (result["item"], result["group"])
        if key in seen:
            continue
        seen.add(key)
        metrics = result["metrics"]
        rows.append([result["item"], *([] if uniform else [result["group"]]),
                     gbps(metrics.get("baseline_busbw_gbps")), gbps(metrics.get("threshold_busbw_gbps"))])
    return fields, rows


def markdown_table(fields, records):
    return ["| " + " | ".join(md(x) for x in fields) + " |", "| " + " | ".join("---" for _ in fields) + " |",
            *("| " + " | ".join(md(x) for x in record) + " |" for record in records)]


def rccl_reports(directory: Path, results: list[dict]):
    records = [dict(test=r["item"], status=r["status"], returncode=r["exit_code"], reason=r["reason_code"], **r["metrics"]) for r in results]
    table_if_changed(directory / "rccl-summary.tsv", RCCL_FIELDS, records)
    content = ["# RCCL 测试结果", "",
               "四列为各消息尺寸的独立峰值（GB/s）；判定取 out/in busbw 最大值与准确卡数基准比较。",
               "完整性与正确性先于性能：数据表须覆盖计划消息尺寸、无损坏行、#wrong 计数为零且有结束标记，性能结论才生效；Avg 摘要仅作旁证。", ""]
    def _row(record):
        head = [record.get(k, "") for k in ("test", "status", *METRIC_COLUMNS, "peak_busbw_gbps",
                                            "baseline_busbw_gbps", "margin_pct", "threshold_busbw_gbps")]
        tail = [f"{record.get('table_rows', '')}/{record.get('planned_sizes', '')}",
                record.get("missing_sizes", ""), record.get("invalid_rows", ""),
                f"{record.get('wrong_out', '')}/{record.get('wrong_in', '')}",
                "有" if record.get("end_evidence") else "无",
                (f"{record.get('actual_np', '')}卡/{record.get('actual_nodes', '')}节点×{record.get('devices_per_node', '')}"
                 if record.get("actual_np") is not None else "无证据"),
                record.get("reason", "")]
        return head + tail
    content += markdown_table(["测试项", "结果", "out algbw", "out busbw", "in algbw", "in busbw", "判定峰值", "基准", "容差%", "下限",
                               "数据行", "缺尺寸", "损坏行", "#wrong", "结束", "实测规模", "原因"],
                              [_row(r) for r in records])
    write_if_changed(directory / "rccl-summary.md", "\n".join(content) + "\n")


def write(root: Path, snapshot: dict):
    output = public_root(root)
    compact = snapshot["control"].get("state_layout") == STATE_LAYOUT
    write_if_changed(root / "report.json", json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    table_if_changed(output / "failures.tsv", FAILURE_FIELDS, snapshot["failures"])
    table_if_changed(root / "preflight.tsv", ["group", "nodes", "status", "exit_code", "cleanup", "reason_code", "detail", "evidence"],
                     snapshot.get("preflight", []))
    for stage in snapshot["stages"]:
        if compact:
            break
        selected = [r for r in snapshot["results"] if r["stage_id"] == stage["stage_id"]]
        directory = root / "stages" / stage["stage_id"]
        # T27: the controller creates each stage's tree on demand at stage
        # start; a missing directory means the stage never ran, so no
        # per-stage artifacts are generated and this root-side writer never
        # owns stage/group directories the entry user extends later.
        if not directory.is_dir():
            continue
        flattened = [dict(r, **r["metrics"]) for r in selected]
        metric_keys = list(dict.fromkeys(k for r in selected for k in r["metrics"]))
        table_if_changed(directory / "summary.tsv", RESULT_FIELDS + metric_keys, flattened)
        if snapshot["control"].get("operation") == "rccl":
            for result in selected:
                group_dir = directory / "groups" / result["group"]
                if group_dir.is_dir():
                    rccl_reports(group_dir / "rccl", [result])
    if compact and snapshot["control"].get("operation") == "rccl":
        for group in snapshot["groups"]:
            directory = root / "prepared" / group["group"]
            if directory.is_dir():
                selected = [result for result in snapshot["results"] if result["group"] == group["group"]]
                rccl_reports(directory, selected)
    if snapshot["control"].get("operation") == "check_deep":
        checks = [dict(check, group=r["group"], stage_id=r["stage_id"]) for r in snapshot["results"] for check in r.get("checks", [])]
        table_if_changed(root / "nodes.tsv", ["node", "check", "status", "exit_code", "group", "detail", "evidence"], checks)
    remainder = snapshot.get("non_participating")
    remainder_line = None
    if remainder and remainder.get("nodes"):
        remainder_line = (f"- 未参与节点（{md(remainder.get('reason') or '未参与')}）："
                          f"{', '.join(md(node) for node in remainder['nodes'])}；"
                          "余数节点不建任务、不计入任务分母，完整组结论仅代表本次参与范围。")
    content = [f"# {md(snapshot['control'].get('operation', 'Cluster'))} 测试报告", "",
               f"- 结果：{snapshot['status']}",
               f"- 运行状态：{md(snapshot['control'].get('status', 'UNKNOWN'))}",
               f"- 计划 / 已执行 / 终态 / 有效结论：{snapshot.get('planned', 0)} / {snapshot.get('executed', 0)} / {snapshot.get('completed', 0)} / {snapshot.get('valid', 0)} 组项",
               "- 有效结论 = PASS/WARN 且清理 CONFIRMED；NA、未执行、取消、超时、清理未确认均不计入通过。",
               f"- 测试范围：{md(snapshot['control'].get('scope', ''))}",
               "- NA 为不适用；WARN 为告警；BLOCKED/CANCELLED/INCOMPLETE 不计通过。"]
    if remainder_line:
        content.append(remainder_line)
    scale = snapshot.get("planned_scale")
    if scale:
        content += [f"- 计划规模：每组 {scale['group_size']} 节点，每节点 {scale['gpus_per_node']} 卡，"
                    f"每组 {scale['planned_gpus_per_group']} 卡，MPI ranks={scale['mpi_ranks_per_group']}（0 表示直接启动）。"]
        if snapshot["control"].get("operation") == "deepep":
            content += [f"- DeepEP 计划 GPU 工作进程：每节点 {scale['planned_deepep_gpu_workers_per_node']}；"
                        "实际工作进程数：未知（未取得完整工作进程身份证据）。设备可见性证据见 report.json。"]
    # T30: coverage accounting. Source/participating/non-participating and
    # planned/executed/valid/untested are stated separately; untested items
    # never count as passing and coverage never merges across runs. Old runs
    # without T02 bookkeeping fall back to the planned group membership.
    members = [n for g in snapshot["groups"] for n in str(g.get("nodes", "")).split(",") if n]
    participating = int(number(snapshot["control"].get("participating_node_count"), len(set(members))))
    source = int(number(snapshot["control"].get("source_node_count"),
                        number(snapshot["control"].get("node_count"), len(set(members)))))
    unpart = max(0, source - participating)
    unpart_reason = clean(str(snapshot["control"].get("unparticipated_reason")
                              or (snapshot.get("non_participating") or {}).get("reason") or "无"))
    coverage = "无未参与节点" if unpart == 0 else f"未参与 {unpart}（{unpart_reason}）· 覆盖 {participating}/{source}"
    content += ["", "## 覆盖与未测范围", "",
                f"- 节点：来源 {source} · 参与 {participating} · {coverage}",
                f"- 组项：计划 {snapshot.get('planned', 0)} · 实际执行 {snapshot.get('executed', 0)} · 终态 {snapshot.get('completed', 0)} · 有效 {snapshot.get('valid', 0)} · 未测 {snapshot.get('not_executed', 0)}",
                "- 未测 = 计划-已执行（未派发/被隔离/被取消），不计入通过；覆盖率仅代表本 run，不同 run 的覆盖不可合并互补。"]
    content.append("")
    if snapshot.get("preflight"):
        content += ["## 环境预检", ""]
        content += markdown_table(["分组", "节点", "结果", "退出码", "清理", "原因", "证据"],
                                  [[r.get(k, "") for k in ("group", "nodes", "status", "exit_code", "cleanup", "detail", "evidence")]
                                   for r in snapshot["preflight"]])
        content += [""]
    content += ["## 按测试项汇总", ""]
    historical_rounds = any(number(s.get("iteration"), 0) != 0 for s in snapshot["stages"])
    content += markdown_table(["测试项", *(["轮次"] if historical_rounds else []), "通过", "告警", "失败", "未完成", "不适用"],
                              [[s.get("item", ""), *([s.get("iteration", "")] if historical_rounds else []),
                                *(s.get(k, "") for k in ("passed", "warned", "failed", "incomplete", "na"))]
                               for s in snapshot["stages"]])
    content += ["", "## 异常与证据", ""]
    content += markdown_table(["测试项", "分组", "节点", "结果", "类别", "原因", "证据"],
                              [[f.get(k, "") for k in ("item", "group", "nodes", "status", "category", "detail", "evidence")] for f in snapshot["failures"]]) if snapshot["failures"] else ["无异常。"]
    content += ["", "## 完整结果", ""]
    content += markdown_table(["阶段", "分组", "节点", "结果", "退出码", "清理", "说明", "日志"],
                              [[r.get(k, "") for k in ("stage_id", "group", "nodes", "status", "exit_code", "cleanup", "detail", "evidence")] for r in snapshot["results"]])
    if snapshot["control"].get("operation") == "rccl":
        content += ["", "## RCCL 指标（GB/s）", "", "基准与下限按测试项列出，组规模一致时全部分组通用；判定取各分组 out/in busbw 最大值，实测值见「完整结果」。", ""]
        content += markdown_table(*rccl_baseline_table(snapshot["results"]))
        details = (".state/prepared/<分组>/rccl-summary.md 与 rccl-summary.tsv" if compact else
                   ".state/stages/<阶段>/groups/<分组>/rccl/rccl-summary.md 与 rccl-summary.tsv")
        content += ["", f"四列独立峰值（out/in algbw/busbw）、判定峰值、真实规模基准与下限的逐分组明细见 run 内 {details}；缺失项留空，不填 0。"]
    if snapshot.get("consistency_summary"):
        content += ["", render_consistency_markdown(snapshot["consistency_summary"]).rstrip()]
    stamp = _deployment_stamp()
    content += ["", "## 版本", "",
                f"- 解析协议 v{PROTOCOL_VERSION} · 报告代码指纹 {stamp['reporting_md5']}"]
    if snapshot["control"].get("operation") == "deepep" and historical_rounds:
        content.append("- 协议 v3 已移除跨轮稳定性判定；历史多轮事实仍逐阶段保留，重建总结果不含旧版跨轮稳定性条件。")
    content.append("- 报告可由 --report-only 在原报告宿主原地重建；不同代码版本重建的展示须分别标注版本，不得互换冒充本轮证据。")
    write_if_changed(output / "summary.md", "\n".join(content) + "\n")
