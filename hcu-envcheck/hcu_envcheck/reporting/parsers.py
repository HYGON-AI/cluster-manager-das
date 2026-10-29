# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""One-shot parsing of completed tasks; running logs are never rescanned."""
from __future__ import annotations

import json
import re
from pathlib import Path

from ..environment import evaluate_environment
from .log_store import log_size
from .model import TERMINAL, control, direct_group_logs, lines, log_path, number, rows

METRIC_COLUMNS = ("out_of_place_algbw_gbps", "out_of_place_busbw_gbps",
                  "in_place_algbw_gbps", "in_place_busbw_gbps")

# T04 explicit default platform profile: structural requirements only, no
# site versions. Optional components surface as WARN instead of silently
# passing; a collection exit code of 0 never means healthy by itself.
PLATFORM_PROFILE = dict(
    expected_device_count=None,
    require_compiler=False,
    require_rdma=False,
    minimum_rdma_devices=0,
    require_rccl=False,
    require_ucx=False,
    expected_rdma_protocol="auto",
    rdma_policy=None,
    required_python_packages=("torch",),
)

# T05 explicit default resource profile, reused from the established resource
# policy defaults (no site-specific thresholds invented here). Busy = a card
# exceeds a threshold in >= busy_sample_quorum of the collected samples.
RESOURCE_PROFILE = dict(
    expected_devices=None,
    max_vram_used_percent=5.0,
    max_hcu_util_percent=5.0,
    busy_sample_quorum=2,
)


def fail(result: dict, code: str, detail: str):
    result.update(status="FAIL", reason_code=code, detail=detail)


def error_excerpt(path: Path) -> str:
    found = []
    command_block = False
    for line in lines(path):
        marker = line.strip()
        if marker == "[COMMAND-BEGIN]" or re.match(r"^【(?:MPI 启动命令|实际测试命令|执行命令)｜", marker):
            command_block = True
            continue
        if marker in {"[COMMAND-END]", "【命令结束】"}:
            command_block = False
            continue
        if command_block or line.startswith(("[COMMAND]", "[MPI-COMMAND]", "[TEST-COMMAND]", "[TEST]", "[NODES]", "[ENV]", "[START]", "[END]")):
            continue
        if re.search(r"error|failed|failure|exception|refused|timed out|traceback", line, re.I):
            found.append(line.strip()[:240])
            if len(found) == 2:
                break
    return " · ".join(found)


def task_diagnostic(root: Path, task: Path, fact: dict, reader=rows) -> tuple[Path, str]:
    """Find the failed node's real output, including runs with empty group.log."""
    if direct_group_logs(root, task):
        path = log_path(root, task, fact.get("log", ""))
        return path, error_excerpt(path) or fact.get("reason_code") or f"执行失败 rc={fact.get('exit_code', '?')}，详见分组日志"
    nodes = reader(task / "node-results.tsv")
    failed = [node for node in nodes if number(node.get("exit_code"), -1) != 0]
    for node in failed:
        path = log_path(root, task, node.get("log", ""))
        excerpt = error_excerpt(path)
        if excerpt:
            return path, f"{node.get('node', '?')} · rc={node.get('exit_code', '?')} · {excerpt}"
    group_log = log_path(root, task, fact.get("log", ""))
    excerpt = error_excerpt(group_log)
    if excerpt:
        return group_log, excerpt
    # Silent commands and non-English errors still need a usable evidence path.
    for node in failed or nodes:
        path = log_path(root, task, node.get("log", ""))
        if log_size(path):
            return path, f"{node.get('node', '?')} · rc={node.get('exit_code', '?')}；详见节点日志"
    return group_log, fact.get("reason_code") or f"执行失败 rc={fact.get('exit_code', '?')}，未取得节点输出"


def rccl(root: Path, task: Path, result: dict, plan=None, directory=None):
    item, group = result["item"], result["group"]
    plan = plan if plan is not None else next((x for x in rows(root / "prepared" / group / "rccl-plan.tsv")
                 if x.get("test") == item), {})
    directory = directory if directory is not None else task / "rccl"
    execution = next((x for x in rows(directory / "execution.tsv")
                      if x.get("test") == item), {})
    if not execution and result["status"] == "FAIL":
        # Launch failures precede creation of the collective log.
        return
    path = log_path(root, directory, execution["log"]) if execution.get("log") else directory / f"{item}.log"
    result["evidence"] = str(path)
    # T08: the full evidence set is parsed first — the planned size header,
    # every 13-column table row with both #wrong counters, the trailing
    # summary and the payload end marker. The summary line or a few
    # high-bandwidth rows can never stand in for the complete table, and
    # non-finite/malformed cells disqualify their row instead of feeding
    # the peaks. Performance is only judged after completeness and
    # correctness hold; the four columns stay independent peaks.
    peaks, buses, averages = [None] * 4, [], []
    observed, invalid_rows, wrong_out, wrong_in = [], 0, 0, 0
    planned_header = None
    ended = False
    # T09: actual execution scale evidence — every participating card prints
    # a "# Rank N Group G Pid P on <node> device D" line (single machine: one
    # process, nGpus cards; multi machine: one MPI rank per card). Planned
    # np never fills these fields; missing rank evidence blocks the verdict.
    rank_devices, rank_ids, rank_duplicates = [], [], 0
    header_gpus = None
    for line in lines(path):
        header = re.search(r"minBytes (\d+) maxBytes (\d+) step: ([\d.]+)\(factor\)", line)
        if header and planned_header is None:
            planned_header = (int(header[1]), int(header[2]), float(header[3]))
        scale = re.search(r"nThread (\d+) nGpus (\d+)", line)
        if scale and header_gpus is None:
            header_gpus = int(scale[2])
        rank = re.match(r"#\s+Rank\s+(\d+)\s+Group\s+\d+\s+Pid\s+\d+\s+on\s+(\S+)\s+device\s+(\d+)", line)
        if rank:
            rank_ids.append(int(rank[1]))
            pair = (rank[2], int(rank[3]))
            if pair in rank_devices or int(rank[1]) in rank_ids[:-1]:
                rank_duplicates += 1
            else:
                rank_devices.append(pair)
        if "rccl test done" in line:
            ended = True
        match = re.search(r"Avg bus bandwidth\s*:\s*([\d.eE+-]+)", line)
        if match and number(match[1]) is not None:
            averages.append(number(match[1]))
        cells = line.split()
        if len(cells) >= 8 and cells[0].isdigit() and cells[1].isdigit():
            # Root/partitioned tests (gather/scatter/alltoall family) print
            # size-0 preamble rows and mark the in-place #wrong column N/A
            # (no in-place validation); N/A is the tool's contract, not an
            # error, while anything else non-numeric disqualifies the row.
            if len(cells) < 13 or not cells[8].isdigit() or not (cells[12].isdigit() or cells[12] == "N/A"):
                invalid_rows += 1
                continue
            values = [number(cells[i]) for i in (6, 7, 10, 11)]
            if any(value is None for value in values):
                invalid_rows += 1
                continue
            observed.append(int(cells[0]))
            peaks = [max(a, b) if a is not None else b for a, b in zip(peaks, values)]
            buses.extend((values[1], values[3]))
            wrong_out += int(cells[8])
            wrong_in += int(cells[12]) if cells[12].isdigit() else 0
    ladder = []
    if planned_header:
        minimum, maximum, factor = planned_header
        size = minimum
        while size <= maximum and len(ladder) < 128:
            ladder.append(size)
            size = int(size * factor) if factor > 1 else maximum + 1
    # The ladder is anchored at the header (minBytes..maxBytes); each test
    # type starts printing at its own effective first size (uniform tests at
    # minBytes, root tests at the first ladder member they actually run), so
    # completeness requires every ladder member from the observed start up.
    positive = sorted({size for size in observed if size > 0})
    expected = [size for size in ladder if positive and size >= positive[0]]
    missing = sorted(set(expected) - set(positive))
    nodes_seen = sorted({node for node, _ in rank_devices})
    per_node = [sum(1 for node, _ in rank_devices if node == member) for member in nodes_seen]
    actual_np = len(rank_devices)
    devices_per_node = (str(per_node[0]) if min(per_node) == max(per_node)
                        else f"{min(per_node)}-{max(per_node)}") if per_node else None
    peak = max(buses) if buses else None
    baseline, margin, np = (number(plan.get(k)) for k in ("baseline_busbw_gbps", "margin_pct", "np"))
    threshold = baseline * (1 - margin / 100) if baseline and margin is not None else None
    result["metrics"] = dict(zip(METRIC_COLUMNS, peaks))
    result["metrics"].update(peak_busbw_gbps=peak, baseline_busbw_gbps=baseline,
                             margin_pct=margin, threshold_busbw_gbps=threshold, np=np,
                             delta_pct=(100 * (peak - baseline) / baseline if peak is not None and baseline else None),
                             avg_busbw_gbps=(averages[-1] if averages else None),
                             table_rows=len(observed), planned_sizes=len(expected),
                             missing_sizes=len(missing), invalid_rows=invalid_rows,
                             wrong_out=wrong_out, wrong_in=wrong_in,
                             end_evidence=int(ended),
                             actual_np=actual_np, actual_nodes=len(nodes_seen),
                             devices_per_node=devices_per_node,
                             rank_duplicates=rank_duplicates, header_ngpus=header_gpus)
    if result["status"] not in {"PASS", "FAIL"}:
        return
    if number(result.get("exit_code"), -1) != 0 or number(execution.get("exit_code"), -1) != 0:
        fail(result, "EXECUTION_FAILED", error_excerpt(path) or f"测试退出码 {execution.get('exit_code', result['exit_code'])}")
    elif baseline is None or baseline <= 0 or np is None or np <= 0 or margin is None or not 0 <= margin <= 100:
        fail(result, "BASELINE_MISSING", "缺少当前卡数对应的有效基准或容差")
    elif not observed:
        fail(result, "DATA_TABLE_MISSING", "未解析到有效数据表（Avg摘要行不能替代完整表格）")
    elif planned_header is None:
        fail(result, "PLAN_HEADER_MISSING", "日志缺少 minBytes/maxBytes/step 计划头，无法核对消息尺寸覆盖")
    elif not positive:
        fail(result, "DATA_TABLE_INCOMPLETE", "数据表仅含0尺寸行，缺少实际消息尺寸证据")
    elif invalid_rows:
        fail(result, "DATA_ROWS_INVALID", f"{invalid_rows} 行数据非有限值或列数/校验列损坏，不能作为有效证据")
    elif missing:
        fail(result, "DATA_TABLE_INCOMPLETE", f"数据表缺 {len(missing)}/{len(expected)} 个计划尺寸（首缺 {missing[0]}B），疑似截断或漏跑")
    elif wrong_out or wrong_in:
        fail(result, "CORRECTNESS_FAILED", f"校验错误计数非零：out-of-place #wrong={wrong_out}，in-place #wrong={wrong_in}")
    elif not ended:
        fail(result, "END_EVIDENCE_MISSING", "缺少载荷结束标记（rccl test done），执行证据不完整")
    elif not rank_devices:
        fail(result, "ACTUAL_SCALE_MISSING", "日志缺少 Rank/设备证据行，实际执行规模无法核验（不默认通过）")
    elif rank_duplicates:
        fail(result, "ACTUAL_SCALE_DUPLICATE", f"Rank/设备证据存在 {rank_duplicates} 处重复")
    elif np is not None and actual_np != int(np):
        fail(result, "SCALE_MISMATCH", f"计划 np={np:g} 与实际 {actual_np} 卡（{len(nodes_seen)} 节点×{devices_per_node}卡）不符")
    elif peak is None:
        fail(result, "METRIC_MISSING", "未解析到带宽")
    elif peak < threshold:
        fail(result, "BELOW_BASELINE", f"实测 {peak:.2f} GB/s，要求 ≥{threshold:.2f} GB/s")
    elif result["status"] == "PASS":
        result.update(reason_code="PASS", detail=f"{peak:.2f} GB/s ≥ {threshold:.2f} GB/s（{len(observed)}/{len(expected)}尺寸，#wrong=0，实际{actual_np}卡/{len(nodes_seen)}节点×{devices_per_node}卡）")


def deepep(root: Path, task: Path, result: dict, nodes: list[dict]):
    master = result["nodes"].split(",")[0]
    node = next((n for n in nodes if n.get("node") == master), {})
    path = log_path(root, task, node.get("log", result["evidence"]))
    result["evidence"] = str(path)
    # T13: fixed-pattern metric with an explicit representative rule. The
    # pinned upstream prints one line per participant (lowl: per-rank
    # "Dispatch + combine bandwidth"; internode/intranode: local-rank-0
    # "[tuning] Best dispatch", possibly once per dtype) — never decide by
    # the last log line. The representative is the MINIMUM over all matched
    # lines (the worst participant, deterministic and rank-agnostic), using
    # a fixed pattern and unit GB/s for the selected mode.
    pattern = (r"Dispatch \+ combine bandwidth:\s*([\d.]+) GB/s" if result["item"] == "lowl"
               else r"\[tuning\] Best dispatch.*?([\d.]+) GB/s \((?:RDMA|NVL)\)")
    values = []
    for line in lines(path):
        match = re.search(pattern, line)
        if not match:
            continue
        value = number(match[1])
        if value is not None:
            values.append(value)
    metric = min(values) if values else None
    result["metrics"] = {"bandwidth_gbps": metric, "metric_rule": "min" if values else None,
                         "metric_samples": len(values), "metric_unit": "GB/s"}
    if result["status"] == "PASS" and (metric is None or metric <= 0):
        fail(result, "METRIC_MISSING", "首节点日志未解析到固定模式带宽指标（lowl=Dispatch+combine；其余=[tuning] Best dispatch）")


# T15: the byte-flux reporting matrix mirrors the payload's fixed 70B sweep
# (flux_70b_test.sh): 16 hidden sizes x ag/gemmrs variants = 64 case IDs per
# node. The parser accounts every case by its stable ID so a duplicate can
# never cancel out a missing case.
FLUX_SIZES = (2048, 2176, 2304, 2560, 3072, 4096, 4160, 4352, 4608, 5120, 6144,
              8192, 16384, 32768, 65536, 131072)


def _flux_expected_cases() -> list[str]:
    cases = [f"ag-{size}x10240" for size in FLUX_SIZES]
    cases += [f"ag-{size}x57344" for size in FLUX_SIZES]
    cases += [f"gemmrs-{size}x8192x8192" for size in FLUX_SIZES]
    cases += [f"gemmrs-{size}x8192x28672" for size in FLUX_SIZES]
    return cases


def byte_flux(root: Path, task: Path, result: dict, nodes: list[dict]):
    expected_cases = _flux_expected_cases()
    declared = None
    cases: dict[str, dict] = {}
    finished = 0
    for node in nodes:
        path = log_path(root, task, node.get("log", ""))
        for line in lines(path):
            text = line.strip()
            match = re.search(r"^flux-70b sweep start cases=(\d+)$", text)
            if match:
                declared = number(match[1])
                continue
            if re.search(r"^flux-70b sweep (PASS|FAIL)$", text):
                finished += 1
                continue
            match = re.search(r"^flux-70b case (\S+) (start|retry|PASS|FAIL rc=\d+)$", text)
            if not match:
                continue
            name, outcome = match[1], match[2]
            state = cases.setdefault(name, {"starts": 0, "retries": 0, "outcomes": []})
            if outcome == "start":
                state["starts"] += 1
            elif outcome == "retry":
                state["retries"] += 1
            else:
                state["outcomes"].append("PASS" if outcome == "PASS" else "FAIL")
    started = sum(1 for state in cases.values() if state["starts"])
    final_pass = [name for name, state in cases.items() if state["outcomes"] and state["outcomes"][-1] == "PASS"]
    final_fail = [name for name, state in cases.items() if state["outcomes"] and state["outcomes"][-1] == "FAIL"]
    # T15: a bounded retry that finally passes keeps the FIRST failure
    # addressable — recovery never erases it from the record.
    first_fail_recovered = [name for name, state in cases.items()
                            if state["outcomes"] and state["outcomes"][0] == "FAIL" and state["outcomes"][-1] == "PASS"]
    retried = sorted({name for name, state in cases.items() if state["retries"] or len(state["outcomes"]) > 1} | set(first_fail_recovered))
    missing = [name for name in expected_cases if name not in cases or not cases[name]["outcomes"]]
    unknown = sorted(name for name in cases if name not in expected_cases)
    duplicated = sorted(name for name, state in cases.items()
                        if state["starts"] > 1 or len(state["outcomes"]) > 2)
    result["metrics"] = {"cases_planned": len(expected_cases), "cases_declared": declared,
                         "cases_started": started, "cases_passed": len(final_pass),
                         "cases_failed": len(final_fail), "cases_retried": len(retried),
                         "retried_cases": retried, "first_fail_recovered": first_fail_recovered,
                         "failed_cases": final_fail, "missing_cases": missing,
                         "unknown_cases": unknown, "duplicate_cases": duplicated,
                         "sweep_finished": finished}
    if result["status"] == "PASS":
        if final_fail:
            fail(result, "CASE_FAILED", f"{len(final_fail)}例最终失败：{', '.join(final_fail[:6])}")
        elif missing:
            fail(result, "INCOMPLETE", f"缺 {len(missing)}/{len(expected_cases)} 个预期case（如 {', '.join(missing[:4])}）")
        elif unknown:
            fail(result, "INCOMPLETE", f"出现计划外case：{', '.join(unknown[:4])}")
        elif duplicated:
            fail(result, "CASE_DUPLICATED", f"case标记重复：{', '.join(duplicated[:4])}")
        elif declared is not None and declared != len(expected_cases):
            fail(result, "MARKER_CONFLICT", f"载荷声明 {declared} 例与解析矩阵 {len(expected_cases)} 例不一致")
        elif finished < len(nodes):
            fail(result, "INCOMPLETE", "部分节点未输出整套扫描结论（疑似截断）")
        elif retried:
            result.update(reason_code="PASS", detail=f"{len(expected_cases)}例全部通过；其中{len(retried)}例首次失败经有界重试恢复（首次失败保留于retried_cases）")
        else:
            result.update(reason_code="PASS", detail=f"{len(expected_cases)}例全部通过，无重试无异常")


def check_deep(root: Path, task: Path, result: dict, nodes: list[dict]):
    checks = []
    for node in nodes:
        path = log_path(root, task, node.get("log", ""))
        sections, active = {}, None
        for line in lines(path):
            header = re.search(r"====\s*([1-4])\. .*====", line)
            if header:
                active = ("ACS", "XDP", "RDMA", "ibv_devinfo")[int(header[1]) - 1]
                sections[active] = []
            elif "====" in line:
                active = None
            elif active and (mark := re.search(r"\[(PASS|WARN|FAIL)\]\s*(.*)", line)):
                sections[active].append((mark[1], mark[2]))
        rc = number(node.get("exit_code"), -1)
        for name in ("ACS", "XDP", "RDMA", "ibv_devinfo"):
            entries = sections.get(name, [])
            status = max((s for s, _ in entries), key=lambda x: {"PASS": 0, "WARN": 1, "FAIL": 2}[x]) if entries else "INCOMPLETE"
            detail = " · ".join(t for s, t in entries if s == status)
            checks.append({"node": node.get("node", ""), "check": name, "status": status,
                           "detail": detail or "缺少检查结论", "evidence": str(path), "exit_code": node.get("exit_code", "")})
        if rc not in {0, 1, 2}:
            checks.append({"node": node.get("node", ""), "check": "execution", "status": "FAIL",
                           "detail": error_excerpt(path) or f"执行失败 rc={rc:g}", "evidence": str(path), "exit_code": rc})
    result["checks"] = checks
    if result["status"] in {"PASS", "FAIL", "WARN"} and checks:
        if any(c["status"] in {"FAIL", "INCOMPLETE"} for c in checks):
            fail(result, "CHECK_FAILED", "存在失败或未完成检查项")
        elif number(result.get("exit_code"), -1) not in {0, 2}:
            fail(result, "EXECUTION_FAILED", "检查执行异常")
        elif any(c["status"] == "WARN" for c in checks):
            result.update(status="WARN", reason_code="CHECK_WARN", detail="检查含告警，请确认是否影响测试")
        elif result["status"] == "PASS":
            result.update(reason_code="PASS", detail="四项检查通过")


def _platform_payload(path: Path) -> dict | None:
    """Extract the pod_probe JSON payload line from a node log."""
    for line in lines(path):
        text = line.strip()
        if not text.startswith("{") or "schema_version" not in text:
            continue
        try:
            payload = json.loads(text)
        except ValueError:
            continue
        if isinstance(payload, dict) and "system" in payload:
            return payload
    return None


def _health_conclusion(result: dict, checks: list[dict], label: str) -> None:
    """Map rule rows onto the stage conclusion, severity only ever escalates.

    A later evaluation (e.g. resource after platform in a combined run) may
    raise PASS->WARN or ->FAIL but never downgrade an existing WARN/FAIL, and
    an existing execution diagnosis keeps its reason untouched.
    """
    if result["reason_code"] in {"EXECUTION_FAILED", "INCOMPLETE"} or result["status"] not in {"PASS", "FAIL", "WARN"}:
        return
    if any(c["status"] == "FAIL" for c in checks):
        if result["status"] != "FAIL":
            fail(result, f"{label}_HEALTH_FAILED", f"{label}健康规则存在FAIL项（明细见逐节点检查）")
    elif any(c["status"] in {"WARN", "UNKNOWN"} for c in checks):
        if result["status"] == "PASS":
            result.update(status="WARN", reason_code=f"{label}_HEALTH_WARN", detail=f"{label}健康规则含WARN项，请确认是否影响训练")
    elif result["status"] == "PASS":
        result.update(reason_code=f"{label}_HEALTH_PASS", detail=f"{label}健康检查通过（规则项见checks）")


def platform_health(root: Path, task: Path, result: dict, nodes: list[dict]) -> None:
    """Judge platform health from collected content (T04).

    Each node's collected pod_probe payload is evaluated with the pure rule in
    hcu_envcheck.environment; collection/parse failures and every rule finding
    keep their reason and raw evidence path. An existing execution diagnosis
    keeps priority; the rule conclusion only replaces a bare exit-code PASS.
    """
    checks = []
    for node in nodes:
        name = node.get("node", "")
        path = log_path(root, task, node.get("log", ""))
        payload = _platform_payload(path)
        if payload is None:
            checks.append({"node": name, "check": "PAYLOAD_PARSE", "status": "FAIL",
                           "detail": "节点日志未解析到platform采集JSON（采集失败或解析失败）",
                           "evidence": str(path), "exit_code": node.get("exit_code", "")})
            result.setdefault("environment_records", {})[name] = {"environment": {}}
            continue
        checks.append({"node": name, "check": "PAYLOAD_PARSE", "status": "PASS",
                       "detail": f"pod_probe schema={payload.get('schema_version')}",
                       "evidence": str(path), "exit_code": node.get("exit_code", "")})
        # evaluate_environment returns (findings, summary, rule_checks); the
        # per-rule rows used here are the third element. Its summary already
        # carries every static field the consistency core compares (T06).
        _, _summary, rule_checks = evaluate_environment(payload, **PLATFORM_PROFILE)
        result.setdefault("environment_records", {})[name] = {"environment": _summary}
        for item in rule_checks:
            checks.append({"node": name, "check": item["check_id"], "status": item["status"],
                           "detail": item["message"], "evidence": str(path),
                           "exit_code": node.get("exit_code", "")})
    result["checks"] = checks
    _health_conclusion(result, checks, "PLATFORM")


def resource_health(root: Path, task: Path, result: dict, nodes: list[dict]) -> None:
    """Judge resource adequacy from collected samples (T05).

    Reuses the pure rules (parse_hy_smi_samples / parse_rocminfo /
    evaluate_metrics) on the payload's resource samples; missing tools, absent
    samples, unparsable output and insufficient samples can never conclude
    idle — each failure keeps its reason row and the raw node-log evidence.
    """
    from ..parsers import ParseError, parse_hy_smi_samples, parse_rocminfo
    from ..preflight import evaluate_metrics

    checks = result.setdefault("checks", [])
    for node in nodes:
        name = node.get("node", "")
        path = log_path(root, task, node.get("log", ""))
        payload = _platform_payload(path) or {}
        metrics = payload.get("resource") or {}
        common = {"node": name, "evidence": str(path), "exit_code": node.get("exit_code", "")}
        if not metrics.get("hy_smi_path"):
            checks.append({**common, "check": "HY_SMI_NOT_FOUND", "status": "FAIL",
                           "detail": "目标环境未找到hy-smi，无法采集卡资源"})
            result.setdefault("environment_records", {}).setdefault(name, {"environment": {}})
            continue
        if not metrics.get("memory"):
            checks.append({**common, "check": "RESOURCE_METRICS", "status": "FAIL",
                           "detail": "节点日志缺少resource采样数据（采集失败或旧格式）"})
            result.setdefault("environment_records", {}).setdefault(name, {"environment": {}})
            continue
        try:
            hy_cards = parse_hy_smi_samples(
                [item.get("stdout", "") for item in metrics.get("memory", [])],
                [item.get("stdout", "") for item in metrics.get("available", [])],
                [item.get("stdout", "") for item in metrics.get("memory_percent", [])],
                [item.get("stdout", "") for item in metrics.get("utilization", [])],
                (metrics.get("bus") or {}).get("stdout"),
            )
            roc_agents = parse_rocminfo((metrics.get("rocminfo") or {}).get("stdout", ""))
        except (ParseError, TypeError, ValueError) as exc:
            checks.append({**common, "check": "RESOURCE_PARSE", "status": "FAIL",
                           "detail": f"采样解析失败: {exc}"})
            result.setdefault("environment_records", {}).setdefault(name, {"environment": {}})
            continue
        devices, metric_findings, summary = evaluate_metrics({}, hy_cards, roc_agents, **RESOURCE_PROFILE)
        # T06: static device profile plus the system memory view for the
        # consistency core; transient utilization/usage values never enter
        # the record, so time-varying load cannot read as a config difference.
        record = result.setdefault("environment_records", {}).setdefault(name, {})
        environment = record.setdefault("environment", {})
        if "mem_total" not in environment:
            # Resource-only payloads still embed collect_system output; in a
            # combined run platform_health already stored the fuller summary.
            system = payload.get("system") or {}
            environment.update({
                "cpu_logical_count": system.get("cpu_logical_count"),
                "cpu_affinity_count": system.get("cpu_affinity_count"),
                "cpu_models": system.get("cpu_models", []),
                "mem_total": (system.get("meminfo") or {}).get("MemTotal"),
                "cgroup_memory_max": system.get("cgroup_memory_max"),
                "cgroup_cpu_max": system.get("cgroup_cpu_max"),
                "dev_shm": system.get("dev_shm"),
            })
        record["devices"] = [{"model": getattr(card, "model", None),
                              "architecture": getattr(card, "architecture", None),
                              "rocminfo_total_mib": getattr(card, "rocminfo_total_mib", None),
                              "hy_smi_total_mib": getattr(card, "hy_smi_total_mib", None)}
                             for card in devices]
        record["device_count"] = len(devices)
        checks.append({**common, "check": "RESOURCE_METRICS", "status": "PASS",
                       "detail": (summary or f"cards={len(hy_cards)} samples={metrics.get('samples')} interval={metrics.get('interval_seconds')}s")[:240]})
        for item in metric_findings:
            device = getattr(item, "device_id", None)
            checks.append({**common, "check": item.reason_code, "status": item.severity,
                           "detail": (f"卡{device} " if device is not None else "") + item.message})
    _health_conclusion(result, checks, "RESOURCE")


def device_evidence(root: Path, task: Path, result: dict, nodes: list[dict]):
    """Runtime visibility is evidence of visible devices, not spawned workers."""
    master = result["nodes"].split(",")[0]
    node = next((entry for entry in nodes if entry.get("node") == master), {})
    path = log_path(root, task, node.get("log", "group.log"))
    seen = {}
    for line in lines(path):
        match = re.search(r"\[DEVICE-SELECTION\] node=(\S+) count=(\d+) HIP_VISIBLE_DEVICES=(\S+) pci=(\S+)", line)
        if match:
            record = {"node": match[1], "visible_count": int(match[2]), "mask": match[3],
                      "pci_devices": match[4].split(",")}
            seen[(match[1], match[3], match[4])] = record
    result["device_visibility_evidence"] = list(seen.values())
    if result["operation"] == "deepep":
        # The upstream does not emit a complete, unique GPU-worker census.
        result["actual_gpu_worker_count"] = None


def parse(root: Path, stage: dict, group: dict, final: bool = False) -> dict:
    task = root / "stages" / stage["stage_id"] / "groups" / group["group"]
    records = rows(task / "task.tsv")
    fact = records[0] if records else {}
    # dispatched distinguishes "planned but never launched" from launched
    # tasks. A BLOCKED task.tsv is a placeholder the scheduler writes for a
    # group that was never dispatched in this stage, so it is not executed.
    result = dict(stage_id=stage["stage_id"], item=stage["item"], iteration=stage.get("iteration", ""),
                  group=group["group"], nodes=group.get("nodes", ""), status=fact.get("status", "PENDING"),
                  exit_code=fact.get("exit_code", ""), cleanup=fact.get("cleanup", "NOT_STARTED"),
                  started_epoch=fact.get("started_epoch", ""), ended_epoch=fact.get("ended_epoch", ""),
                  reason_code=fact.get("reason_code", ""), detail="", evidence=str(log_path(root, task, fact.get("log", ""))), metrics={},
                  dispatched=bool(records) and fact.get("status") != "BLOCKED")
    if final and result["status"] not in TERMINAL:
        result.update(status="INCOMPLETE", reason_code="INCOMPLETE", detail="计划任务未进入终态")
    if result["status"] == "NA":
        # Applicability is orthogonal to cleanup: an unconfirmed cleanup must
        # still surface on NA items instead of being silently skipped.
        if result["cleanup"] != "CONFIRMED":
            result.update(reason_code="CLEANUP_UNCONFIRMED", detail="不适用，但清理未确认")
        else:
            result["detail"] = "不适用"
        return result
    if result["status"] not in TERMINAL or result["status"] in {"BLOCKED", "CANCELLED", "TIMEOUT"}:
        result["detail"] = result["reason_code"] or result["detail"]
        return result
    nodes = rows(task / "node-results.tsv")
    expected = set(filter(None, group.get("nodes", "").split(",")))
    actual = [n.get("node") for n in nodes]
    operation = result["operation"] = control(root).get("operation", "")
    allowed = {0, 2} if operation == "check_deep" else {0}
    # Collective group launchers run on the master and write one result for all peers.
    required = ({next(iter(group.get("nodes", "").split(",")), "")}
                if operation in {"rccl", "deepep", "ib-write-bw"} else expected)
    if len(actual) != len(set(actual)) or not required.issubset(set(actual)):
        fail(result, "INCOMPLETE", "逐节点执行记录缺失或重复")
    elif any(number(n.get("exit_code"), -1) not in allowed for n in nodes):
        evidence, detail = task_diagnostic(root, task, fact)
        result["evidence"] = str(evidence)
        fail(result, "EXECUTION_FAILED", detail)
    if operation == "rccl":
        rccl(root, task, result)
    elif operation == "deepep":
        deepep(root, task, result, nodes)
    elif operation == "byte-flux":
        byte_flux(root, task, result, nodes)
    elif operation == "check_deep":
        check_deep(root, task, result, nodes)
    if operation in {"rccl", "deepep"} and control(root).get("gpus_per_node"):
        device_evidence(root, task, result, nodes)
    if operation in {"platform", "platform,resource"}:
        platform_health(root, task, result, nodes)
    if operation in {"resource", "platform,resource"}:
        resource_health(root, task, result, nodes)
    if result["cleanup"] != "CONFIRMED":
        fail(result, "CLEANUP_UNCONFIRMED", "测试进程清理未确认，暂停本组后续项")
    if result["status"] == "FAIL" and not result["detail"]:
        evidence, detail = task_diagnostic(root, task, fact)
        result.update(evidence=str(evidence), detail=detail)
    return result
