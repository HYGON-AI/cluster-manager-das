import argparse
import glob
import json
import logging
import os

# Issue: [B403:blacklist] Consider possible security implications associated with pickle module.
# Severity: Low   Confidence: High
# CWE: CWE-502 (https://cwe.mitre.org/data/definitions/502.html)
# More Info: https://bandit.readthedocs.io/en/1.8.6/blacklists/blacklist_imports.html#b403-import-pickle
import pickle  # nosec
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

try:
    from .standalone_support import (
        AttributionState,
        NVRxAttribution,
        bounded_log_value,
        effective_run_or_init_config,
        normalize_attribution_args,
    )
except ImportError:
    from standalone_support import (
        AttributionState,
        NVRxAttribution,
        bounded_log_value,
        effective_run_or_init_config,
        normalize_attribution_args,
    )

try:
    from .capture import capture_logs
except ImportError:
    from capture import capture_logs

try:
    from .topo_to_json import TopologyFormatError, convert_topology_file
except ImportError:
    from topo_to_json import TopologyFormatError, convert_topology_file

logger = logging.getLogger(__name__)

DEFAULT_JSON_RESULT_PATH = Path("/tmp/trace_analyzer_result.json")


_TOPOLOGY_GROUP_BY_PG_DESC = {
    "TENSOR_MODEL_PARALLEL_GROUP": "tp",
    "PIPELINE_MODEL_PARALLEL_GROUP": "pp",
    "DATA_PARALLEL_GROUP": "dp",
    "DATA_PARALLEL_GROUP_WITH_CP": "dp-cp",
    "CONTEXT_PARALLEL_GROUP": "cp",
    "TENSOR_AND_CONTEXT_PARALLEL_GROUP": "tp-cp",
    "EXPERT_MODEL_PARALLEL_GROUP": "ep",
    "EXPERT_TENSOR_PARALLEL_GROUP": "etp",
    "EXPERT_DATA_PARALLEL_GROUP": "edp",
    "EXPERT_DATA_PARALLEL_GROUP_WITH_CP": "edp",
    "TENSOR_AND_EXPERT_PARALLEL_GROUP": "tp-ep",
    "TENSOR_AND_DATA_PARALLEL_GROUP_WITH_CP": "tp-dp-cp",
    "TENSOR_AND_PIPELINE_PARALLEL_GROUP": "tp-pp",
    "MODEL_PARALLEL_GROUP": "tp-pp",
    "EMBEDDING_GROUP": "embd-pp",
    "POSITION_EMBEDDING_GROUP": "pos_embd-pp",
}


# Helper to print to stderr instead of stdout (for MCP compatibility)
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class _RestrictedTraceUnpickler(pickle.Unpickler):
    """Load plain-data FR dumps without allowing arbitrary Python objects."""

    def find_class(self, module: str, name: str):
        raise pickle.UnpicklingError(
            f"unsupported object in flight-recorder dump: {module}.{name}"
        )


def _parse_rank_list(rank_text: str) -> List[int]:
    ranks = []
    for token in rank_text.split(','):
        token = token.strip()
        if not token:
            continue
        try:
            ranks.append(int(token))
        except ValueError:
            continue
    return ranks


def _extract_missing_ranks_from_table(text: str) -> List[int]:
    hanging_ranks = set()
    capture = False

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("PGID") and "Missing Ranks" in stripped:
            capture = True
            continue
        if not capture or "|" not in stripped:
            continue

        columns = [col.strip() for col in stripped.split("|")]
        if len(columns) < 6:
            continue
        for rank in _parse_rank_list(columns[-1]):
            hanging_ranks.add(rank)

    return sorted(hanging_ranks)


def _process_group_type(pg_desc: str) -> str:
    """Return the process-group type without the window/index suffix."""
    return pg_desc.split(",", 1)[0].strip()


def _topology_group_type(pg_desc: str) -> Optional[str]:
    """Map a Flight Recorder process-group description to a topology group key."""

    return _TOPOLOGY_GROUP_BY_PG_DESC.get(_process_group_type(pg_desc))


def _pg_result_sort_key(value: Any) -> Tuple[int, Any, str]:
    raw_pg_id = value[0] if isinstance(value, tuple) and value else value
    rank_key = _rank_sort_key(raw_pg_id)
    return rank_key[0], rank_key[1], str(value)


def _parse_fr_table_rows(text: str) -> List[Dict[str, Any]]:
    rows = []
    capture = False

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("PGID") and "Missing Ranks" in stripped:
            capture = True
            continue
        if not capture or "|" not in stripped:
            continue

        columns = [col.strip() for col in stripped.split("|")]
        if len(columns) < 6:
            continue

        pgid, pg_desc, op_type, size, dtype, missing_ranks = columns[:6]
        rows.append(
            {
                "pgid": pgid,
                "pg_desc": pg_desc,
                "group_type": _process_group_type(pg_desc),
                "op_type": op_type,
                "size": size,
                "dtype": dtype,
                "missing_ranks": _parse_rank_list(missing_ranks),
            }
        )

    return rows


def _rank_sort_key(rank_value: Any) -> Tuple[int, Any]:
    try:
        return (0, int(rank_value))
    except (TypeError, ValueError):
        return (1, str(rank_value))


def _find_progress_outliers(
    expected_ranks: Any, progress_by_rank: Mapping[Any, Any]
) -> Tuple[set, Dict[str, Any]]:
    """Find process-group progress outliers using a strict-majority baseline.

    A maximum-progress baseline incorrectly blames the majority when one rank is ahead.
    This helper only attributes a divergent rank when one progress value has a strict
    majority. A split without a majority is reported as ambiguous and is not converted
    into rank attribution.
    """

    expected = sorted({int(rank) for rank in expected_ranks})
    expected_set = set(expected)
    normalized_progress: Dict[int, int] = {}
    for rank, value in progress_by_rank.items():
        try:
            normalized_rank = int(rank)
            normalized_value = int(value)
        except (TypeError, ValueError):
            continue
        if normalized_rank in expected_set:
            normalized_progress[normalized_rank] = normalized_value

    missing_status_ranks = expected_set - set(normalized_progress)
    counts = Counter(normalized_progress.values())
    baseline = None
    baseline_support = 0
    strategy = "no_status"
    ambiguous_ranks = set()
    divergent_ranks = set()

    if counts:
        most_common_value, most_common_support = counts.most_common(1)[0]
        baseline_support = most_common_support
        if len(counts) == 1:
            baseline = most_common_value
            strategy = "consensus"
        elif most_common_support > len(normalized_progress) / 2:
            baseline = most_common_value
            strategy = "strict_majority"
            divergent_ranks = {
                rank
                for rank, progress in normalized_progress.items()
                if progress != baseline
            }
        else:
            strategy = "ambiguous_no_majority"
            ambiguous_ranks = set(normalized_progress)

    outlier_ranks = divergent_ranks | missing_status_ranks
    return outlier_ranks, {
        "baseline": baseline,
        "baseline_support": baseline_support,
        "status_count": len(normalized_progress),
        "expected_rank_count": len(expected),
        "strategy": strategy,
        "missing_status_ranks": sorted(missing_status_ranks),
        "ambiguous_ranks": sorted(ambiguous_ranks),
    }


def _relative_score_confidence_level(relative_score_ratio: float) -> str:
    """Map a relative evidence share to a coarse, non-probabilistic level."""
    if relative_score_ratio >= 0.40:
        return "high"
    if relative_score_ratio >= 0.20:
        return "medium"
    return "low"


_DIAGNOSTIC_SIGNAL_ZH = {
    "unmatched_collective_ahead": "collective 序列超前或调用不匹配",
    "collective_progress_behind": "collective 进度落后",
    "missing_process_group_status": "缺少进程组通信状态",
    "missing_collective_participation": "缺少 collective 参与记录或 P2P 配对",
    "trace_entry_count_outlier": "Trace 条目数异常，可能提前退出或转储截断",
    "multi_group_propagation": "异常跨多个通信组传播",
    "multi_operation_impact": "异常涉及多种通信操作",
}

_STRONG_ROOT_SIGNALS = {
    "unmatched_collective_ahead",
    "missing_process_group_status",
    "missing_collective_participation",
}


def _anomaly_progress_delta(anomaly: Mapping[str, Any]) -> Optional[int]:
    try:
        current = int(anomaly.get("last_enqueued"))
        baseline = int(anomaly.get("baseline_enqueued"))
    except (TypeError, ValueError):
        return None
    return abs(current - baseline)


def _build_diagnostic_signal_profile(
    item: Mapping[str, Any], entry_count_outlier_ranks: set
) -> Dict[str, Any]:
    """Summarize root-cause signals without assuming one NCCL failure pattern."""
    signals = set()
    signal_score = 0.0
    anomalies = item["progress_anomalies"]

    for anomaly in anomalies:
        direction = anomaly.get("direction")
        if direction == "ahead":
            signals.add("unmatched_collective_ahead")
            signal_score += 3.0
        elif direction == "missing_status":
            signals.add("missing_process_group_status")
            signal_score += 3.0
        elif direction == "behind":
            signals.add("collective_progress_behind")
            signal_score += 1.0
        else:
            signal_score += 0.25

        delta = _anomaly_progress_delta(anomaly)
        if delta:
            # A large progress gap is relevant but must not dominate all other
            # evidence because sequence-number spaces can differ across groups.
            signal_score += min(1.5, 0.25 * delta)

    if item["evidence"] and not anomalies:
        # Rows without a pg-status anomaly usually come from direct absence in
        # a collective window, an old dump without pg_status, or P2P mismatch.
        signals.add("missing_collective_participation")
        signal_score += 2.5

    if item["rank"] in entry_count_outlier_ranks:
        signals.add("trace_entry_count_outlier")
        signal_score += 1.0

    if len(item["group_types"]) > 1:
        signals.add("multi_group_propagation")
        signal_score += 0.50 * (len(item["group_types"]) - 1)

    if len(item["op_types"]) > 1:
        signals.add("multi_operation_impact")
        signal_score += 0.25 * (len(item["op_types"]) - 1)

    return {
        "diagnostic_signals": sorted(signals),
        "diagnostic_signals_zh": [
            _DIAGNOSTIC_SIGNAL_ZH[signal] for signal in sorted(signals)
        ],
        "root_cause_signal_score": round(signal_score, 4),
    }


def _candidate_topology_groups(candidate: Mapping[str, Any]) -> List[set]:
    groups = []
    seen = set()
    for anomaly in candidate["progress_anomalies"]:
        raw_ranks = anomaly.get("topology_expected_ranks")
        if not isinstance(raw_ranks, (list, tuple, set)):
            continue
        try:
            ranks = frozenset(int(rank) for rank in raw_ranks)
        except (TypeError, ValueError):
            continue
        if ranks and ranks not in seen:
            groups.append(set(ranks))
            seen.add(ranks)
    return groups


def build_rank_candidates(payload: Any) -> Dict[str, Any]:
    """Separate affected ranks from root suspects using multiple NCCL signals.

    The classifier deliberately treats ``ahead`` as only one possible signal.
    Missing process-group state, direct collective/P2P absence, truncated traces,
    and a dominant repeated ``behind`` pattern can also identify a likely cause.
    Ambiguous evidence remains visible as affected/unresolved instead of being
    hidden or forced into a root-cause label.
    """
    if isinstance(payload, Mapping):
        analysis_text = str(payload.get("analysis_text") or "")
        fallback_ranks = payload.get("hanging_rank_list") or []
        progress_anomalies = payload.get("progress_anomalies") or []
        trace_summary = payload.get("trace_summary") or {}
    else:
        analysis_text = str(payload or "")
        fallback_ranks = []
        progress_anomalies = []
        trace_summary = {}

    raw_entry_count_outliers = (
        trace_summary.get("entry_count_outlier_ranks", [])
        if isinstance(trace_summary, Mapping)
        else []
    )
    entry_count_outlier_ranks = set()
    for rank in raw_entry_count_outliers:
        try:
            entry_count_outlier_ranks.add(int(rank))
        except (TypeError, ValueError):
            continue

    rank_evidence: Dict[int, Dict[str, Any]] = defaultdict(
        lambda: {
            "rank": None,
            "score": 0.0,
            "occurrences": 0,
            "pgids": set(),
            "group_types": set(),
            "op_types": set(),
            "evidence": [],
            "progress_anomalies": [],
        }
    )

    rows = _parse_fr_table_rows(analysis_text)
    for row in rows:
        for rank in row["missing_ranks"]:
            item = rank_evidence[rank]
            item["rank"] = rank
            item["occurrences"] += 1
            item["pgids"].add(row["pgid"])
            item["group_types"].add(row["group_type"])
            item["op_types"].add(row["op_type"])
            item["score"] += 1.0
            item["evidence"].append(
                {
                    "pgid": row["pgid"],
                    "pg_desc": row["pg_desc"],
                    "group_type": row["group_type"],
                    "op_type": row["op_type"],
                    "size": row["size"],
                    "dtype": row["dtype"],
                }
            )

    if not rank_evidence:
        fallback_candidates = list(fallback_ranks)
        fallback_candidates.extend(sorted(entry_count_outlier_ranks))
        for rank in fallback_candidates:
            try:
                normalized_rank = int(rank)
            except (TypeError, ValueError):
                continue
            item = rank_evidence[normalized_rank]
            item["rank"] = normalized_rank
            item["occurrences"] = 1
            item["score"] = 1.0

    for anomaly in progress_anomalies:
        if not isinstance(anomaly, Mapping):
            continue
        try:
            rank = int(anomaly.get("rank"))
        except (TypeError, ValueError):
            continue
        if rank in rank_evidence:
            rank_evidence[rank]["progress_anomalies"].append(dict(anomaly))

    for item in rank_evidence.values():
        item["score"] += 0.50 * max(0, len(item["group_types"]) - 1)
        item["score"] += 0.25 * max(0, len(item["op_types"]) - 1)
        item.update(
            _build_diagnostic_signal_profile(item, entry_count_outlier_ranks)
        )

    total_score = sum(float(item["score"]) for item in rank_evidence.values()) or 1.0
    total_root_signal_score = (
        sum(
            float(item["root_cause_signal_score"])
            for item in rank_evidence.values()
        )
        or 1.0
    )
    candidates = []
    for item in rank_evidence.values():
        relative_score_ratio = float(item["score"]) / total_score
        root_cause_signal_ratio = (
            float(item["root_cause_signal_score"]) / total_root_signal_score
        )
        candidates.append(
            {
                "rank": item["rank"],
                "relative_score_ratio": round(relative_score_ratio, 4),
                "score": round(float(item["score"]), 4),
                "root_cause_signal_score": item["root_cause_signal_score"],
                "root_cause_signal_ratio": round(root_cause_signal_ratio, 4),
                "diagnostic_signals": item["diagnostic_signals"],
                "diagnostic_signals_zh": item["diagnostic_signals_zh"],
                "occurrences": item["occurrences"],
                "pgids": sorted(item["pgids"], key=_rank_sort_key),
                "group_types": sorted(item["group_types"]),
                "op_types": sorted(item["op_types"]),
                "evidence": item["evidence"],
                "progress_anomalies": item["progress_anomalies"],
            }
        )

    candidates.sort(
        key=lambda item: (
            -item["root_cause_signal_score"],
            -item["relative_score_ratio"],
            item["rank"],
        )
    )
    ranked_candidate_ranks = [item["rank"] for item in candidates]
    affected_ranks = sorted(ranked_candidate_ranks)
    strong_signal_candidates = [
        item
        for item in candidates
        if _STRONG_ROOT_SIGNALS.intersection(item["diagnostic_signals"])
    ]
    relative_score_leader = (
        max(
            candidates,
            key=lambda item: (
                item["relative_score_ratio"],
                item["root_cause_signal_score"],
                -item["rank"],
            ),
        )
        if candidates
        else None
    )
    top_relative_score_ratio = (
        relative_score_leader["relative_score_ratio"]
        if relative_score_leader
        else 0.0
    )

    if strong_signal_candidates:
        strongest_signal_score = max(
            item["root_cause_signal_score"]
            for item in strong_signal_candidates
        )
        primary_suspect_ranks = [
            item["rank"]
            for item in strong_signal_candidates
            if item["root_cause_signal_score"]
            >= strongest_signal_score * 0.80
        ]
        classification_basis = "multi_signal_root_cause_evidence"
        classification_basis_zh = "综合 collective 进度、状态缺失、参与记录和 Trace 完整性判定"
    elif len(candidates) == 1:
        primary_suspect_ranks = affected_ranks[:1]
        classification_basis = "single_affected_rank"
        classification_basis_zh = "仅发现一个受影响 Rank"
    elif (
        candidates
        and candidates[0]["root_cause_signal_ratio"] >= 0.55
        and (
            len(candidates) == 1
            or candidates[0]["root_cause_signal_score"]
            >= candidates[1]["root_cause_signal_score"] * 1.50
        )
    ):
        primary_suspect_ranks = ranked_candidate_ranks[:1]
        classification_basis = "dominant_repeated_progress_anomaly"
        classification_basis_zh = "该 Rank 的重复进度异常显著强于其他受影响 Rank"
    elif top_relative_score_ratio >= 0.70:
        primary_suspect_ranks = [relative_score_leader["rank"]]
        classification_basis = "dominant_relative_evidence"
        classification_basis_zh = "该 Rank 的总体相对证据占比具有明显优势"
    else:
        primary_suspect_ranks = []
        classification_basis = "insufficient_causal_evidence"
        classification_basis_zh = "现有证据不足以区分根因 Rank 和并列受影响 Rank"

    candidate_by_rank = {item["rank"]: item for item in candidates}
    primary_suspect_set = set(primary_suspect_ranks)
    topology_groups = [
        group
        for candidate in candidates
        for group in _candidate_topology_groups(candidate)
    ]
    topology_available = bool(topology_groups)
    primary_has_strong_propagating_signal = any(
        _STRONG_ROOT_SIGNALS.intersection(
            candidate_by_rank[rank]["diagnostic_signals"]
        )
        for rank in primary_suspect_ranks
    )
    primary_has_ahead_signal = any(
        "unmatched_collective_ahead"
        in candidate_by_rank[rank]["diagnostic_signals"]
        for rank in primary_suspect_ranks
    )

    def is_downstream_candidate(rank: int) -> bool:
        candidate = candidate_by_rank[rank]
        anomalies = candidate["progress_anomalies"]
        if not anomalies or any(
            anomaly.get("direction") != "behind" for anomaly in anomalies
        ):
            return False
        if any(
            rank in group
            and any(primary in group for primary in primary_suspect_set)
            for group in topology_groups
        ):
            return primary_has_strong_propagating_signal
        # Old traces without topology can still show the classic unmatched-call
        # pattern: one rank is ahead while all other candidates are only behind.
        return not topology_available and primary_has_ahead_signal

    downstream_affected_ranks = (
        [
            rank
            for rank in affected_ranks
            if rank not in primary_suspect_set
            and is_downstream_candidate(rank)
        ]
        if primary_suspect_ranks
        else []
    )
    classified_ranks = primary_suspect_set | set(downstream_affected_ranks)
    unresolved_affected_ranks = [
        rank for rank in affected_ranks if rank not in classified_ranks
    ]
    role_by_rank = {
        rank: (
            ("primary_suspect", "首要故障嫌疑")
            if rank in primary_suspect_set
            else ("downstream_affected", "下游受影响")
            if rank in downstream_affected_ranks
            else ("unresolved_affected", "受影响，因果关系待判定")
        )
        for rank in affected_ranks
    }
    for candidate in candidates:
        role, role_zh = role_by_rank[candidate["rank"]]
        candidate["classification_role"] = role
        candidate["classification_role_zh"] = role_zh
        if role == "primary_suspect":
            confidence_level = _relative_score_confidence_level(
                candidate["root_cause_signal_ratio"]
            )
            confidence_basis = "root_cause_signal_ratio"
            confidence_basis_zh = "根据根因诊断信号占比评定"
        elif role == "downstream_affected":
            confidence_level = "medium"
            confidence_basis = "causal_group_propagation"
            confidence_basis_zh = "根据通信组关联和纯落后传播模式评定"
        else:
            confidence_level = "low"
            confidence_basis = "causal_relationship_unresolved"
            confidence_basis_zh = "已确认受影响，但因果关系证据不足"
        candidate["confidence_level"] = confidence_level
        candidate["confidence_level_zh"] = {
            "high": "高",
            "medium": "中",
            "low": "低",
        }[confidence_level]
        candidate["confidence_basis"] = confidence_basis
        candidate["confidence_basis_zh"] = confidence_basis_zh
    recommendation = (
        "primary_suspect_identified_with_downstream_impact"
        if primary_suspect_ranks and downstream_affected_ranks
        else "primary_suspect_identified"
        if primary_suspect_ranks
        else "ambiguous_candidates_need_additional_evidence"
    )
    recommendation_zh = (
        "已识别首要故障嫌疑 Rank，并发现下游受影响 Rank"
        if primary_suspect_ranks and downstream_affected_ranks
        else "已识别首要故障嫌疑 Rank"
        if primary_suspect_ranks
        else "候选 Rank 存在歧义，需要更多证据"
    )

    conclusion = {
        "首要故障嫌疑 Rank": primary_suspect_ranks,
        "全部受影响 Rank": affected_ranks,
        "下游受影响 Rank": downstream_affected_ranks,
        "待进一步判定 Rank": unresolved_affected_ranks,
        "覆盖的故障信号": sorted(
            {
                signal_zh
                for candidate in candidates
                for signal_zh in candidate["diagnostic_signals_zh"]
            }
        ),
        "判定依据": classification_basis_zh,
        "分析建议": recommendation_zh,
    }

    return {
        "candidate_ranks": candidates,
        "rank_list": ranked_candidate_ranks,
        "ranked_candidate_ranks": ranked_candidate_ranks,
        "primary_suspect_ranks": primary_suspect_ranks,
        "affected_ranks": affected_ranks,
        "downstream_affected_ranks": downstream_affected_ranks,
        "unresolved_affected_ranks": unresolved_affected_ranks,
        "classification_basis": classification_basis,
        "classification_basis_zh": classification_basis_zh,
        "recommendation": recommendation,
        "recommendation_zh": recommendation_zh,
        "结论": conclusion,
    }


def _structured_stdout_mode(cfg: Mapping[str, Any]) -> bool:
    return bool(cfg.get("emit_stdout")) and cfg.get("stdout_format") in {"json", "ranks"}


def _write_detailed_json_result(
    payload: Mapping[str, Any],
    output_path: Union[str, Path] = DEFAULT_JSON_RESULT_PATH,
) -> Dict[str, Any]:
    """Atomically persist full JSON and return the concise terminal payload."""
    result_path = Path(output_path)
    temporary_path = result_path.with_name(
        f".{result_path.name}.{os.getpid()}.tmp"
    )
    serialized = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    try:
        temporary_path.write_text(serialized, encoding="utf-8")
        os.replace(temporary_path, result_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    return {
        "结论": dict(payload.get("结论") or {}),
        "详细结果文件": str(result_path),
    }


@dataclass
class Collective:
    """
    A class that represents a collective operation.
    Each field corresponds to fields in the FR dump file
    """

    record_id: int
    file_id: str
    collective_seq_id: int
    p2p_seq_id: int
    pg_id: int
    op_id: int
    profiling_name: str
    state: str
    time_created_ns: int
    time_discovered_started_ns: int
    time_discovered_completed_ns: int
    process_group: List[str]
    input_sizes: List[List[int]]
    output_sizes: List[List[int]]
    input_dtypes: List[str]
    output_dtypes: List[str]


class CollectiveAnalyzer(NVRxAttribution):
    """
    This attribution module analyzes the PyTorch Flight Recorder (FR) traces dumped at timeout or exceptions.
    This does the following:

    1. Analyzes the PyTorch Flight Recorder (FR) traces dumped at timeout or exceptions.
    This analysis matches the collectives across ranks per process group and shows,
    which ranks are identified or missing.
    2. If application framework provides the descriptions of process groups,
    and provides global ordering of those process groups in the trace,
    we use that ordering to find the root cause of the interruption (hang or exception)
    - e.g.) Reduction should happen later than model parallel collectives.
            Default_pg should happen after all the collectives.
    3. Returns the missing ranks of the wavefront process group of the chained process groups in the step 2.

    This rank list means the ranks that need to be isolated to fix the interruption.
    The actual root cause of the interruption will be fulfilled by combining other attribution modules
    on the ranks in the list.
    """

    def __init__(self, args: Union[argparse.Namespace, Mapping[str, Any]]):
        """
        Initialize the CollectiveAnalyzer class.

        Args:
            args: Run parameters as a dict (preferred) or :class:`argparse.Namespace` (CLI).

        """
        self._reset_run_state()
        self._init_config = normalize_attribution_args(args)
        if not _structured_stdout_mode(self._init_config):
            eprint(f"args: {bounded_log_value(self._init_config)}")
        # initialize the NVRxAttribution class to run the attribution pipeline
        super().__init__(
            preprocess_input=self.preprocess_FR_dumps,
            attribution=self.collective_analysis,
            output_handler=self.print_output,
        )

    @property
    def init_config(self) -> Dict[str, Any]:
        """Constructor-normalized parameters (immutable snapshot for introspection/tests)."""
        return dict(self._init_config)

    def _reset_run_state(self) -> None:
        """Clear FR data derived from a single trace-analysis run."""
        # the data structures to store the collective operations
        self.collectives_by_file: Dict[str, List[Collective]] = {}
        # the data structure to store the process group status per rank
        self.pg_status: Dict[str, Dict[str, Dict[str, int]]] = {}
        # the data structure to store the collective operations grouped by process group
        self.collective_groups: Dict[Tuple[str, str], List[Collective]] = defaultdict(list)
        # the data structure to store the process group configurations
        self.pg_configs: Dict[str, Dict[int, int]] = {}
        self.collectives_to_order: Dict[Any, int] = {}
        self.trace_metadata: Dict[str, Dict[str, Any]] = {}
        self._progress_anomalies: Dict[
            Tuple[str, Tuple[int, ...], int], Dict[str, Any]
        ] = {}
        self.topology_payload: Optional[Dict[str, Any]] = None
        self.topology_source_path: Optional[str] = None
        self.topology_json_path: Optional[str] = None
        self.topology_groups: Dict[str, List[Tuple[int, ...]]] = {}
        self.topology_rank_index: Dict[
            str, Dict[int, Tuple[int, Tuple[int, ...]]]
        ] = {}
        self.topology_stats: Counter = Counter()
        self.topology_unmapped_descriptions = set()
        self.topology_missing_group_types = set()

    def _load_topology(self, topo_file: str) -> None:
        """Convert a topology text file to JSON and build rank-to-group indexes."""

        input_path = Path(topo_file).expanduser()
        output_path = input_path.with_suffix(".json")
        try:
            payload = convert_topology_file(input_path, output_path)
        except TopologyFormatError as exc:
            raise ValueError(f"Invalid topology file {input_path}: {exc}") from exc

        raw_groups = payload.get("groups")
        if not isinstance(raw_groups, Mapping):
            raise ValueError(f"Invalid topology JSON generated from {input_path}: missing groups")

        topology_groups: Dict[str, List[Tuple[int, ...]]] = {}
        topology_rank_index: Dict[
            str, Dict[int, Tuple[int, Tuple[int, ...]]]
        ] = {}
        for group_type, groups in raw_groups.items():
            normalized_groups = [
                tuple(sorted(int(rank) for rank in group))
                for group in groups
            ]
            topology_groups[str(group_type)] = normalized_groups
            rank_index: Dict[int, Tuple[int, Tuple[int, ...]]] = {}
            for group_index, group in enumerate(normalized_groups):
                for rank in group:
                    if rank in rank_index:
                        raise ValueError(
                            f"Topology group type {group_type!r} contains rank {rank} "
                            "in more than one group"
                        )
                    rank_index[rank] = (group_index, group)
            topology_rank_index[str(group_type)] = rank_index

        self.topology_payload = dict(payload)
        self.topology_source_path = str(input_path.resolve())
        self.topology_json_path = str(output_path.resolve())
        self.topology_groups = topology_groups
        self.topology_rank_index = topology_rank_index

    def _validate_topology_trace_ranks(self) -> None:
        """Reject a topology that cannot contain ranks present in the traces."""

        if self.topology_payload is None:
            return
        world_size = int(self.topology_payload["world_size"])
        trace_ranks = sorted(int(rank) for rank in self.collectives_by_file)
        extra_ranks = [rank for rank in trace_ranks if rank >= world_size]
        if extra_ranks:
            preview = ",".join(str(rank) for rank in extra_ranks[:12])
            raise ValueError(
                f"Topology world_size={world_size} does not cover trace ranks: {preview}"
            )

    def _partition_collectives_by_topology(
        self, key: Tuple[Any, str, int], collectives: List[Collective]
    ) -> List[Tuple[List[Collective], Optional[Dict[str, Any]]]]:
        """Split a numeric PG bucket using authoritative model-parallel groups."""

        if self.topology_payload is None:
            return [(collectives, None)]

        _, pg_desc, _ = key
        group_type = _topology_group_type(pg_desc)
        if group_type is None:
            self.topology_unmapped_descriptions.add(_process_group_type(pg_desc))
            self.topology_stats["fallback_collective_groups"] += 1
            return [(collectives, None)]
        if group_type not in self.topology_rank_index:
            self.topology_missing_group_types.add(group_type)
            self.topology_stats["fallback_collective_groups"] += 1
            return [(collectives, None)]

        rank_index = self.topology_rank_index[group_type]
        buckets: Dict[int, List[Collective]] = defaultdict(list)
        expected_by_index: Dict[int, Tuple[int, ...]] = {}
        for collective in collectives:
            try:
                rank = int(collective.file_id)
            except (TypeError, ValueError):
                self.topology_stats["fallback_collective_groups"] += 1
                return [(collectives, None)]
            match = rank_index.get(rank)
            if match is None:
                self.topology_stats["fallback_collective_groups"] += 1
                return [(collectives, None)]
            group_index, expected_ranks = match
            buckets[group_index].append(collective)
            expected_by_index[group_index] = expected_ranks

        if len(buckets) > 1:
            self.topology_stats["split_numeric_pg_groups"] += 1
        partitions = []
        for group_index in sorted(buckets):
            expected_ranks = expected_by_index[group_index]
            partitions.append(
                (
                    buckets[group_index],
                    {
                        "group_type": group_type,
                        "group_index": group_index,
                        "expected_ranks": expected_ranks,
                    },
                )
            )
        self.topology_stats["matched_collective_groups"] += len(partitions)
        return partitions

    def _build_topology_summary(self) -> Dict[str, Any]:
        if self.topology_payload is None:
            return {}
        return {
            "enabled": True,
            "source_file": self.topology_source_path,
            "json_file": self.topology_json_path,
            "world_size": int(self.topology_payload["world_size"]),
            "group_counts": {
                group_type: len(groups)
                for group_type, groups in sorted(self.topology_groups.items())
            },
            "matched_collective_groups": int(
                self.topology_stats["matched_collective_groups"]
            ),
            "split_numeric_pg_groups": int(
                self.topology_stats["split_numeric_pg_groups"]
            ),
            "fallback_collective_groups": int(
                self.topology_stats["fallback_collective_groups"]
            ),
            "unmapped_process_group_descriptions": sorted(
                self.topology_unmapped_descriptions
            ),
            "missing_topology_group_types": sorted(
                self.topology_missing_group_types
            ),
        }

    def _build_trace_summary(self) -> Dict[str, Any]:
        """Return compact file-level diagnostics without treating them as root cause."""

        entry_count_by_rank = {
            str(rank): int(metadata.get("entry_count", 0))
            for rank, metadata in sorted(
                self.trace_metadata.items(), key=lambda item: _rank_sort_key(item[0])
            )
        }
        count_frequency = Counter(entry_count_by_rank.values())
        entry_count_mode = None
        entry_count_mode_support = 0
        if count_frequency:
            mode_value, mode_support = count_frequency.most_common(1)[0]
            entry_count_mode_support = mode_support
            if len(count_frequency) == 1 or mode_support > len(entry_count_by_rank) / 2:
                entry_count_mode = mode_value

        entry_count_outlier_ranks = []
        if entry_count_mode is not None:
            entry_count_outlier_ranks = sorted(
                int(rank)
                for rank, count in entry_count_by_rank.items()
                if count != entry_count_mode
            )

        state_counts = Counter()
        operation_counts = Counter()
        trace_versions = set()
        comm_lib_versions = set()
        scheduled_collective_count = 0
        for metadata in self.trace_metadata.values():
            state_counts.update(metadata.get("state_counts", {}))
            operation_counts.update(metadata.get("operation_counts", {}))
            scheduled_collective_count += int(
                metadata.get("scheduled_collective_count", 0)
            )
            if metadata.get("version") is not None:
                trace_versions.add(str(metadata["version"]))
            if metadata.get("comm_lib_version") is not None:
                comm_lib_versions.add(str(metadata["comm_lib_version"]))

        return {
            "rank_count": len(entry_count_by_rank),
            "total_entry_count": sum(entry_count_by_rank.values()),
            "scheduled_collective_count": scheduled_collective_count,
            "entry_count_by_rank": entry_count_by_rank,
            "entry_count_mode": entry_count_mode,
            "entry_count_mode_support": entry_count_mode_support,
            "entry_count_outlier_ranks": entry_count_outlier_ranks,
            "state_counts": dict(sorted(state_counts.items())),
            "operation_counts": dict(sorted(operation_counts.items())),
            "trace_versions": sorted(trace_versions),
            "comm_lib_versions": sorted(comm_lib_versions),
        }

    """
    Routines registered for the attribution pipeline
    """

    # output handler to print the attribution results
    async def print_output(self, attribution_result: Optional[str]):
        text = attribution_result or ""
        hanging_ranks_list = _extract_missing_ranks_from_table(text)
        hanging_ranks = f"hanging ranks: {hanging_ranks_list}"
        progress_anomalies = sorted(
            self._progress_anomalies.values(),
            key=lambda item: (_rank_sort_key(item["pgid"]), item["rank"]),
        )
        payload = {
            "analysis_text": text,
            "hanging_ranks": hanging_ranks,
            "hanging_rank_list": hanging_ranks_list,
            "trace_summary": self._build_trace_summary(),
            "progress_anomalies": progress_anomalies,
        }
        if self.topology_payload is not None:
            payload["topology_analysis"] = self._build_topology_summary()
        assessment_with_conclusion = build_rank_candidates(payload)
        conclusion = assessment_with_conclusion["结论"]
        rank_assessment = {
            key: value
            for key, value in assessment_with_conclusion.items()
            if key != "结论"
        }
        payload["rank_assessment"] = rank_assessment
        payload["candidate_rank_list"] = rank_assessment["rank_list"]
        payload["primary_suspect_ranks"] = rank_assessment[
            "primary_suspect_ranks"
        ]
        payload["affected_ranks"] = rank_assessment["affected_ranks"]
        payload["downstream_affected_ranks"] = rank_assessment[
            "downstream_affected_ranks"
        ]
        payload["unresolved_affected_ranks"] = rank_assessment[
            "unresolved_affected_ranks"
        ]
        payload["结论"] = conclusion
        # Dict form preserves collective table text for MCP clients and FRAnalysisResult parity.
        return (
            payload,
            AttributionState.CONTINUE,
        )

    # preprocess input to analyze the collective operations
    async def preprocess_FR_dumps(self) -> str:
        """
        Analyzes the collective operations across multiple JSON files.

        This method performs the following steps:
        - Processes all input paths to collect collective data
        - Prints the process group configurations
        - Analyzes the collective operations
        - Prints the analysis output

        ``fr_path`` may be a directory (scan with ``pattern``), a single existing dump file, or a
        **path prefix** as in ``TORCH_FR_DUMP_TEMP_FILE=/tmp/checkpoints/_dump_`` (rank files are
        ``_dump_0``, ``_dump_1``, …). Prefix mode uses ``glob.glob(prefix + "*")``, matching
        :func:`fr_support._fr_traces_exist_for_prefix`.
        """
        self._reset_run_state()
        cfg = effective_run_or_init_config(self._init_config)
        logger.info("FR args: %s", bounded_log_value(cfg))
        topo_file = cfg.get("topo_file")
        if topo_file:
            self._load_topology(str(topo_file))
            logger.info(
                "Topology loaded: source=%s, json=%s",
                self.topology_source_path,
                self.topology_json_path,
            )
        file_paths = [cfg["fr_path"]]
        pattern = cfg.get("pattern", "_dump_*")
        logger.info(
            "file_paths: %s, pattern: %s",
            file_paths,
            pattern,
        )
        processed_files = 0
        # Process all input paths
        # read files from file_paths and prepare data structure for collective analysis
        for path in file_paths:
            logger.info(f"path: {path}")
            if os.path.isdir(path):
                json_files = glob.glob(os.path.join(path, pattern))
            elif os.path.isfile(path):
                json_files = glob.glob(path)
            else:
                # Path prefix (not a directory / not an existing file): match _dump_<rank> siblings.
                json_files = glob.glob(path + "*")
            logger.info("json_files: %s", bounded_log_value(json_files))
            json_files.sort()
            for filepath in json_files:
                if cfg.get("verbose"):
                    logger.info(f"Processing {filepath}...")
                if self.process_file(filepath):
                    processed_files += 1
            self.collective_groups = self.group_collectives_by_windows()

            def build_collectives_to_order():
                """
                Collectives to order.
                """
                order_map = {}
                idx = 0
                for key, collectives in self.collective_groups.items():
                    order_map[key] = idx
                    idx += 1
                return order_map

            self.collectives_to_order = build_collectives_to_order()
            logger.info("collective_groups: %s", bounded_log_value(self.collective_groups.keys()))
            logger.info("collectives_to_order: %s", bounded_log_value(self.collectives_to_order))
            if cfg.get("verbose"):
                self.print_pg_configs(verbose=bool(cfg.get("verbose")))

        if processed_files == 0:
            raise ValueError(f"No files at {file_paths} were processed successfully.")

        self._validate_topology_trace_ranks()
        logger.info(f"\nSuccessfully processed {processed_files} files.")
        # analyze collectives to find process groups with missing and completed ranks
        completed_pg, missing_pg = self.analyze_matches(verbose=bool(cfg.get("verbose")))

        # Print the analysis output
        original_level = logger.level
        if logger.getEffectiveLevel() > logging.INFO:
            logger.setLevel(logging.INFO)
        try:
            with capture_logs(logger.name) as output:

                def print_ranks_in_pgs(pg_indices, pg_dict, missing_or_completed="Missing"):
                    logger.info(
                        f"{'PGID':<6} | {'Process Group Desc':<25} | {'Op Type':<10} | {'Size':<8} \
                            | {'Dtype':<8} | {missing_or_completed} Ranks"
                    )
                    for pg_idx in sorted(pg_indices, key=_pg_result_sort_key):
                        for entry in pg_dict[pg_idx]:
                            op_type = str(entry[2])
                            if "nccl:send" in op_type or "nccl:recv" in op_type:
                                continue
                            if missing_or_completed == "Missing":
                                ranks_to_print = entry[7]
                            else:
                                ranks_to_print = entry[6]
                            logger.info(
                                f"{entry[0]:<6} | {entry[1]:<25} | {entry[2]:<10} | {entry[3]:<8} \
                                    | {entry[4]:<8} | {ranks_to_print}"
                            )

                if missing_pg:
                    logger.debug(
                        "missing_pg_indices: %s",
                        sorted(missing_pg, key=_pg_result_sort_key),
                    )
                    print_ranks_in_pgs(missing_pg.keys(), missing_pg, "Missing")
            analysis_output = output.getvalue()
        finally:
            logger.setLevel(original_level)
        return analysis_output

    async def collective_analysis(self, analysis_output: str) -> str:
        """Pass deterministic collective analysis to the output formatter."""
        return analysis_output

    """
    Helper functions to define steps for the registered attribution steps above
    """

    def group_collectives_by_windows(self) -> Dict:
        """
        Group the collectives by windows.
        """
        # Track the current index for each rank's collective list
        rank_indices = {rank_id: 0 for rank_id in self.collectives_by_file.keys()}

        # Track how many times each process group has been fully processed (window/phase counter)
        pg_window_counter = defaultdict(int)

        # Track which ranks have participated in the current window for each PG
        # Key: (pg, window_idx), Value: set of rank_ids that have processed this PG in this window
        pg_window_participants = defaultdict(set)

        # Track which PGs were active (had at least one rank working on them) in the last iteration
        # This helps us detect when we've completely left a PG and come back to it
        pgs_with_active_ranks_last_iter = set()

        # Result structure: maps (process_group, sub_group, window_index) to list of collectives
        matched_groups = defaultdict(list)

        # Keep processing until all ranks have processed all their collectives
        while any(
            rank_indices[rank_id] < len(self.collectives_by_file[rank_id])
            for rank_id in rank_indices.keys()
        ):

            # Get the current process group type for each rank that hasn't finished
            current_pg_types = {}
            for rank_id, idx in rank_indices.items():
                if idx < len(self.collectives_by_file[rank_id]):
                    collective = self.collectives_by_file[rank_id][idx]
                    # Use (process_group[0], process_group[1]) as the key
                    pg_key = (collective.process_group[0], collective.process_group[1])
                    current_pg_types[rank_id] = pg_key

            if not current_pg_types:
                break

            # Find the most common process group type among active ranks
            # This represents the "wavefront" - the PG type most ranks are working on
            pg_counter = Counter(current_pg_types.values())
            current_pg, _ = pg_counter.most_common(1)[0]

            # Determine current window for this PG
            window_idx = pg_window_counter[current_pg]
            pg_window_key = (current_pg, window_idx)

            # Check if we should create a new window for this PG
            # This happens when a significantly different set of ranks arrives
            ranks_with_current_pg = set(
                rid for rid, pg in current_pg_types.items() if pg == current_pg
            )
            already_participated = pg_window_participants[pg_window_key] & ranks_with_current_pg
            previous_participants = pg_window_participants[pg_window_key]

            has_previous_participants = len(previous_participants) > 0
            # TODO: This heuristic is imprecise. collective_seq_id is not a reliable signal
            # because ranks diverge in seq_id when they participate in p2p operations, making
            # cross-rank seq_id comparison error-prone. A more principled window-split criterion
            # is needed. For now, a fixed threshold of 2 is used as a best-effort approximation.
            has_significant_new_ranks = len(ranks_with_current_pg - previous_participants) >= 2

            # Create new window if:
            # 1. Some ranks have already participated (same ranks coming back), OR
            # 2. We have previous participants and mostly/completely new ranks (different batch)
            should_create_new_window = False

            if current_pg not in pgs_with_active_ranks_last_iter:
                # PG was inactive - check if we need a new window
                if already_participated or (
                    has_previous_participants and has_significant_new_ranks
                ):
                    should_create_new_window = True

            if should_create_new_window:
                # We're starting a new window/phase
                pg_window_counter[current_pg] += 1
                window_idx = pg_window_counter[current_pg]
                pg_window_key = (current_pg, window_idx)

            # Create key with window index to separate different phases
            key_with_window = (current_pg[0], current_pg[1], window_idx)

            # Collect all consecutive collectives for this PG from ranks that are at this PG
            # Keep processing until no more ranks have this PG as their next collective
            has_matches = True
            while has_matches:
                has_matches = False
                for rank_id in list(rank_indices.keys()):
                    idx = rank_indices[rank_id]
                    if idx < len(self.collectives_by_file[rank_id]):
                        collective = self.collectives_by_file[rank_id][idx]
                        pg_key = (collective.process_group[0], collective.process_group[1])

                        # If this rank's next collective matches the current PG, process it
                        if pg_key == current_pg:
                            matched_groups[key_with_window].append(collective)
                            rank_indices[rank_id] += 1
                            has_matches = True
                            # Track that this rank participated in this window
                            pg_window_participants[pg_window_key].add(rank_id)

            # Update the set of PGs that have active ranks for the next iteration
            # After processing, check which PGs still have ranks waiting on them
            pgs_with_active_ranks = set()
            for rank_id, idx in rank_indices.items():
                if idx < len(self.collectives_by_file[rank_id]):
                    collective = self.collectives_by_file[rank_id][idx]
                    pg_key = (collective.process_group[0], collective.process_group[1])
                    pgs_with_active_ranks.add(pg_key)

            pgs_with_active_ranks_last_iter = pgs_with_active_ranks

            # Note: Ranks that have moved to a different PG type "pop up" and wait
            # They'll be processed in the next iteration when we select their PG as the wavefront

        return matched_groups

    def analyze_matches(self, verbose: bool = False) -> Tuple[Dict, Dict]:
        """
        Analyze matching collectives across files, grouped by process group type and ordered by sub group.
        Dynamically identifies group types from the data.

        Args:
            verbose (bool): Whether to include more detailed analysis in the output
        """
        logger.info("\n=== Collective Operations Analysis ===\n")

        if verbose:
            logger.info("Files processed:")
            for rank_id in sorted(self.collectives_by_file.keys()):
                count = len(self.collectives_by_file[rank_id])
                logger.info(f"  {rank_id}: {count} collectives")
        logger.info("")

        def match_collectives():
            # Extract unique sub-group types from the data
            group_types = set()
            for key in self.collective_groups.keys():
                if (
                    len(self.collective_groups[key]) > 1
                ):  # Only consider groups with multiple collectives
                    process_group, sub_group, window_idx = key
                    if sub_group:  # Ensure sub_group is not empty
                        group_types.add(sub_group)

            # Convert to sorted list
            group_types = sorted(group_types)

            # If no group types were found, use default ones
            if not group_types:
                group_types = ["TENSOR_MODEL", "PIPELINE_MODEL", "DATA_PARALLEL"]
                logger.info("No sub-group types found in data. Using default group types.")
            else:
                logger.info(f"Found group types: {', '.join(group_types)}")

            # Headers for this section
            headers = [
                ("Process Group", 15),
                ("PG Desc", 30),
                ("Op Type", 20),
                ("Size", 15),
                ("Dtype", 10),
                ("Total NRanks", 20),
                ("Identified Ranks", 40),
                ("Missing Ranks", 40),
            ]

            header_line = " ".join(f"{name:>{width}}" for name, width in headers)
            logger.info(header_line)
            logger.info("-" * len(header_line))

            # Process each category in the order of group_types
            completed_pg = defaultdict(list)
            missing_pg = defaultdict(list)

            def matching_collectives_per_process_group(
                collective_group, topology_match: Optional[Dict[str, Any]] = None
            ):
                logger.debug(f"collective_group: {collective_group}")
                key, collectives = collective_group
                process_group, sub_group, window_idx = key
                max_completed_collective_seq_id = -1
                max_enqueued_collective_seq_id = -1
                local_pg_id_counts = defaultdict(Counter)
                for c in collectives:
                    rank_id = c.file_id
                    logger.debug(
                        f"rank_id: {rank_id}, c.pg_id: {c.pg_id}, c.file_id: {c.file_id}, c.collective_seq_id: {c.collective_seq_id}, process_group: {process_group},"
                        f"c.state: {c.state}"
                    )
                    local_pg_id_counts[rank_id][c.pg_id] += 1
                    pg_status = self.pg_status.get(rank_id, {}).get(str(c.pg_id), {})
                    last_completed = pg_status.get('last_completed_collective')
                    last_enqueued = pg_status.get('last_enqueued_collective')
                    if (
                        last_completed is not None
                        and last_completed >= max_completed_collective_seq_id
                    ):
                        max_completed_collective_seq_id = last_completed
                    if (
                        last_enqueued is not None
                        and last_enqueued >= max_enqueued_collective_seq_id
                    ):
                        max_enqueued_collective_seq_id = last_enqueued

                logger.debug(f"max_completed_collective_seq_id: {max_completed_collective_seq_id}")
                logger.debug(f"max_enqueued_collective_seq_id: {max_enqueued_collective_seq_id}")
                local_pg_map = {
                    rank: counts.most_common(1)[0][0]
                    for rank, counts in local_pg_id_counts.items()
                }
                representative_local_pg_id = (
                    Counter(local_pg_map.values()).most_common(1)[0][0]
                    if local_pg_map
                    else None
                )
                # Ranks holding entries earlier than max_completed_collective_seq_id -> ranks failing to complete expected collectives
                rank_counts = defaultdict(list)
                for c in collectives:
                    if c.state != 'scheduled':
                        continue
                    rank_counts['appeared'].append(c.file_id)
                appeared_rank_counts = Counter(rank_counts['appeared'])

                # Get a list of unique ranks that appeared in the trace
                unique_ranks = sorted(map(int, appeared_rank_counts.keys()))

                # Find the most common operation type
                op_names = [
                    c.profiling_name
                    for c in collectives
                    if get_correct_seq_id(c) > max_completed_collective_seq_id
                ]
                op_counts = Counter(op_names)
                logger.debug(f"process_group: {process_group}, op_counts: {op_counts}")
                op_type = op_counts.most_common(1)[0][0] if op_counts else "Unknown"

                def pair_send_recv_operations():
                    # Pair corresponding send/recv operations
                    send_ops = {}
                    recv_ops = {}
                    other_ops = {}

                    for op, count in op_counts.items():
                        if "nccl:send" in op:
                            parts = op.split()
                            if len(parts) > 1:
                                src, dst = parts[1].split("->")  # e.g., "0->1"
                                send_ops[(src, dst)] = count
                        elif "nccl:recv" in op:
                            parts = op.split()
                            if len(parts) > 1:
                                dst, src = parts[1].split("<-")  # e.g., "0<-1"
                                recv_ops[(dst, src)] = count
                        else:
                            other_ops[op] = count
                    # Combine all unique src-dst pairs
                    all_pairs = set(send_ops.keys()) | set(
                        (dst, src) for dst, src in recv_ops.keys()
                    )
                    return all_pairs, send_ops, recv_ops, other_ops

                all_pairs, send_ops, recv_ops, other_ops = pair_send_recv_operations()
                # expected ranks for this process group
                if topology_match is not None:
                    global_ranks = list(topology_match["expected_ranks"])
                else:
                    global_ranks = list(sorted(self.pg_configs[process_group]['ranks']))
                total_unique_ranks = len(global_ranks)
                missing_ranks = set()
                if "nccl:send" in op_type or "nccl:recv" in op_type:
                    for src, dst in sorted(all_pairs):
                        send_count = send_ops.get((src, dst), 0)
                        recv_count = recv_ops.get((dst, src), 0)
                        logger.debug(
                            f"src: {src}, dst: {dst}, send_count: {send_count}, recv_count: {recv_count}"
                        )
                        # Only mark as missing if the rank is truly not present in the trace
                        # Check if recv > send AND the sender is not in unique_ranks
                        if recv_count > send_count:
                            missing_global_rank = global_ranks[int(src)]
                            # Only add to missing if this rank is truly not present
                            if missing_global_rank not in unique_ranks:
                                missing_ranks = missing_ranks | set([missing_global_rank])
                        # Similarly for send > recv
                        if send_count > recv_count:
                            missing_global_rank = global_ranks[int(dst)]
                            if missing_global_rank not in unique_ranks:
                                missing_ranks = missing_ranks | set([missing_global_rank])
                else:
                    progress_by_rank = {}
                    for global_rank in global_ranks:
                        rank_key = str(global_rank)
                        local_pg_id = local_pg_map.get(
                            rank_key, representative_local_pg_id
                        )
                        if local_pg_id is None:
                            continue
                        status = self.pg_status.get(rank_key, {}).get(
                            str(local_pg_id), {}
                        )
                        if status.get('last_enqueued_collective') is not None:
                            progress_by_rank[global_rank] = status[
                                'last_enqueued_collective'
                            ]

                    progress_outliers, progress_summary = _find_progress_outliers(
                        global_ranks, progress_by_rank
                    )
                    if progress_summary["status_count"] > 0:
                        missing_ranks = set(progress_outliers)
                    else:
                        # Old dumps may not include pg_status. In that case only use
                        # direct trace participation as a best-effort fallback.
                        missing_ranks = set(global_ranks) - set(unique_ranks)

                    baseline = progress_summary["baseline"]
                    for outlier_rank in sorted(progress_outliers):
                        progress = progress_by_rank.get(outlier_rank)
                        if progress is None:
                            direction = "missing_status"
                        elif baseline is not None and progress > baseline:
                            direction = "ahead"
                        elif baseline is not None and progress < baseline:
                            direction = "behind"
                        else:
                            direction = "unknown"
                        anomaly = {
                            "rank": outlier_rank,
                            "pgid": str(process_group),
                            "pg_desc": sub_group,
                            "window": window_idx,
                            "local_pg_id": local_pg_map.get(
                                str(outlier_rank), representative_local_pg_id
                            ),
                            "last_enqueued": progress,
                            "baseline_enqueued": baseline,
                            "direction": direction,
                            "baseline_support": progress_summary[
                                "baseline_support"
                            ],
                            "status_count": progress_summary["status_count"],
                            "expected_rank_count": progress_summary[
                                "expected_rank_count"
                            ],
                            "strategy": progress_summary["strategy"],
                        }
                        if topology_match is not None:
                            anomaly["expected_rank_source"] = "topology"
                            anomaly["topology_group_type"] = topology_match[
                                "group_type"
                            ]
                            anomaly["topology_group_index"] = topology_match[
                                "group_index"
                            ]
                            anomaly["topology_expected_ranks"] = list(global_ranks)
                        anomaly_key = (
                            str(process_group),
                            tuple(global_ranks),
                            outlier_rank,
                        )
                        previous = self._progress_anomalies.get(anomaly_key)
                        previous_delta = (
                            abs(
                                int(previous["last_enqueued"])
                                - int(previous["baseline_enqueued"])
                            )
                            if previous
                            and previous.get("last_enqueued") is not None
                            and previous.get("baseline_enqueued") is not None
                            else -1
                        )
                        current_delta = (
                            abs(int(progress) - int(baseline))
                            if progress is not None and baseline is not None
                            else 0
                        )
                        if previous is None or current_delta > previous_delta:
                            self._progress_anomalies[anomaly_key] = anomaly

                if "nccl:send" in op_type or "nccl:recv" in op_type:
                    correct_unique_ranks = set(unique_ranks) - missing_ranks
                else:
                    correct_unique_ranks = set(global_ranks) - missing_ranks
                logger.debug(f"missing_ranks: {missing_ranks}")

                # Get size and dtype from the first collective
                size_str = (
                    'x'.join(str(x) for x in collectives[0].input_sizes[0])
                    if collectives[0].input_sizes
                    else "N/A"
                )
                dtype = collectives[0].input_dtypes[0] if collectives[0].input_dtypes else "N/A"

                row_data = [
                    (process_group, 15, ''),
                    (','.join(map(str, key[1:])), 30, ''),
                    (op_type, 20, ''),
                    (size_str, 15, ''),
                    (dtype, 10, ''),
                    (total_unique_ranks, 10, 'd'),
                    (','.join(map(str, correct_unique_ranks)), 40, ''),
                    (','.join(map(str, sorted(missing_ranks))), 40, ''),
                ]

                row = " ".join(f"{val:>{width}{fmt}}" for val, width, fmt in row_data)
                parsed_row = tuple(row_elem[0] for row_elem in row_data)
                result_group_key: Any = int(parsed_row[0])
                if topology_match is not None:
                    result_group_key = (
                        int(parsed_row[0]),
                        topology_match["group_type"],
                        topology_match["group_index"],
                        tuple(global_ranks),
                    )
                if len(missing_ranks) <= 0:
                    completed_pg[result_group_key].append(parsed_row)
                    return
                else:
                    missing_pg[result_group_key].append(parsed_row)
                    logger.info(row)

                # Print detailed rank count distribution
                if verbose:
                    logger.info(f"  Rank count distribution for {process_group}:")
                    for rank, count in sorted(appeared_rank_counts.items()):
                        logger.info(f"    Rank {rank}: {count} occurrences")

                # Print operation type distribution with paired send/recv analysis
                logger.info("  Operation type distribution:")
                # Print paired send/recv operations
                logger.info("    Send/Receive pairs (src->dst):")

                # Print each pair with send and recv counts
                for src, dst in all_pairs:
                    send_count = send_ops.get((src, dst), 0)
                    recv_count = recv_ops.get((dst, src), 0)

                    # Highlight imbalances
                    if send_count != recv_count:
                        imbalance = f" [IMBALANCE: {send_count-recv_count:+d}]"
                    else:
                        imbalance = ""

                    logger.info(
                        f"      {global_ranks[int(src)]}->{global_ranks[int(dst)]}: {send_count} sends, {recv_count} recvs{imbalance}"
                    )

                # Print other operations
                if other_ops:
                    logger.info("    Other operations:")
                    for op, count in sorted(other_ops.items(), key=lambda x: (-x[1], x[0])):
                        logger.info(f"      {op}: {count}")

            def get_correct_seq_id(collective):
                if (
                    "nccl:send" in collective.profiling_name
                    or "nccl:recv" in collective.profiling_name
                ):
                    return collective.p2p_seq_id
                else:
                    return collective.collective_seq_id

            for key, collective_group in self.collective_groups.items():
                logger.debug(f"key: {key}, collective_group: {collective_group}")
                for partition, topology_match in self._partition_collectives_by_topology(
                    key, collective_group
                ):
                    matching_collectives_per_process_group(
                        (key, partition), topology_match
                    )

            # Cross-window matching: if the same PG has missing ranks in different windows,
            # try to match them across windows
            pg_all_windows = defaultdict(
                list
            )  # pg_id -> list of (window_idx, identified_ranks, missing_ranks)

            for pg_id, entries in missing_pg.items():
                for entry in entries:
                    # entry format: (pg_id, pg_desc, op_type, size, dtype, total_nranks, identified_ranks, missing_ranks)
                    pg_desc = entry[1]  # e.g., "default_pg,0" or "default_pg,1"
                    identified_ranks_str = entry[6]
                    missing_ranks_str = entry[7]

                    identified_ranks = (
                        set(map(int, identified_ranks_str.split(',')))
                        if identified_ranks_str
                        else set()
                    )
                    missing_ranks = (
                        set(map(int, missing_ranks_str.split(','))) if missing_ranks_str else set()
                    )

                    window_idx = int(pg_desc.split(',')[-1]) if ',' in pg_desc else 0
                    pg_all_windows[pg_id].append(
                        (window_idx, identified_ranks, missing_ranks, entry)
                    )

            # For each PG with multiple windows, try to match missing ranks across windows
            merged_missing_pg = defaultdict(list)
            for pg_id, windows_data in pg_all_windows.items():
                if len(windows_data) <= 1:
                    # Single window, keep as-is
                    for _, _, _, entry in windows_data:
                        merged_missing_pg[pg_id].append(entry)
                    continue

                # Multiple windows for this PG - try to match across windows
                all_identified = set()
                all_missing = set()
                representative_entry = windows_data[0][3]  # Use first window's entry as template

                for window_idx, identified, missing, entry in windows_data:
                    all_identified.update(identified)
                    all_missing.update(missing)

                # Ranks that are identified in at least one window should not be considered missing
                truly_missing = all_missing - all_identified
                raw_pg_id = pg_id[0] if isinstance(pg_id, tuple) else pg_id
                expected_signature = (
                    tuple(pg_id[3])
                    if isinstance(pg_id, tuple) and len(pg_id) > 3
                    else None
                )
                progress_outliers = {
                    rank
                    for (
                        progress_pg_id,
                        progress_expected_ranks,
                        rank,
                    ), _ in self._progress_anomalies.items()
                    if progress_pg_id == str(raw_pg_id)
                    and (
                        expected_signature is None
                        or progress_expected_ranks == expected_signature
                    )
                }
                truly_missing.update(progress_outliers)
                all_identified.difference_update(truly_missing)

                if truly_missing:
                    # Create merged entry with truly missing ranks
                    merged_entry = list(representative_entry)
                    merged_entry[6] = ','.join(map(str, sorted(all_identified)))
                    merged_entry[7] = ','.join(map(str, sorted(truly_missing)))
                    merged_entry = tuple(merged_entry)
                    merged_missing_pg[pg_id].append(merged_entry)
                else:
                    # No truly missing ranks after cross-window matching
                    # Don't add to merged_missing_pg (it's complete now)
                    pass

            return completed_pg, merged_missing_pg

        completed_pg, missing_pg = match_collectives()
        return completed_pg, missing_pg

    def group_pgs(self, pgs: Dict[str, List]) -> Dict[int, List[int]]:
        """
        Groups process groups by finding longest paths in the graph when their ranks overlap.
        Each process group proceeds to neighbors with equal or higher index of process group type.
        pgs are connected if they share any ranks.

        If there are multiple overlapped paths, the longest path is selected.

        Args:
            pgs: Dictionary where keys are group types and values are lists of process group data

        Returns:
            Dictionary with grouped process groups, where each group contains PGs in the longest path
        """

        grouped_pgs = defaultdict(set)
        pg_rank_mapping = {}
        # Build adjacency graph - PGs are connected if they share any ranks
        graph = defaultdict(set)
        for group_type, pg_list in pgs.items():
            if not pg_list:
                continue

            # Extract rank information from each process group
            # Each pg_data is a list of tuples (row_data), and we need to find the ranks
            pg_data_list = []  # Keep track of original pg_data objects

            for pg_data in pg_list:
                if len(pg_data) > 6:
                    logger.debug(f"pg_data: {pg_data}")
                    ranks_str = pg_data[6].split(',') + pg_data[7].split(
                        ','
                    )  # identified, missing ranks
                    ranks_str = [int(rank) for rank in ranks_str if rank != '']
                    if ranks_str:
                        logger.debug(f"ranks: {ranks_str}")
                        pg_rank_mapping[(int)(pg_data[0])] = ranks_str  # Use index as key
                        pg_data_list.append(pg_data)

            if not pg_rank_mapping:
                continue
            logger.debug(f"pg_rank_mapping: {pg_rank_mapping}")

        pg_indices = list(map(int, pg_rank_mapping.keys()))
        for i, pg1_idx in enumerate(pg_indices):
            graph[pg1_idx].add(pg1_idx)
            for j, pg2_idx in enumerate(pg_indices):
                if i != j:
                    pg2_ranks = pg_rank_mapping[pg2_idx]
                    # Check if PGs share any ranks
                    if set(pg_rank_mapping[pg1_idx]) & set(
                        pg_rank_mapping[pg2_idx]
                    ):  # Set intersection
                        graph[pg1_idx].add(pg2_idx)
        logger.debug(f"graph: {graph}")
        # Find longest paths in the graph
        visited = set()
        group_id = 0

        def dfs(node, current_path, visited_in_path, visited_keys):
            current_key = pgs[node][0][1]
            logger.debug(f"current_key: {current_key}, visited_keys: {visited_keys}")
            if node in visited_in_path or current_key in visited_keys:
                logger.debug(f"visited_in_path: {visited_in_path}, visited_keys: {visited_keys}")
                logger.debug(f"Cycle detected, returning current path: {current_path}")
                return [current_path]  # Cycle detected, return current path

            visited_in_path.add(node)
            current_path.append(node)
            visited_keys.add(current_key)
            if not graph[node] or all(neighbor in visited_in_path for neighbor in graph[node]):
                # Leaf node or all neighbors visited, return this path
                if current_key in visited_keys:
                    visited_keys.remove(current_key)
                return [current_path.copy()]

            def find_type_val(key: Tuple[str, str]) -> int:
                """
                Find the order index of a given process group type
                """
                type_name = key[0]
                last_comma = key[1].rfind(',')
                type_val = key[1][:last_comma]
                per_pg_seq = int(key[1][last_comma + 1 :])
                parsed_key = (type_name, type_val, per_pg_seq)
                logger.debug(
                    f"key: {parsed_key}, self.collectives_to_order: {self.collectives_to_order[parsed_key]}"
                )
                return self.collectives_to_order.get(parsed_key, -1)

            all_paths = []
            for neighbor in graph[node]:
                if neighbor not in visited_in_path:
                    tail_key = pgs[node][0][:2]
                    tail_pg_type = find_type_val(tail_key)
                    new_node_key = pgs[neighbor][0][:2]
                    new_node_pg_type = find_type_val(new_node_key)
                    logger.debug(f"current_path: {current_path}, neighbor: {neighbor}")
                    logger.debug(
                        f"tail_pg_type: {tail_pg_type}, new_node_pg_type: {new_node_pg_type}"
                    )
                    if tail_pg_type <= new_node_pg_type:
                        paths_from_neighbor = dfs(
                            neighbor, current_path.copy(), visited_in_path.copy(), visited_keys
                        )
                        all_paths.extend(paths_from_neighbor)
                    else:
                        all_paths.append(current_path.copy())
            visited_keys.remove(current_key)
            return all_paths

        def find_valid_paths(graph, start_node, visited):
            """
            Find all longest paths starting from a given node using DFS.
            Returns a list of paths, where each path is a list of nodes.
            """
            return dfs(start_node, [], set(), set())

        sorted_pg_indices = sorted(pg_indices, key=lambda x: len(pg_rank_mapping[x]), reverse=True)
        logger.debug(f"sorted_pg_indices: {sorted_pg_indices}")

        for pg_idx in sorted_pg_indices:
            if pg_idx in visited:
                continue

            # Find all longest paths starting from this PG
            all_paths = find_valid_paths(graph, pg_idx, visited)
            if not all_paths:
                logger.info(f"No paths from PG {pg_idx}. Skipping this PG")
                continue
            else:
                seen_paths = set()
                for path in all_paths:
                    # Convert path to tuple for hashing and duplicate detection
                    path_tuple = tuple(path)
                    if path_tuple not in seen_paths:
                        seen_paths.add(path_tuple)
                logger.debug(f"all_paths: {all_paths}")
                logger.debug(f"Filtered(excl. dup.) paths starting from {pg_idx}: {seen_paths}")
                for path in seen_paths:
                    for node in path:
                        visited.add(node)
                    grouped_pgs[group_id] = list(path)
                    group_id += 1

        logger.debug(f"grouped_pgs: {grouped_pgs}")
        # Remove paths that are subsets of other paths
        unique_paths = []
        path_tuples = list(grouped_pgs.values())
        logger.debug(f"path_tuples: {path_tuples}")
        for i, path1 in enumerate(path_tuples):
            is_subset = False
            for j, path2 in enumerate(path_tuples):
                if i != j:
                    if set(path1) < (set(path2)):
                        logger.debug(f"path1: {path1} is a subset of path2: {path2}")
                        is_subset = True
                        break
                    elif set(path1) == (set(path2)):
                        if path1 not in unique_paths and path2 not in unique_paths:
                            unique_paths.append(path1)
                        is_subset = True
                        break
            if not is_subset:
                logger.debug(f"path1: {path1}")
                unique_paths.append(path1)
        grouped_pgs = {i: path for i, path in enumerate(unique_paths)}
        logger.debug(f"unique_paths: {unique_paths}")
        return grouped_pgs

    def process_file(self, filepath: str) -> bool:
        """
        Process a single file to extract collective operations and other metadata
        """

        def load_trace_file(filename: str) -> Dict:
            if filename.lower().endswith('.json'):
                try:
                    with open(filename, 'r') as f:
                        return json.load(f)
                except (json.JSONDecodeError, FileNotFoundError):
                    raise ValueError(f"Error loading JSON file: {filename}")
            else:
                try:
                    with open(filename, 'rb') as f:
                        data = _RestrictedTraceUnpickler(f).load()
                        # Convert pickle data to JSON-compatible format
                        converted_data = json.loads(json.dumps(data))
                    if effective_run_or_init_config(self._init_config).get("debug"):
                        with open(filename + '.json', 'w') as f:
                            f.write(json.dumps(converted_data, indent=2))
                            f.write('\n')
                    return converted_data
                except (pickle.PickleError, FileNotFoundError, json.JSONDecodeError):
                    raise ValueError(f"Error loading pickle file: {filename}")

        def extract_collectives(data: Dict, file_id: str) -> List[Collective]:
            """
            Extract collective operations from the JSON data
            """
            collectives = []
            for entry in data['entries']:
                if 'collective_seq_id' in entry and entry['state'] == 'scheduled':
                    collective = Collective(
                        record_id=entry.get('record_id', -1),
                        file_id=file_id,
                        collective_seq_id=entry['collective_seq_id'],
                        p2p_seq_id=entry.get('p2p_seq_id', -1),
                        pg_id=entry['pg_id'],
                        op_id=entry['op_id'],
                        profiling_name=entry['profiling_name'],
                        time_created_ns=entry['time_created_ns'],
                        time_discovered_started_ns=entry.get(
                            'time_discovered_started_ns', entry['time_created_ns']
                        ),
                        time_discovered_completed_ns=entry.get(
                            'time_discovered_completed_ns', entry['time_created_ns']
                        ),
                        process_group=entry['process_group'],
                        state=entry['state'],
                        input_sizes=entry['input_sizes'],
                        output_sizes=entry['output_sizes'],
                        input_dtypes=entry['input_dtypes'],
                        output_dtypes=entry['output_dtypes'],
                    )
                    collectives.append(collective)
            return collectives

        try:
            file_id = Path(filepath).stem
            # Extract rank ID from the filename assuming it's the last part after an underscore
            rank_id = file_id.split('_')[-1]
            data = load_trace_file(filepath)

            # Extract pg_config from the JSON data
            if 'pg_config' in data:
                for group, mapping in data['pg_config'].items():

                    def is_int(s):
                        try:
                            int(s)
                            return True
                        except (TypeError, ValueError):
                            return False

                    raw_ranks = mapping.get('ranks', '')
                    if isinstance(raw_ranks, str):
                        ranks = [
                            i
                            for i in raw_ranks.strip('[]').split(',')
                            if is_int(i)
                        ]
                    elif isinstance(raw_ranks, (list, tuple, set)):
                        ranks = [i for i in raw_ranks if is_int(i)]
                    else:
                        ranks = []
                    if len(ranks) > 0:
                        mapping['ranks'] = set(map(int, ranks))
                    else:
                        mapping['ranks'] = set()
                    if group not in self.pg_configs:
                        self.pg_configs[group] = mapping
                    else:
                        self.pg_configs[group]['ranks'] = (
                            self.pg_configs[group]['ranks'] | mapping['ranks']
                        )
            collectives = extract_collectives(data, rank_id)
            self.pg_status[rank_id] = data['pg_status']
            self.collectives_by_file[rank_id] = collectives
            entries = data.get('entries', [])
            self.trace_metadata[rank_id] = {
                "entry_count": len(entries),
                "scheduled_collective_count": len(collectives),
                "state_counts": dict(
                    Counter(str(entry.get("state", "unknown")) for entry in entries)
                ),
                "operation_counts": dict(
                    Counter(
                        str(entry.get("profiling_name", "unknown"))
                        for entry in entries
                    )
                ),
                "version": data.get("version"),
                "comm_lib_version": data.get("comm_lib_version"),
            }

            return True
        except Exception as e:
            eprint(f"Error processing {filepath}: {str(e)}")
            return False

    def print_pg_configs(self, verbose: bool = False):
        """Print process group configurations in a more readable format."""
        eprint("\n=== Process Group Configurations ===\n")

        # Table header
        eprint(f"{'Group ID':<10} {'Description':<35} {'Ranks':<50}")
        eprint("-" * 95)
        # Sort by group ID numerically
        for group_id in sorted(self.pg_configs.keys(), key=lambda x: int(x)):
            group = self.pg_configs[group_id]
            ranks = str(group['ranks'])
            eprint(f"{group_id:<10} {group['desc']:<35} {ranks:<50}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze collective operations across JSON dump files.'
    )
    parser.add_argument(
        '--fr-path',
        type=str,
        required=True,
        help='Path to JSON files or directories containing JSON files',
    )
    parser.add_argument(
        '-p', '--pattern', default="_dump_*", help='File pattern to match (default: _dump_*)'
    )
    parser.add_argument(
        '--topo_file',
        '--topo-file',
        dest='topo_file',
        type=str,
        default=None,
        help=(
            'Optional model-parallel topology text file. When provided, '
            'topo_to_json.py generates a sibling JSON file and topology groups '
            'are used to disambiguate process-group membership.'
        ),
    )
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Convert the trace file to json file, if the trace is binary, for debugging',
    )
    parser.add_argument(
        '--emit-stdout',
        action='store_true',
        help='Print final FR summary table to stdout for machine consumers',
    )
    parser.add_argument(
        '--stdout-format',
        choices=('table', 'json', 'ranks'),
        default='table',
        help=(
            'Format used with --emit-stdout: table prints the legacy summary table, '
            'json writes the full result to /tmp/trace_analyzer_result.json and '
            'prints a concise conclusion, ranks prints only the JSON candidate rank list'
        ),
    )

    args = parser.parse_args()

    if _structured_stdout_mode(vars(args)):
        logger.setLevel(logging.WARNING)
        logger.propagate = False

    analyzer = CollectiveAnalyzer(args)
    result = analyzer.run_sync(args)

    if args.emit_stdout and isinstance(result, tuple) and result:
        payload = result[0]
        if isinstance(payload, dict):
            if args.stdout_format == "json":
                terminal_payload = _write_detailed_json_result(payload)
                print(
                    json.dumps(
                        terminal_payload,
                        ensure_ascii=False,
                        indent=2,
                    )
                )
            elif args.stdout_format == "ranks":
                print(json.dumps(payload.get("candidate_rank_list", [])))
            else:
                text = payload.get("analysis_text", "")
                if text:
                    print(text)
        elif payload:
            print(payload)


if __name__ == "__main__":
    _fr_cli_level = (
        logging.DEBUG if os.getenv('FR_DEBUG', '').lower() in ('1', 'true', 'yes') else logging.INFO
    )
    if not logging.root.handlers:
        logging.basicConfig(level=_fr_cli_level)
    else:
        logging.getLogger("trace_analyzer").setLevel(_fr_cli_level)
    main()
