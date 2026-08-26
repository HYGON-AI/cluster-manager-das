# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Megatron / hcu_megatron parallel rank grouping.

Ported from Megatron-LM ``megatron.core.parallel_state``:
  - ``generate_masked_orthogonal_rank_groups``
  - ``RankGenerator.get_ranks``

hcu_megatron default (``initialize.py``):
  order = ``tp-cp-ep-dp-pp`` unless ``--use-tp-pp-dp-mapping`` -> ``tp-cp-ep-pp-dp``.
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import ParallelTopology

# hcu_megatron / Megatron-LM defaults (cp=1, ep=1 for standard GPT training)
DEFAULT_PARALLEL_ORDER = "tp-cp-ep-dp-pp"
TP_PP_DP_MAPPING_ORDER = "tp-cp-ep-pp-dp"


@dataclass(frozen=True)
class ParallelLayout:
    """Megatron-style parallel dimensions for rank layout."""

    tensor_model_parallel_size: int = 1
    pipeline_model_parallel_size: int = 1
    data_parallel_size: int = 1
    context_parallel_size: int = 1
    expert_model_parallel_size: int = 1
    order: str = DEFAULT_PARALLEL_ORDER

    @property
    def world_size(self) -> int:
        return (
            self.tensor_model_parallel_size
            * self.pipeline_model_parallel_size
            * self.data_parallel_size
            * self.context_parallel_size
            * self.expert_model_parallel_size
        )


def generate_masked_orthogonal_rank_groups(
    world_size: int, parallel_size: list[int], mask: list[bool]
) -> list[list[int]]:
    """Megatron ``generate_masked_orthogonal_rank_groups`` (orthogonal TP/PP/DP/...)."""

    def prefix_product(values: list[int], init: int = 1) -> list[int]:
        result = [init]
        for value in values:
            init *= value
            result.append(init)
        return result

    def inner_product(a: list[int], b: list[int]) -> int:
        return sum(x * y for x, y in zip(a, b))

    def decompose(index: int, shape: list[int], stride: list[int] | None = None) -> list[int]:
        if stride is None:
            stride = prefix_product(shape)
        return [(index // step) % size for size, step in zip(shape, stride)]

    masked_shape = [size for size, enabled in zip(parallel_size, mask) if enabled]
    unmasked_shape = [size for size, enabled in zip(parallel_size, mask) if not enabled]

    global_stride = prefix_product(parallel_size)
    masked_stride = [step for step, enabled in zip(global_stride, mask) if enabled]
    unmasked_stride = [step for step, enabled in zip(global_stride, mask) if not enabled]

    group_size = prefix_product(masked_shape)[-1]
    num_groups = world_size // group_size

    groups: list[list[int]] = []
    for group_index in range(num_groups):
        decomposed_group_idx = decompose(group_index, unmasked_shape)
        ranks: list[int] = []
        for rank_in_group in range(group_size):
            decomposed_rank_idx = decompose(rank_in_group, masked_shape)
            ranks.append(
                inner_product(decomposed_rank_idx, masked_stride)
                + inner_product(decomposed_group_idx, unmasked_stride)
            )
        groups.append(ranks)
    return groups


class MegatronRankGenerator:
    """Megatron ``RankGenerator`` for TP/EP/DP/PP/CP rank groups."""

    def __init__(
        self,
        *,
        tp: int,
        ep: int,
        dp: int,
        pp: int,
        cp: int,
        order: str,
        rank_offset: int = 0,
    ) -> None:
        if ep != 1 and cp != 1:
            raise ValueError("EP and CP cannot both be > 1 in one rank generator")

        self.tp = tp
        self.ep = ep
        self.dp = dp
        self.pp = pp
        self.cp = cp
        self.rank_offset = rank_offset
        self.world_size = tp * dp * pp * cp * ep

        self.name_to_size = {
            "tp": self.tp,
            "pp": self.pp,
            "dp": self.dp,
            "ep": self.ep,
            "cp": self.cp,
        }
        normalized_order = order.lower()
        for name, size in self.name_to_size.items():
            if name not in normalized_order and size != 1:
                raise ValueError(
                    f"parallel size {name}={size} but order {order!r} omits {name}"
                )
            if name not in normalized_order:
                normalized_order = f"{normalized_order}-{name}"

        self.order = normalized_order
        self.ordered_size = [
            self.name_to_size[token] for token in self.order.split("-")
        ]

    def get_ranks(self, token: str) -> list[list[int]]:
        ordered_tokens = self.order.split("-")
        token_list = token.split("-")
        mask = [False] * len(ordered_tokens)
        for item in token_list:
            mask[ordered_tokens.index(item)] = True

        groups = generate_masked_orthogonal_rank_groups(
            self.world_size, self.ordered_size, mask
        )
        if self.rank_offset:
            for group in groups:
                for index, rank in enumerate(group):
                    group[index] = rank + self.rank_offset
        return groups


def resolve_parallel_order(
    *,
    order: str | None = None,
    use_tp_pp_dp_mapping: bool = False,
) -> str:
    if use_tp_pp_dp_mapping:
        return TP_PP_DP_MAPPING_ORDER
    return order or DEFAULT_PARALLEL_ORDER


def infer_parallel_topology(
    world_size: int,
    *,
    pp_size: int,
    tp_size: int = 1,
    dp_size: int | None = None,
    cp_size: int = 1,
    ep_size: int = 1,
    order: str | None = None,
    use_tp_pp_dp_mapping: bool = False,
) -> ParallelTopology:
    """
    Build PP/TP/DP groups using hcu_megatron / Megatron rank layout.

    Rank assignment follows hostfile slot order (rank 0 .. world_size-1).
    """
    if world_size < 1:
        raise ValueError("world_size must be >= 1")

    resolved_order = resolve_parallel_order(
        order=order, use_tp_pp_dp_mapping=use_tp_pp_dp_mapping
    )

    if world_size % (pp_size * tp_size) != 0:
        raise ValueError(
            f"world_size={world_size} is not divisible by pp_size*tp_size="
            f"{pp_size * tp_size}"
        )

    if dp_size is None:
        dp_size = world_size // (pp_size * tp_size)

    layout = ParallelLayout(
        tensor_model_parallel_size=tp_size,
        pipeline_model_parallel_size=pp_size,
        data_parallel_size=dp_size,
        context_parallel_size=cp_size,
        expert_model_parallel_size=ep_size,
        order=resolved_order,
    )
    if layout.world_size != world_size:
        raise ValueError(
            f"pp({pp_size})*tp({tp_size})*dp({dp_size})*cp({cp_size})*ep({ep_size}) "
            f"= {layout.world_size} != world_size({world_size})"
        )

    generator = MegatronRankGenerator(
        tp=tp_size,
        ep=ep_size,
        dp=dp_size,
        pp=pp_size,
        cp=cp_size,
        order=resolved_order,
    )

    pp_groups = [frozenset(group) for group in generator.get_ranks("pp")]
    tp_groups = (
        [frozenset(group) for group in generator.get_ranks("tp")]
        if tp_size > 1
        else []
    )
    dp_groups = [frozenset(group) for group in generator.get_ranks("dp")]

    return ParallelTopology(
        pp_groups=pp_groups,
        tp_groups=tp_groups,
        dp_groups=dp_groups,
    )
