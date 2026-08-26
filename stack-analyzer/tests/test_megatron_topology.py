# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from stack_analyzer.megatron_topology import (
    DEFAULT_PARALLEL_ORDER,
    MegatronRankGenerator,
    infer_parallel_topology,
)


def test_megatron_default_pp_groups_16x4():
    topo = infer_parallel_topology(16, pp_size=4, tp_size=1, dp_size=4)
    assert sorted(topo.pp_groups) == sorted(
        [
            frozenset({0, 4, 8, 12}),
            frozenset({1, 5, 9, 13}),
            frozenset({2, 6, 10, 14}),
            frozenset({3, 7, 11, 15}),
        ]
    )


def test_megatron_tp_pp_dp_mapping_consecutive_pp():
    topo = infer_parallel_topology(
        16,
        pp_size=4,
        tp_size=1,
        dp_size=4,
        use_tp_pp_dp_mapping=True,
    )
    assert sorted(topo.pp_groups) == sorted(
        [
            frozenset({0, 1, 2, 3}),
            frozenset({4, 5, 6, 7}),
            frozenset({8, 9, 10, 11}),
            frozenset({12, 13, 14, 15}),
        ]
    )


def test_rank_generator_dp_groups_size():
    generator = MegatronRankGenerator(
        tp=2, ep=1, dp=4, pp=3, cp=1, order=DEFAULT_PARALLEL_ORDER
    )
    dp_groups = generator.get_ranks("dp")
    assert len(dp_groups) == 6
    assert all(len(group) == 4 for group in dp_groups)
    assert sorted(sum(dp_groups, [])) == list(range(24))
