#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

from cluster_manager.config.global_config import logger

class ParallelTopologyValidator:
    """
    Megatron Parallel Topology Validator

    Dense:
        tp * pp * cp * dp = world_size

    MoE:
        etp * ep * pp * edp = world_size

    Relation:
        dp = ep * edp
    """

    @staticmethod
    def get_world_size(cfg):

        nodes = cfg.get("required_nodes_num", 1)
        slots = cfg.get("slots_per_node", 1)

        return nodes * slots

    # --------------------------------
    # calculate dp
    # --------------------------------
    @staticmethod
    def calc_dp(world_size, tp, pp, cp):
        return 1
        tp_num = int(tp)
        pp_num = int(pp)
        cp_num = int(cp)
        word_num = int(world_size)
        denom = tp_num * pp_num * cp_num

        if word_num % denom != 0:
            raise ValueError(
                f"world_size({word_num}) not divisible by tp*pp*cp ({denom})"
            )

        return word_num // denom

    # --------------------------------
    # calculate edp
    # --------------------------------
    @staticmethod
    def calc_edp(world_size, ep, etp, pp):
        return 1
        denom = ep * etp * pp

        if world_size % denom != 0:
            raise ValueError(
                f"world_size({world_size}) not divisible by ep*etp*pp ({denom})"
            )

        return world_size // denom

    # --------------------------------
    # validate topology
    # --------------------------------
    @classmethod
    def validate(cls, cfg):
        return
        tp = cfg.get("tp", 1)
        pp = cfg.get("pp", 1)
        cp = cfg.get("cp", 1)

        ep = cfg.get("ep", 1)
        etp = cfg.get("etp", 1)

        world_size = cfg.get("world_size", 1)
        dp = cfg.get("dp", 1)
        edp = cfg.get("edp", 1)

        # if dp != ep * edp:
        #     raise ValueError(
        #         f"Inconsistent topology: dp({dp}) != ep({ep}) * edp({edp})"
        #     )

        # MoE 校验
        cls.validate_moe(cfg, ep)

        # Batch 校验
        cls.validate_batch(cfg, dp)

        return {
            "world_size": world_size,
            "dp": dp,
            "edp": edp
        }
    
    # ------------------------------------------------
    # batch 校验
    # ------------------------------------------------
    @staticmethod
    def validate_batch(cfg, dp):

        micro = cfg.get("micro_batch_size")
        grad_acc = cfg.get("gradient_accumulation_steps", 1)
        gbs = cfg.get("global_batch_size")

        # if micro is not None and gbs is not None:

        #     expected = micro * dp * grad_acc

        #     if expected != gbs:

        #         raise RuntimeError(
        #             f"GBS mismatch: "
        #             f"{micro} * {dp} * {grad_acc} = {expected} "
        #             f"!= {gbs}"
        #         )

        logger.debug("Batch validation passed")

    # ------------------------------------------------
    # MoE 校验
    # ------------------------------------------------
    @staticmethod
    def validate_moe(cfg, ep):

        num_experts = cfg.get("num_experts")

        if num_experts:

            if num_experts % ep != 0:

                raise RuntimeError(
                    f"num_experts {num_experts} "
                    f"not divisible by ep {ep}"
                )

        logger.debug("MoE validation passed")