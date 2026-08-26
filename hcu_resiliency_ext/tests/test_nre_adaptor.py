# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import sys
from types import ModuleType

from hcu_resiliency_ext.hcu_resiliency_ext.adaptor.nre_adaptor import NreAdaptation


def test_adaptation_register_and_apply_replaces_target():
    module_name = "unit_nre_target"
    module = ModuleType(module_name)
    module.run = lambda: "old"
    sys.modules[module_name] = module
    original_collection = dict(NreAdaptation._patch_info_collection)
    original_callbacks = dict(NreAdaptation._module_patch_callbacks)

    try:
        NreAdaptation._patch_info_collection = {}
        NreAdaptation._module_patch_callbacks = {}
        NreAdaptation.register(f"{module_name}.run", lambda: "new")
        NreAdaptation.apply()
        assert module.run() == "new"
    finally:
        NreAdaptation._patch_info_collection = original_collection
        NreAdaptation._module_patch_callbacks = original_callbacks
        sys.modules.pop(module_name, None)


def test_register_module_patch_is_applied_to_loaded_module():
    module_name = "unit_nre_callback_target"
    module = ModuleType(module_name)
    sys.modules[module_name] = module
    original_collection = dict(NreAdaptation._patch_info_collection)
    original_callbacks = dict(NreAdaptation._module_patch_callbacks)

    try:
        NreAdaptation._patch_info_collection = {}
        NreAdaptation._module_patch_callbacks = {}
        NreAdaptation.register_module_patch(
            module_name, lambda loaded: setattr(loaded, "patched", True)
        )
        NreAdaptation.apply()
        assert module.patched is True
    finally:
        NreAdaptation._patch_info_collection = original_collection
        NreAdaptation._module_patch_callbacks = original_callbacks
        sys.modules.pop(module_name, None)
