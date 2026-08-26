# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import sys
from types import ModuleType

import pytest

from hcu_resiliency_ext.hcu_resiliency_ext.adaptor.module_redirector import ModuleRedirector
from hcu_resiliency_ext.hcu_resiliency_ext.adaptor.patch_utils import ImportHookPatch, Patch


@pytest.fixture(autouse=True)
def restore_import_state():
    original_meta_path = list(sys.meta_path)
    original_redirects = dict(ModuleRedirector._redirects)
    original_installed = getattr(ModuleRedirector, "_finder_installed", False)
    original_targets = dict(ImportHookPatch._targets)
    original_hook_installed = ImportHookPatch._installed
    yield
    sys.meta_path[:] = original_meta_path
    ModuleRedirector._redirects = original_redirects
    ModuleRedirector._finder_installed = original_installed
    ImportHookPatch._targets = original_targets
    ImportHookPatch._installed = original_hook_installed
    for name in ("unit_patch_target", "unit_dummy", "unit_dummy.child"):
        sys.modules.pop(name, None)


def test_patch_replaces_function_and_is_idempotent():
    module = ModuleType("unit_patch_target")
    original = lambda value: f"old:{value}"
    replacement = lambda value: f"new:{value}"
    module.run = original
    sys.modules[module.__name__] = module

    patch = Patch("unit_patch_target.run", replacement, create_dummy=False)
    patch.apply_patch()
    patch.apply_patch()

    assert module.run("x") == "new:x"
    assert patch.orig_func_or_cls is original


def test_patch_applies_wrapper_to_original_function():
    module = ModuleType("unit_patch_target")
    module.run = lambda value: value + 1
    sys.modules[module.__name__] = module

    def wrapper(func):
        def wrapped(value):
            return func(value) * 2

        return wrapped

    patch = Patch(
        "unit_patch_target.run",
        wrapper,
        create_dummy=False,
        apply_wrapper=True,
    )
    patch.apply_patch()

    assert module.run(3) == 8


def test_patch_can_create_missing_module_and_dummy_function():
    patch = Patch("unit_dummy.child.missing", None, create_dummy=True)
    patch.apply_patch()

    with pytest.raises(RuntimeError, match="missing"):
        sys.modules["unit_dummy.child"].missing()


def test_import_hook_calls_callback_for_loaded_module():
    module = ModuleType("unit_patch_target")
    sys.modules[module.__name__] = module
    observed = []

    ImportHookPatch.add_target(module.__name__, observed.append)

    assert observed == [module]


def test_module_redirector_builds_spec_for_registered_module():
    replacement = str(Patch.__module__)
    replacement_path = sys.modules[replacement].__file__
    target = "vendor.package.profiling"

    ModuleRedirector.add_redirect(target, replacement_path)
    spec = ModuleRedirector._Finder().find_spec(target, None)

    assert spec is not None
    assert spec.name == target
    assert spec.origin == replacement_path
