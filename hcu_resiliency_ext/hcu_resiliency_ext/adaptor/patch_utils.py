# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import importlib
import sys
import types
from importlib.abc import MetaPathFinder, Loader
import importlib.abc


def dummy_function_wrapper(func_name):
    def dummy_function(*args, **kwargs):
        raise RuntimeError(f'函数 {func_name} 不存在')
    return dummy_function


class Patch:
    def __init__(self, orig_func_or_cls_name, new_func_or_cls, create_dummy, apply_wrapper=False, remove_origin_wrappers=False):
        split_name = orig_func_or_cls_name.rsplit('.', 1)
        if len(split_name) == 1:
            self.orig_module_name, self.orig_func_or_cls_name = orig_func_or_cls_name, None
        else:
            self.orig_module_name, self.orig_func_or_cls_name = split_name
        self.orig_module = None
        self.orig_func_or_cls = None

        self.patch_func_or_cls = None
        self.wrappers = []
        self.remove_origin_wrappers = False
        if (
            new_func_or_cls is None
            and not remove_origin_wrappers
        ):
            new_func_or_cls = dummy_function_wrapper(orig_func_or_cls_name)

        self.set_patch_func(new_func_or_cls, apply_wrapper=apply_wrapper, remove_origin_wrappers=remove_origin_wrappers)
        self.is_applied = False
        self.create_dummy = create_dummy

    @property
    def orig_func_or_cls_id(self):
        return id(self.orig_func_or_cls)

    @property
    def patch_func_id(self):
        return id(self.patch_func_or_cls)

    @staticmethod
    def remove_wrappers(module, func_name, func):
        while True:
            if (
                module.__dict__
                and func_name in module.__dict__
                and isinstance(module.__dict__[func_name], (staticmethod, classmethod))
            ):
                func = module.__dict__[func_name].__func__
            if hasattr(func, '__wrapped__') and func.__wrapped__ is not None:
                func = func.__wrapped__
            elif hasattr(func, '__closure__') and func.__closure__ is not None:
                func = func.__closure__[0].cell_contents
            else:
                break

        return func

    def set_patch_func(self, new_func_or_cls=None, force_patch=False, apply_wrapper=False, remove_origin_wrappers=False):
        if remove_origin_wrappers:
            self.remove_origin_wrappers = True
        else:
            assert new_func_or_cls is not None

        if new_func_or_cls is None:
            return

        if (
            apply_wrapper
            or (hasattr(new_func_or_cls, '__name__') and new_func_or_cls.__name__.endswith(('wrapper', 'decorator')))
        ):
            for wrapper in self.wrappers:
                if id(wrapper) == id(new_func_or_cls):
                    raise RuntimeError(f"wrapper {getattr(new_func_or_cls, '__name__')} has already been applied")
            self.wrappers.append(new_func_or_cls)
        else:
            if (
                self.patch_func_or_cls
                and not force_patch
                and id(new_func_or_cls) != id(self.patch_func_or_cls)
            ):
                raise RuntimeError('the patch of {} exist !'.format(self.orig_func_or_cls_name))
            self.patch_func_or_cls = new_func_or_cls
        self.is_applied = False

    def apply_patch(self):
        if self.is_applied:
            return

        self.orig_module, self.orig_func_or_cls = Patch.parse_path(self.orig_module_name, self.orig_func_or_cls_name, self.create_dummy)

        final_patch_func_or_cls = self.orig_func_or_cls
        if self.patch_func_or_cls is not None:
            final_patch_func_or_cls = self.patch_func_or_cls

        # remove original wrappers
        if self.remove_origin_wrappers:
            final_patch_func_or_cls = self.remove_wrappers(self.orig_module, self.orig_func_or_cls_name, final_patch_func_or_cls)

        # add new wrappers
        for wrapper in self.wrappers:
            final_patch_func_or_cls = wrapper(final_patch_func_or_cls)

        if self.orig_func_or_cls_name is not None:
            setattr(self.orig_module, self.orig_func_or_cls_name, final_patch_func_or_cls)
        for key, value in sys.modules.copy().items():
            if self.orig_func_or_cls_name is not None and hasattr(value, self.orig_func_or_cls_name) \
                    and id(getattr(value, self.orig_func_or_cls_name)) == self.orig_func_or_cls_id:
                setattr(value, self.orig_func_or_cls_name, final_patch_func_or_cls)

        self.is_applied = True

    @staticmethod
    def parse_path(module_path, function_name, create_dummy):
        from importlib.machinery import ModuleSpec
        modules = module_path.split('.')
        for i in range(1, len(modules) + 1):
            parent = '.'.join(modules[:i - 1])
            path = '.'.join(modules[:i])
            try:
                importlib.import_module(path)
            except ModuleNotFoundError as e:
                if not parent or not hasattr(importlib.import_module(parent), modules[i - 1]):
                    if not create_dummy:
                        raise ModuleNotFoundError(e) from e
                    sys.modules[path] = types.ModuleType(path)
                    sys.modules[path].__file__ = 'hcu_megatron.dummy_module.py'
                    sys.modules[path].__spec__ = ModuleSpec(path, None)
                    if parent:
                        setattr(importlib.import_module(parent), modules[i - 1], sys.modules[path])
                else:
                    module = getattr(importlib.import_module(parent), modules[i - 1])
                    if hasattr(module, function_name):
                        return module, getattr(module, function_name)
                    elif create_dummy:
                        return module, dummy_function_wrapper(function_name)
                    else:
                        raise RuntimeError('no exist {} of {}'.format(function_name, module))

        if function_name is not None and not hasattr(sys.modules[module_path], function_name):
            setattr(sys.modules[module_path], function_name, None)
        return sys.modules[module_path], getattr(sys.modules[module_path], function_name) if function_name is not None else None


class ImportHookPatch:
    """
    延迟 patch：不主动 import 目标模块，避免触发模块顶层副作用。
    当目标模块真正被 import 完成后，再执行 patch。
    """
    _installed = False
    _targets = {}  # module_name -> list[callable(module)]

    @classmethod
    def add_target(cls, module_name: str, callback):
        cls._targets.setdefault(module_name, []).append(callback)
        cls._ensure_installed()

        # 如果模块已经 import 过了，直接执行 callback
        if module_name in sys.modules:
            callback(sys.modules[module_name])

    @classmethod
    def _ensure_installed(cls):
        if cls._installed:
            return
        sys.meta_path.insert(0, cls._Finder())
        cls._installed = True

    class _Finder(MetaPathFinder):
        def find_spec(self, fullname, path, target=None):
            if fullname not in ImportHookPatch._targets:
                return None

            # 避免递归：不要调用 importlib.util.find_spec(fullname)
            for finder in sys.meta_path:
                if finder is self:
                    continue
                if hasattr(finder, "find_spec"):
                    spec = finder.find_spec(fullname, path, target)
                    if spec is not None:
                        break
            else:
                spec = None

            if spec is None or spec.loader is None:
                return spec

            original_loader = spec.loader
            spec.loader = ImportHookPatch._Loader(fullname, original_loader)
            return spec

    class _Loader(Loader):
        def __init__(self, fullname, original_loader):
            self.fullname = fullname
            self.original_loader = original_loader

        def create_module(self, spec):
            if hasattr(self.original_loader, "create_module"):
                return self.original_loader.create_module(spec)
            return None

        def exec_module(self, module):
            # 首先让原模块正常导入完成
            self.original_loader.exec_module(module)
            
            # 然后执行 patch 回调
            callbacks = ImportHookPatch._targets.get(self.fullname, [])
            for cb in callbacks:
                try:
                    cb(module)
                except Exception as e:
                    print(f"[ImportHookPatch] patch failed for {self.fullname}: {e}")