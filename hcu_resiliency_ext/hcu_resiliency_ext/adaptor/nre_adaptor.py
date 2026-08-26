# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
import abc
import sys
import traceback


class NreAdaptation:
    """
        A module manager supports adaptation registration, application and execution.
    """
    _patch_info_collection = {}
    _args = None
    _module_patch_callbacks = {}

    @classmethod
    def execute(cls):
        """
        Execute adaptations.
        """
        adaptation = CoreAdaptation()
        adaptation.execute()
        NreAdaptation.apply()

    @classmethod
    def register(cls, orig_func_name, new_func=None, force_patch=False, create_dummy=False, apply_wrapper=False, remove_origin_wrappers=False):
        """
        Register adaptations into collection.
        """
        if orig_func_name not in cls._patch_info_collection:
            from .patch_utils import Patch
            cls._patch_info_collection[orig_func_name] = Patch(
                orig_func_name,
                new_func,
                create_dummy,
                apply_wrapper=apply_wrapper,
                remove_origin_wrappers=remove_origin_wrappers
            )
        else:
            cls._patch_info_collection.get(orig_func_name).set_patch_func(
                new_func,
                force_patch,
                apply_wrapper=apply_wrapper,
                remove_origin_wrappers=remove_origin_wrappers
            )

    @staticmethod
    def register_cls_funcs(orig_class, new_funcs: list = None, create_dummy=False):
        if not orig_class.endswith("."):
            orig_class += "."

        for new_func in new_funcs:
            assert hasattr(new_func, '__name__') and not new_func.__name__.endswith(('wrapper', 'decorator'))

            orig_func_name = orig_class + new_func.__name__
            NreAdaptation.register(orig_func_name, new_func=new_func, create_dummy=create_dummy)

    @classmethod
    def register_module_patch(cls, module_name: str, callback):
        cls._module_patch_callbacks.setdefault(module_name, []).append(callback)

    @classmethod
    def apply(cls):
        # 先装 import hook（延迟 patch）
        from .patch_utils import ImportHookPatch
        for module_name, callbacks in cls._module_patch_callbacks.items():
            for cb in callbacks:
                ImportHookPatch.add_target(module_name, cb)

        # 再做原有 patch（立即 patch）
        for patch in cls._patch_info_collection.values():
            patch.apply_patch()

    @classmethod
    def post_execute(cls):
        """
        Execute after other adaptations.
        """
        pass


class NreAdaptationABC:
    """
    Abstract class for adaptation.
    """
    @abc.abstractmethod
    def execute(self):
        """
        Do Adaptation
        """


class CoreAdaptation(NreAdaptationABC):
    """
    Adaptations for models in Megatron-LM Core structure.
    """
    
    def execute(self):
        # 使用模块重定向方案
        self.redirect_profiling_module()
    
    def redirect_profiling_module(self):
        """使用模块重定向替换整个 profiling 模块"""
        try:
            # 获取当前文件所在目录
            current_dir = os.path.dirname(os.path.abspath(__file__))
            patched_module_path = os.path.join(current_dir,'../shared_utils', "profiling.py")
            
            # 检查补丁模块文件是否存在
            if not os.path.exists(patched_module_path):
                return
            
            
            # 导入模块重定向器
            from .module_redirector import ModuleRedirector
            
            # 设置重定向
            original_module = "nvidia_resiliency_ext.shared_utils.profiling"
            ModuleRedirector.add_redirect(original_module, patched_module_path)
            
            
        except Exception as e:
            traceback.print_exc()


# 执行补丁
NreAdaptation.execute()