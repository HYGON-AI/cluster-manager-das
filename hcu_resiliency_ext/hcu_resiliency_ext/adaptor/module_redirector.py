# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import sys
import os
import importlib.abc
import importlib.util


class ModuleRedirector:
    """模块重定向器，将特定模块的导入重定向到我们的版本"""
    
    _redirects = {}
    
    @classmethod
    def add_redirect(cls, original_module, replacement_module_path):
        """添加模块重定向
        
        Args:
            original_module: 原始模块名，如 "nvidia_resiliency_ext.shared_utils.profiling"
            replacement_module_path: 替换模块的文件路径
        """
        cls._redirects[original_module] = replacement_module_path
        cls._install_finder()
    
    @classmethod
    def _install_finder(cls):
        """安装查找器"""
        if not hasattr(cls, '_finder_installed') or not cls._finder_installed:
            sys.meta_path.insert(0, cls._Finder())
            cls._finder_installed = True
    
    class _Finder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path, target=None):
            # 检查是否需要重定向
            if fullname in ModuleRedirector._redirects:
                replacement_path = ModuleRedirector._redirects[fullname]
                
                # 创建spec
                spec = importlib.util.spec_from_file_location(
                    fullname, 
                    replacement_path,
                    submodule_search_locations=path
                )
                return spec
            
            return None