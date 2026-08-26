<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# nvidia-resiliency-ext Adaptor Patch 使用说明

完整的子模块初始化、适配注入、安装、验证和回退流程见[安装与验证](docs/INSTALLATION_CN.md)。下面仅保留开发环境的最短接入步骤。

## 1. 进入项目目录

```bash
cd nvidia-resiliency-ext
```

## 2. 修改 __init__.py

将下面的代码 复制到 nvidia-resiliency-ext/src/nvidia_resiliency_ext/__init__.py 文件的第一行，用于自动添加 adaptor 路径并触发模块重定向：

```python

import sys
import os
def _apply_resiliency_adaptor_patch():
    try:
        # 添加 adaptor 路径到 sys.path
        adaptor_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), 
            '../../../'
        ))
        if adaptor_path not in sys.path:
            sys.path.insert(0, adaptor_path)  # 确保在最前面
        
        
        # 导入 nre_adaptor，触发模块重定向
        import hcu_resiliency_ext.adaptor.nre_adaptor
        
    except Exception as e:
        print(f'导入 nre adaptor 包出错: {e}')
        import traceback
        traceback.print_exc()


_apply_resiliency_adaptor_patch()

```

## 3. 设置环境变量

```bash
export STRAGGLER_DET_SKIP_CUPTI_EXT_BUILD=1
```

## 4. 安装项目

```bash
pip install -e . -i https://pypi.tuna.tsinghua.edu.cn/simple
```

## 5. License

The HCU adaptation is licensed under the Apache License, Version 2.0. NVIDIA
source and the Git submodule retain their original notices. See the
repository-level [LICENSE](../LICENSE), [NOTICE](../NOTICE), and
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md).
