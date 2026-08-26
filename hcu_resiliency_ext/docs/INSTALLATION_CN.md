<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hcu_resiliency_ext 安装与验证

## 组件边界

`hcu_resiliency_ext` 不是完整的 `nvidia-resiliency-ext` 分支，也不是当前可独立 `pip install` 的 Python 包。它通过 import hook，把：

```text
nvidia_resiliency_ext.shared_utils.profiling
```

重定向到本仓库的 HCU 适配实现。该操作会修改当前 Python 进程的模块导入行为，因此只能在经过验证的第三方源码版本上启用。

## 1. 初始化第三方子模块

在仓库根目录执行：

```bash
git submodule update --init hcu_resiliency_ext/nvidia_resiliency_ext
```

应使用主仓库记录的 gitlink commit，不要在构建过程中自动拉取 `main` 最新提交。升级第三方版本时需要重新运行适配测试和实际训练验证。

## 2. 注入适配入口

编辑：

```text
hcu_resiliency_ext/nvidia_resiliency_ext/src/nvidia_resiliency_ext/__init__.py
```

在文件开头加入：

```python
import os
import sys


def _apply_hcu_resiliency_adaptor():
    adaptor_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../")
    )
    if adaptor_root not in sys.path:
        sys.path.insert(0, adaptor_root)

    import hcu_resiliency_ext.adaptor.nre_adaptor  # noqa: F401


_apply_hcu_resiliency_adaptor()
```

该相对路径依赖当前目录布局：适配目录和 `nvidia_resiliency_ext` 子模块必须同位于仓库的 `hcu_resiliency_ext/` 下。

## 3. 环境和依赖

禁用第三方库中不适用于当前 HCU 环境的 CUPTI 扩展构建：

```bash
export STRAGGLER_DET_SKIP_CUPTI_EXT_BUILD=1
```

适配 profiling 实现还会导入 `nv_one_logger` 相关接口。安装前应按照目标 `nvidia-resiliency-ext` 版本的依赖文件准备完整环境，不能只安装本目录源码。

## 4. 安装第三方项目

```bash
cd hcu_resiliency_ext/nvidia_resiliency_ext
python -m pip install -e .
```

生产环境建议构建固定 commit 对应的 wheel，并记录 Python、PyTorch、DTK、`nvidia-resiliency-ext` 和 `nv_one_logger` 的确切版本。`-e` 方式更适合开发验证。

## 5. 验证重定向

在安装环境中执行：

```bash
python - <<'PY'
import nvidia_resiliency_ext.shared_utils.profiling as profiling

print(profiling.__file__)
assert "hcu_resiliency_ext" in profiling.__file__
print("HCU profiling adaptor is active")
PY
```

然后在仓库根目录运行适配单元测试：

```bash
python -m pytest hcu_resiliency_ext/tests -q
```

单元测试只验证重定向和补丁逻辑。正式使用前还需要在目标训练环境验证：

- 首次导入和重复导入不会产生重复 hook。
- profiling 事件可以正常初始化、记录和关闭。
- Slurm、MPI 和本地环境的 session metadata 正确。
- 未启用 WandB 时不会因可选依赖缺失而失败。
- 训练异常恢复路径能够完整执行。

## 卸载与回退

1. 卸载或切回原始 `nvidia-resiliency-ext` 安装。
2. 删除对上游 `__init__.py` 的适配入口修改。
3. 启动新的 Python 进程。import hook 已进入当前进程的 `sys.meta_path`，不能仅通过重新导入可靠回退。

## 当前限制

- 适配层尚未提供独立的包元数据和稳定公共 API。
- 启用逻辑在导入 `nre_adaptor` 时立即执行。
- 上游版本兼容性需要逐 commit 验证。
- 修改第三方 `__init__.py` 会增加升级和部署维护成本。

后续建议把适配层改造成独立可安装包，并提供显式、幂等、可撤销的 `enable()`/`disable()` 接口。
