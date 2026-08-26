# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import pandas as pd
import numpy as np

# 设置随机种子以便结果可重复
np.random.seed(42)

steps = np.arange(1, 1001)
# 模拟 loss：从 5 逐渐下降到 1，加上噪声
loss = 5 * np.exp(-steps / 500) + np.random.normal(0, 0.05, size=len(steps))
# 模拟 tflops：逐步上升至稳定
tflops = 100 + 50 * (1 - np.exp(-steps / 300)) + np.random.normal(0, 2, size=len(steps))
# 模拟 elapsed_seconds：线性增长
elapsed_seconds = steps * 2 + np.random.normal(0, 5, size=len(steps))

df = pd.DataFrame({
    'step': steps,
    'loss': loss,
    'tflops': tflops,
    'elapsed_seconds': elapsed_seconds
})

# 保存为 CSV
df.to_csv('data.csv', index=False)
print("初始 data.csv 生成完毕（step 1-1000）")