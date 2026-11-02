# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import pandas as pd
import math
import random

# 设置随机种子以便结果可重复
random.seed(42)

steps = list(range(1, 1001))
# 模拟 loss：从 5 逐渐下降到 1，加上噪声
loss = [5 * math.exp(-step / 500) + random.gauss(0, 0.05) for step in steps]
# 模拟 tflops：逐步上升至稳定
tflops = [
    100 + 50 * (1 - math.exp(-step / 300)) + random.gauss(0, 2)
    for step in steps
]
# 模拟 elapsed_seconds：线性增长
elapsed_seconds = [step * 2 + random.gauss(0, 5) for step in steps]

df = pd.DataFrame({
    'step': steps,
    'loss': loss,
    'tflops': tflops,
    'elapsed_seconds': elapsed_seconds
})

# 保存为 CSV
df.to_csv('data.csv', index=False)
print("初始 data.csv 生成完毕（step 1-1000）")
