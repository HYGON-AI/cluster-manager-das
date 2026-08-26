# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os

import pandas as pd
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)
CSV_PATH = "data.csv"

def load_data(last_step=0):
    """读取 CSV，返回 step > last_step 的数据"""
    try:
        df = pd.read_csv(CSV_PATH)
        df = df.astype({'step': int, 'loss': float, 'tflops': float, 'elapsed_seconds': float})
        # 筛选大于 last_step 的数据
        new_df = df[df['step'] > last_step]
        return {
            'steps': new_df['step'].tolist(),
            'loss': new_df['loss'].tolist(),
            'tflops': new_df['tflops'].tolist(),
            'elapsed': new_df['elapsed_seconds'].tolist()
        }
    except Exception as e:
        print(f"读取失败: {e}")
        return {'steps': [], 'loss': [], 'tflops': [], 'elapsed': []}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def api_data():
    """增量接口，通过 last_step 参数获取新数据"""
    last_step = request.args.get('last_step', default=0, type=int)
    return jsonify(load_data(last_step))

if __name__ == '__main__':
    host = os.environ.get("HCU_VIEW_HOST", "127.0.0.1")
    port = int(os.environ.get("HCU_VIEW_PORT", "5000"))
    debug = os.environ.get("HCU_VIEW_DEBUG", "").lower() in {"1", "true", "yes"}
    app.run(host=host, port=port, debug=debug)
