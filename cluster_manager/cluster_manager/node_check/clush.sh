# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
clushnode='./hostfile'

clush --hostfile=$clushnode -f 1000 "free -g | grep Mem" | sort -n -k 4
clush --hostfile=$clushnode -f 1000 -b "ps -ef | grep python | grep -v grep | wc -l"
clush --hostfile=$clushnode -f 1000 -b "netstat -ant|awk '{print \$5}' | grep 25905  | wc -l"
clush --hostfile=$clushnode -f 1000 -b "source /opt/dtk-25.04/env.sh && /opt/dtk-25.04/bin/rocminfo | grep gfx "

