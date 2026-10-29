#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# check_deepep_env.sh -- DeepEP 运行环境自检
#
# 检查项:
#   1. PCIe ACS Upstream Forwarding (UpstreamFwd) 是否开启
#      - 关闭时 IBGDA P2P 会卡住
#   2. hycu 驱动 XDP 是否打开 (/sys/module/hycu/parameters/xdp_size)
#   3. RDMA 网卡 / 端口状态(ACTIVE vs 其余) + 端口是 RoCE 还是 IB
#   4. ibv_devinfo 能否枚举到 IB 设备
#      - 没输出 / "No IB devices found" => WARN
#      - 正常输出设备信息              => PASS
#      注意: RoCE 网卡在 ibv_devinfo 里也叫 "IB device" (设备名 mlx5_N),
#            所以这一项过不了通常不是"没有 IB 网卡", 而是 libibverbs 找不到
#            驱动/设备节点 (没装 rdma-core、容器里没挂 /dev/infiniband 等)。
#
# 退出码: 0 = 全部通过, 1 = 有 FAIL, 2 = 有 WARN(无 FAIL)
#
# 用法:
#   ./check_deepep_env.sh              # 检查
#   ./check_deepep_env.sh --color      # 输出重定向时也强制带颜色
#   ./check_deepep_env.sh -h|--help    # 看用法和排查命令
#
# 判定口径:
#   - ACS: 只要有设备 UpstreamFwd+ 即算通过; 全是 UpstreamFwd- 才 FAIL
#   - 颜色: stdout 是终端时自动带颜色; 进文件/管道自动关闭(--color 强制开)

set -o pipefail

FORCE_COLOR=0
while [ $# -gt 0 ]; do
    case "$1" in
        --color)   FORCE_COLOR=1 ;;
        -h|--help) sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "未知参数: $1 (用 --help 查看用法)" >&2; exit 64 ;;
    esac
    shift
done

# ---------- 颜色 (tty 自动检测; --color 强制) ----------
if [ -t 1 ] || [ "$FORCE_COLOR" = 1 ]; then
    C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'
    C_BLD=$'\033[1m';  C_DIM=$'\033[2m';  C_RST=$'\033[0m'
else
    C_RED=""; C_GRN=""; C_YEL=""; C_BLD=""; C_DIM=""; C_RST=""
fi


FAILED=0
WARNED=0

pass() { printf '%s[PASS]%s %s\n' "$C_GRN" "$C_RST" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "$C_YEL" "$C_RST" "$*"; WARNED=1; }
fail() { printf '%s[FAIL]%s %s\n' "$C_RED" "$C_RST" "$*"; FAILED=1; }
info() { printf '%s      %s%s\n' "$C_DIM" "$*" "$C_RST"; }
hdr()  { printf '\n%s==== %s ====%s\n' "$C_BLD" "$*" "$C_RST"; }

# =====================================================================
# 1. PCIe ACS Upstream Forwarding
# =====================================================================
check_acsc() {
    hdr "1. PCIe ACS Upstream Forwarding"

    if ! command -v lspci >/dev/null 2>&1; then
        warn "lspci 不存在, 跳过 ACS 检查 (装 pciutils: apt/yum install pciutils)"
        return
    fi

    # 读扩展配置空间需要 root
    if [ "$(id -u)" -ne 0 ]; then
        warn "非 root, lspci -vvv 可能读不到 ACS 能力位, 结果不可信 (建议 sudo 运行)"
    fi

    local out
    # 屏蔽 "Unable to load libkmod resources" 之类的噪声
    out=$(lspci -vvv 2>/dev/null | grep -F 'ACSCtl:' | sed 's/^[[:space:]]*//')

    if [ -z "$out" ]; then
        warn "未找到任何 ACSCtl 行 (老平台/虚拟化环境可能没有 ACS 能力)"
        return
    fi

    local total up_on up_off
    total=$(printf '%s\n' "$out" | wc -l)
    up_on=$(printf '%s\n' "$out" | grep -c 'UpstreamFwd+')
    up_off=$(printf '%s\n' "$out" | grep -c 'UpstreamFwd-')

    printf '%s原始输出 (uniq -c):%s\n' "$C_BLD" "$C_RST"
    printf '%s\n' "$out" | sort | uniq -c | sed 's/^/      /'
    printf '\n'
    info "共 ${total} 个设备的 ACSCtl:  UpstreamFwd+ = ${up_on},  UpstreamFwd- = ${up_off}"

    if [ "$up_on" -eq 0 ]; then
        fail "ACSCtl check fail  --  所有设备的 UpstreamFwd 都是关闭的, IBGDA P2P 会卡住"
        info "修复: BIOS 里开启 ACS/Upstream Forwarding, 或"
        info "      setpci -s <BDF> ECAP_ACS+0x6.w=0000  (临时, 需逐个设备处理)"
        return
    fi

    pass "ACSCtl check pass."
    if [ "$up_off" -gt 0 ]; then
        info "注意: 仍有 ${up_off} 个设备 UpstreamFwd-, 若 P2P 对端正好落在这些设备下仍可能卡住"
        info "      (判定依据是最少有一个设备 UpstreamFwd+; 可用 lspci -vvv 核对具体 BDF)"
    fi
}

# =====================================================================
# 2. hycu XDP
# =====================================================================
XDP_FILE=/sys/module/hycu/parameters/xdp_size

check_xdp() {
    hdr "2. hycu XDP (${XDP_FILE})"

    if [ ! -e "$XDP_FILE" ]; then
        warn "读不到 ${XDP_FILE}  --  hycu 模块未加载, 或不是海光平台"
        info "确认模块: lsmod | grep hycu / modinfo hycu"
        return
    fi

    local val
    val=$(grep . "$XDP_FILE" 2>/dev/null | tr -d '[:space:]')
    if [ -z "$val" ]; then
        warn "无法读取 ${XDP_FILE} (权限不足?), 原样 cat:"
        cat "$XDP_FILE" 2>&1 | sed 's/^/      /'
        return
    fi
    if ! [[ "$val" =~ ^[0-9]+$ ]]; then
        warn "xdp_size 值异常: '${val}' (期望 0 或 13~15)"
        return
    fi

    info "xdp_size = ${val}"

    if [ "$val" -eq 0 ]; then
        warn "XDP not opened"
        info "打开 XDP: modprobe -r hycu && modprobe hycu xdp_size=14   (或写 /etc/modprobe.d/)"
    elif [ "$val" -ge 13 ] && [ "$val" -le 15 ]; then
        pass "XDP is opened, set to ${val}"
    else
        warn "xdp_size = ${val}, 不在期望范围 (0 或 13~15)"
    fi
}

# =====================================================================
# 3. RDMA 网卡 / 端口状态 + 端口类型 (RoCE / IB)
# =====================================================================
# 说明(命令怎么查):
#   端口状态 / 物理状态 / 速率 (脚本用的就是 sysfs, 无工具依赖):
#       cat /sys/class/infiniband/<dev>/ports/<p>/state        # 4: ACTIVE / 1: DOWN
#       cat /sys/class/infiniband/<dev>/ports/<p>/phys_state   # 5: LinkUp / 3: Disabled
#       cat /sys/class/infiniband/<dev>/ports/<p>/rate
#   等价的人类可读命令:
#       rdma link show               # 一行给出 state + phys_state + netdev
#       ibstat / ibstatus            # 装 infiniband-diags
#       ibv_devinfo
#   端口类型 (IB 还是 RoCE):
#       cat /sys/class/infiniband/<dev>/ports/<p>/link_layer   # InfiniBand 或 Ethernet
#       cat /sys/class/infiniband/<dev>/ports/<p>/gid_attrs/types/*   # RoCE v1 / v2
#       show_gids                    # 上面那堆 GID 的汇总
#       注: 网卡和交换机必须同为 IB 或同为 RoCE, 不能混插
#   对端/交换机信息 (本地查不出交换机型号, 只能看拓扑或用运维的台账):
#       iblinkinfo                   # IB 拓扑, 能看到交换机节点和端口
#       ibnetdiscover -p
#       ibdev2netdev -v              # PCI BDF <-> netdev 对应
check_rdma() {
    hdr "3. RDMA 网卡 / 端口状态"

    local ib_root=/sys/class/infiniband
    if [ ! -d "$ib_root" ] || [ -z "$(ls -A "$ib_root" 2>/dev/null)" ]; then
        warn "没有 RDMA 设备 (${ib_root} 为空)。"
        info "确认驱动: lsmod | grep -E 'mlx5_ib|ionic|hns_roce|bnxt_re|erdma' ; 以及 rdma link show"
        return
    fi

    if [ "$(id -u)" -ne 0 ]; then
        info "(非 root: 状态信息一般可读; 若为 '-' 请用 sudo 重跑)"
    fi

    local dev port state phys link_layer rate n_active=0 n_notactive=0
    local n_ib=0 n_roce=0 n_other=0
    local active_list=() notactive_list=() ib_list=() roce_list=() other_list=()

    for dev in $(ls "$ib_root" 2>/dev/null | sort); do
        for portdir in "$ib_root/$dev"/ports/*; do
            [ -d "$portdir" ] || continue
            port=$(basename "$portdir")
            # 只处理真实端口 (通常 1,2,...), 跳过 sysfs 里的非数字项
            case "$port" in ''|*[!0-9]*) continue ;; esac

            state=$(grep . "$portdir/state" 2>/dev/null | head -1)
            phys=$(grep . "$portdir/phys_state" 2>/dev/null | head -1)
            link_layer=$(grep . "$portdir/link_layer" 2>/dev/null | head -1)
            rate=$(grep . "$portdir/rate" 2>/dev/null | head -1)
            [ -n "$state" ] || state="-"
            [ -n "$phys" ] || phys="-"
            [ -n "$link_layer" ] || link_layer="-"
            [ -n "$rate" ] || rate="-"

            # sysfs 里是 "4: ACTIVE" 这种带数字前缀的, 取冒号后的名字
            state=$(printf '%s' "$state" | sed 's/^[0-9]*: *//')
            phys=$(printf '%s' "$phys" | sed 's/^[0-9]*: *//')

            # 端口类型归类
            case "$link_layer" in
                InfiniBand) n_ib=$((n_ib+1));    ib_list+=("$dev/$port") ;;
                Ethernet)   n_roce=$((n_roce+1)); roce_list+=("$dev/$port") ;;
                *)          n_other=$((n_other+1)); other_list+=("$dev/$port") ;;
            esac

            # 状态归类: ACTIVE(全大写) 视为正常
            if [ "$(printf '%s' "$state" | tr '[:lower:]' '[:upper:]')" = ACTIVE ]; then
                n_active=$((n_active+1))
                active_list+=("$dev/$port")
                printf '      %s%-20s port %-2s%s  state=%-8s phys=%-10s layer=%-11s rate=%s\n' \
                       "$C_GRN" "$dev" "$port" "$C_RST" "$state" "$phys" "$link_layer" "$rate"
            else
                n_notactive=$((n_notactive+1))
                notactive_list+=("$dev/$port (state=$state, phys=$phys)")
                printf '      %s%-20s port %-2s%s  state=%-8s phys=%-10s layer=%-11s rate=%s\n' \
                       "$C_YEL" "$dev" "$port" "$C_RST" "$state" "$phys" "$link_layer" "$rate"
            fi
        done
    done

    printf '\n'
    if [ "$n_notactive" -eq 0 ]; then
        pass "所有 RDMA 端口都是 ACTIVE (共 ${n_active} 个): ${active_list[*]}"
    else
        warn "有 ${n_notactive} 个端口不是 ACTIVE:"
        local x
        for x in "${notactive_list[@]}"; do info "$x"; done
        info "排查: ibstat / ibstatus ; dmesg | grep -iE 'mlx5|link down' ; 检查光模块/线缆/交换机口"
        info "若是 Down 且物理层 Polling: 线缆或对端口未起来; 若 Initializing: SM(IB)或 QoS(RoCE)问题"
    fi

    printf '\n%s端口类型 (网卡与交换机必须同为一种):%s\n' "$C_BLD" "$C_RST"
    info "InfiniBand : ${n_ib}  个  ${ib_list[*]:-}"
    info "Ethernet   : ${n_roce} 个 (RoCE)  ${roce_list[*]:-}"
    [ "$n_other" -gt 0 ] && info "未知       : ${n_other} 个  ${other_list[*]}"
    if [ "$n_ib" -gt 0 ] && [ "$n_roce" -gt 0 ]; then
        fail "同一台机器上同时出现 IB 和 RoCE 端口, 确认是否混用了两种网络"
    elif [ "$n_ib" -gt 0 ]; then
        pass "端口类型: InfiniBand (交换机应为 IB 交换机)"
    elif [ "$n_roce" -gt 0 ]; then
        pass "端口类型: RoCE (交换机应为以太网交换机, 需配好 PFC/ECN)"
    fi

    local netdevs
    netdevs=$(rdma_netdevs "$ib_root")
    if [ -n "$netdevs" ]; then
        printf '\n%s绑定的网口状态 (只列 RDMA 端口对应的 netdev):%s\n' "$C_BLD" "$C_RST"
        local nd state2
        for nd in $netdevs; do
            state2=$(cat "/sys/class/net/$nd/operstate" 2>/dev/null)
            [ -n "$state2" ] || state2=-
            if [ "$(printf '%s' "$state2" | tr '[:lower:]' '[:upper:]')" = UP ]; then
                printf '      %s%-16s up%s\n' "$C_GRN" "$nd" "$C_RST"
            else
                printf '      %s%-16s %s%s\n' "$C_YEL" "$nd" "$state2" "$C_RST"
            fi
        done
        info "看单体详情: ethtool <ifname>  (Speed / Link detected / 光模块)"
    fi
}

# 找出 RDMA 端口绑定的 netdev 名 (IB 端口没有, 属于正常)
rdma_netdevs() {
    local ib_root=$1 dev port out=""
    for dev in $(ls "$ib_root" 2>/dev/null | sort); do
        for portdir in "$ib_root/$dev"/ports/*; do
            [ -d "$portdir" ] || continue
            port=$(basename "$portdir")
            case "$port" in ''|*[!0-9]*) continue ;; esac
            # 新版内核: sysfs 的 gid_attrs/ndevs/<idx> 就是绑定的 netdev 名
            if [ -d "$portdir/gid_attrs/ndevs" ]; then
                local f
                for f in "$portdir"/gid_attrs/ndevs/*; do
                    [ -e "$f" ] || continue
                    out="$out $(cat "$f" 2>/dev/null)"
                done
            fi
        done
    done
    # 去重去空
    printf '%s\n' "$out" | tr ' ' '\n' | grep -v "^$" | sort -u | tr '\n' ' '
}

# =====================================================================
# 4. ibv_devinfo -- libibverbs 能否枚举到 IB 设备
# =====================================================================
# 为什么单独查一项: 上面第 3 项读的是 sysfs, 内核有驱动就能看到;
# 而 ibv_devinfo 走的是 userspace (libibverbs -> /dev/infiniband/), 两者可能
# 不一致 —— 典型场景是容器里没挂 /dev/infiniband, sysfs 看着正常但 verbs 全瞎,
# 这时 IBGDA/RDMA 一样起不来。
# 退出码: ibv_devinfo 查到设备返回 0, 找不到设备返回非 0 (有的版本是 1, 有的是 20)。
# 只认"有没有设备信息", 不认退出码, 免得被版本差异误伤。
check_ibv_devinfo() {
    hdr "4. ibv_devinfo 设备枚举"

    local bin=${IBV_DEVINFO:-ibv_devinfo}
    # 允许 IBV_DEVINFO 直接给绝对路径, 也允许给命令名
    if ! { [ -x "$bin" ] || command -v "$bin" >/dev/null 2>&1; }; then
        warn "没找到 ${bin} (libibverbs-utils/rdma-core 没装)"
        info "安装: yum install -y libibverbs-utils  |  apt install -y ibverbs-utils"
        return
    fi

    local out rc n_dev
    out=$("$bin" 2>&1); rc=$?
    # 有效输出 = 有 "device:" 行 (hca_id / device 各版本字段名不同, 都算)
    n_dev=$(printf '%s\n' "$out" | grep -cE '^[[:space:]]*(hca_id|device)[[:space:]]*:')

    if [ "$n_dev" -gt 0 ]; then
        pass "ibv_devinfo 输出正常 (枚举到 ${n_dev} 个 IB 设备)"
        local devname
        devname=$(printf '%s\n' "$out" \
                  | sed -n 's/^[[:space:]]*\(hca_id\|device\)[[:space:]]*:[[:space:]]*\(.*\)$/\2/p' \
                  | tr '\n' ' ')
        info "设备: ${devname:-?}"
        # 顺带核对一下和 sysfs 是否对得上
        if [ -d /sys/class/infiniband ]; then
            local k
            for k in $(ls /sys/class/infiniband 2>/dev/null | sort); do
                case " $devname " in *" $k "*) ;; *) info "sysfs 有 ${k} 但 ibv_devinfo 没报 (检查 /dev/infiniband 权限/挂载)";; esac
            done
        fi
    else
        warn "没查到IB设备"
        if [ -n "$out" ]; then
            printf '%s' "$out" | head -3 | while IFS= read -r l; do info "ibv_devinfo: $l"; done
        fi
        [ "$rc" -ne 0 ] && info "ibv_devinfo 退出码 ${rc}"
        info "排查: ls /dev/infiniband/ ; ibv_devices ; rdma link show ; modprobe mlx5_ib"
        info "容器里注意: 需要 --device /dev/infiniband (以及 ulimit -l unlimited)"
    fi
}

# =====================================================================
main() {
    local t0 total=4 idx=0 f
    t0=$(date +%s)
    printf '%sDeepEP 环境自检%s  (%s)  共 %d 项\n' "$C_BLD" "$C_RST" "$(date '+%F %T')" "$total"
    # 逐项打印进度（长耗时项如 lspci 期间也能看到当前在查什么，避免误判卡死）
    for f in check_acsc check_xdp check_rdma check_ibv_devinfo; do
        idx=$((idx + 1))
        printf '%s[%d/%d] 执行中...%s' "$C_DIM" "$idx" "$total" "$C_RST"
        "$f"
    done

    hdr "汇总"
    printf '%s     自检耗时 %ss%s\n' "$C_DIM" "$(( $(date +%s) - t0 ))" "$C_RST"
    if [ "$FAILED" -eq 1 ]; then
        printf '%s结果: 存在 FAIL, 环境不可用/需修复%s\n' "$C_RED" "$C_RST"
        exit 1
    elif [ "$WARNED" -eq 1 ]; then
        printf '%s结果: 有 WARN, 请确认是否影响你的用例%s\n' "$C_YEL" "$C_RST"
        exit 2
    else
        printf '%s结果: 全部通过%s\n' "$C_GRN" "$C_RST"
        exit 0
    fi
}

main
