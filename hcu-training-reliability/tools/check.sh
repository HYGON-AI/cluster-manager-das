#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
#
# check.sh - 训练前节点检查脚本
# 使用 fault_detection.py 中的函数检查节点健康状态
# 采用级联检查模式：仅对健康节点执行后续检查
#
# 使用方法:
#   ./check.sh -f <hostfile> [-t <timeout>] [-o <output_dir>] [-h]
#
# 示例:
#   ./check.sh -f /path/to/hostfile
#   ./check.sh -f /path/to/hostfile -t 600 -o /path/to/output
#

set -e

# ==================== 颜色定义 ====================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ==================== 默认参数 ====================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
HOSTFILE=""
TIMEOUT=300
OUTPUT_DIR="${SCRIPT_DIR}/check_results"
# 默认执行的检查
NHC_CHECK=true
HCU_CHECK=true
MEM_CHECK=true

# ==================== 帮助信息 ====================
usage() {
    echo -e "\n${BLUE}训练前节点检查工具${NC}"
    echo -e "使用 fault_detection.py 中的函数检查节点健康状态"
    echo -e "采用级联检查模式：仅对健康节点执行后续检查\n"
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -f, --hostfile <file>   必选：指定待检查节点的 hostfile 路径"
    echo "  -t, --timeout <seconds> 可选：检查超时时间（默认: 300秒）"
    echo "  -o, --output <dir>      可选：结果输出目录（默认: ./check_results）"
    echo "  -h, --help              显示帮助信息"
    echo ""
    echo -e "${YELLOW}跳过检查选项（默认执行所有检查）:${NC}"
    echo "  --skip-nhc              跳过 NHC 检查"
    echo "  --skip-hcu              跳过 HCU 信息检查"
    echo "  --skip-mem              跳过内存信息检查"
    echo ""
    echo -e "${YELLOW}示例:${NC}"
    echo "  # 默认执行所有检查"
    echo "  $0 -f /path/to/hostfile"
    echo ""
    echo "  # 仅执行 NHC 检查"
    echo "  $0 -f /path/to/hostfile --skip-hcu --skip-mem"
    echo ""
    echo "  # 自定义超时和输出目录"
    echo "  $0 -f /path/to/hostfile -t 600 -o /path/to/output"
    exit 1
}

# ==================== 参数解析 ====================
while [[ $# -gt 0 ]]; do
    case "$1" in
        -f|--hostfile)
            HOSTFILE="$2"
            shift 2
            ;;
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --skip-nhc)
            NHC_CHECK=false
            shift 1
            ;;
        --skip-hcu)
            HCU_CHECK=false
            shift 1
            ;;
        --skip-mem)
            MEM_CHECK=false
            shift 1
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}错误：无效参数 $1${NC}"
            usage
            ;;
    esac
done

# ==================== 参数验证 ====================
if [[ -z "${HOSTFILE}" ]]; then
    echo -e "${RED}错误：必须指定 hostfile 参数${NC}"
    usage
fi

if [[ ! -f "${HOSTFILE}" ]]; then
    echo -e "${RED}错误：hostfile 不存在: ${HOSTFILE}${NC}"
    exit 1
fi

# ==================== 初始化 ====================
start_time=$(date +%s)
timestamp=$(date +"%Y%m%d_%H%M%S")

# 创建输出目录
mkdir -p "${OUTPUT_DIR}"

# 结果文件路径
HEALTHY_FILE="${OUTPUT_DIR}/healthy_nodes.txt"
FAULT_FILE="${OUTPUT_DIR}/fault_nodes.txt"
CHECK_LOG="${OUTPUT_DIR}/check_${timestamp}.log"

# 正在使用节点文件
HCU_IN_USE_FILE="${OUTPUT_DIR}/hcu_in_use_nodes.txt"
MEM_IN_USE_FILE="${OUTPUT_DIR}/mem_in_use_nodes.txt"

# 当前检查用的 hostfile（会动态更新）
CURRENT_HOSTFILE="${OUTPUT_DIR}/current_hostfile.txt"

# 清理旧结果
rm -f "${HEALTHY_FILE}" "${FAULT_FILE}" "${CURRENT_HOSTFILE}"

# 初始化：复制原始 hostfile 作为当前检查用的 hostfile
cp "${HOSTFILE}" "${CURRENT_HOSTFILE}"

# 累积故障节点文件
ACCUMULATED_FAULT_FILE="${OUTPUT_DIR}/accumulated_fault_nodes.txt"
> "${ACCUMULATED_FAULT_FILE}"

# 统计节点数量
total_nodes=$(grep -cve '^\s*$' "${HOSTFILE}")
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}       训练前节点检查工具${NC}"
echo -e "${BLUE}    （级联检查模式：仅对健康节点执行后续检查）${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}时间: $(date)${NC}"
echo -e "${BLUE}Hostfile: ${HOSTFILE}${NC}"
echo -e "${BLUE}总节点数: ${total_nodes}${NC}"
echo -e "${BLUE}超时时间: ${TIMEOUT}s${NC}"
echo -e "${BLUE}输出目录: ${OUTPUT_DIR}${NC}"
echo -e "${BLUE}========================================${NC}\n"

# ==================== 更新 hostfile 函数 ====================
# 从健康节点文件更新当前 hostfile，并累积故障节点
update_hostfile_and_accumulate_faults() {
    local healthy_file="$1"
    local fault_file="$2"
    local check_name="$3"
    
    # 如果健康节点文件存在且有内容，更新当前 hostfile
    if [[ -f "${healthy_file}" ]] && [[ $(wc -l < "${healthy_file}") -gt 0 ]]; then
        # 添加 slots=8 后缀
        sed 's/$/ slots=8/' "${healthy_file}" > "${CURRENT_HOSTFILE}"
        healthy_count=$(wc -l < "${healthy_file}")
        echo -e "${GREEN}健康节点数: ${healthy_count}，将作为后续检查的输入${NC}"
    else
        # 没有健康节点，清空当前 hostfile
        > "${CURRENT_HOSTFILE}"
        echo -e "${RED}没有健康节点，后续检查将跳过${NC}"
    fi
    
    # 累积故障节点
    if [[ -f "${fault_file}" ]] && [[ $(wc -l < "${fault_file}") -gt 0 ]]; then
        cat "${fault_file}" >> "${ACCUMULATED_FAULT_FILE}"
        fault_count=$(wc -l < "${fault_file}")
        echo -e "${RED}本次检测到故障节点: ${fault_count} 个${NC}"
    fi
}

# ==================== Python 脚本封装 ====================
run_python_check() {
    local check_type="$1"
    local python_script="
import sys
sys.path.insert(0, '${PROJECT_ROOT}')
from cluster_manager.launcher.fault_detection import FaultDetection
from cluster_manager.node_management.hostfile_handler import HostfileHandler

def main():
    fd = FaultDetection()
    hostfile = '${CURRENT_HOSTFILE}'
    timeout = ${TIMEOUT}
    
    try:
"

    case "${check_type}" in
        "nhc")
            python_script+="
        print('[INFO] 执行 NHC 检查...')
        passed, failed = fd.run_nhc(hostfile, timeout)
        print(f'[RESULT] NHC 检查完成')
        print(f'[RESULT] 通过节点: {len(passed)}')
        print(f'[RESULT] 失败节点: {len(failed)}')
        
        # 保存结果
        with open('${HEALTHY_FILE}', 'w') as f:
            for node in passed:
                f.write(node + '\\n')
        with open('${FAULT_FILE}', 'w') as f:
            for node in failed:
                f.write(node + '\\n')
        
        # 详细信息保存到日志文件
        import json
        with open('${CHECK_LOG}', 'a') as f:
            f.write('\\n--- NHC 详细信息 ---\\n')
            f.write(f'通过节点 ({len(passed)}): {passed}\\n')
            f.write(f'失败节点 ({len(failed)}): {failed}\\n')
            f.write('\\n')
"
            ;;
        "hcu_info")
            python_script+="
        print('[INFO] 获取 HCU 信息...')
        hcu_info = fd.get_hcu_info(hostfile, timeout)
        print(f'[RESULT] HCU 信息获取完成')
        print(f'[RESULT] 节点数: {len(hcu_info)}')
        
        # 检查正在使用 HCU 的节点（vram != 0 表示正在使用）
        hcu_in_use_nodes = []
        for node, cards in hcu_info.items():
            for card_key, card_info in cards.items():
                vram = card_info.get('vram', 0)
                if vram != 0:
                    hcu_in_use_nodes.append(node)
                    break  # 只要有一张卡在使用，该节点就在使用
        
        # 保存正在使用的节点
        with open('${HCU_IN_USE_FILE}', 'w') as f:
            for node in hcu_in_use_nodes:
                f.write(node + '\\n')
        
        print(f'[RESULT] HCU 正在使用节点数: {len(hcu_in_use_nodes)}')
        if hcu_in_use_nodes:
            print(f'[RESULT] HCU 正在使用节点: {hcu_in_use_nodes}')
        
        # 详细信息保存到日志文件
        import json
        with open('${CHECK_LOG}', 'a') as f:
            f.write('\\n--- HCU 详细信息 ---\\n')
            f.write(json.dumps(hcu_info, indent=2, ensure_ascii=False))
            f.write('\\n')
            f.write(f'\\nHCU 正在使用节点 ({len(hcu_in_use_nodes)}): {hcu_in_use_nodes}\\n')
"
            ;;
        "mem_info")
            python_script+="
        print('[INFO] 获取内存信息...')
        mem_info = fd.get_mem_info(hostfile, timeout)
        print(f'[RESULT] 内存信息获取完成')
        print(f'[RESULT] 节点数: {len(mem_info)}')
        
        # 检查正在使用内存的节点（Mem.used != 0 表示正在使用）
        mem_in_use_nodes = []
        for node_info in mem_info:
            node = node_info.get('node', '')
            mem_data = node_info.get('Mem', {})
            used = mem_data.get('used', 0)
            if used != 0:
                mem_in_use_nodes.append(node)
        
        # 保存正在使用的节点
        with open('${MEM_IN_USE_FILE}', 'w') as f:
            for node in mem_in_use_nodes:
                f.write(node + '\\n')
        
        print(f'[RESULT] 内存正在使用节点数: {len(mem_in_use_nodes)}')
        if mem_in_use_nodes:
            print(f'[RESULT] 内存正在使用节点: {mem_in_use_nodes}')
        
        # 详细信息保存到日志文件
        import json
        with open('${CHECK_LOG}', 'a') as f:
            f.write('\\n--- 内存详细信息 ---\\n')
            f.write(json.dumps(mem_info, indent=2, ensure_ascii=False))
            f.write('\\n')
            f.write(f'\\n内存正在使用节点 ({len(mem_in_use_nodes)}): {mem_in_use_nodes}\\n')
"
            ;;
        "sinfo")
            python_script+="
        print('[INFO] 执行 sinfo -R 检查...')
        error_nodes = fd.check_sinfo_R(hostfile, timeout)
        print(f'[RESULT] sinfo -R 检查完成')
        print(f'[RESULT] 异常节点数: {len(error_nodes)}')
        if error_nodes:
            import json
            print(json.dumps(error_nodes, indent=2, ensure_ascii=False))
        
        # 从当前 hostfile 读取所有节点
        all_nodes = []
        with open(hostfile, 'r') as f:
            for line in f:
                node = line.strip().split()[0] if line.strip() else ''
                if node:
                    all_nodes.append(node)
        
        # 计算健康节点（不在 error_nodes 中的节点）
        error_node_list = list(error_nodes.keys()) if error_nodes else []
        healthy_nodes = [n for n in all_nodes if n not in error_node_list]
        
        # 保存结果
        with open('${HEALTHY_FILE}', 'w') as f:
            for node in healthy_nodes:
                f.write(node + '\\n')
        with open('${FAULT_FILE}', 'w') as f:
            for node in error_node_list:
                f.write(node + '\\n')
        
        # 详细信息保存到日志文件
        import json
        with open('${CHECK_LOG}', 'a') as f:
            f.write('\\n--- sinfo -R 详细信息 ---\\n')
            f.write(f'检查节点: {all_nodes}\\n')
            f.write(f'健康节点 ({len(healthy_nodes)}): {healthy_nodes}\\n')
            f.write(f'异常节点 ({len(error_node_list)}): {error_nodes}\\n')
            f.write('\\n')
"
            ;;
    esac

    python_script+="
    except Exception as e:
        print(f'[ERROR] 检查失败: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
"

    echo "${python_script}" | python3 2>&1 | tee -a "${CHECK_LOG}"
    return ${PIPESTATUS[0]}
}

# ==================== 检查是否有健康节点 ====================
check_has_healthy_nodes() {
    if [[ ! -f "${CURRENT_HOSTFILE}" ]] || [[ $(wc -l < "${CURRENT_HOSTFILE}") -eq 0 ]]; then
        return 1
    fi
    return 0
}

# ==================== 执行检查 ====================
# 检查顺序：1.sinfo -R -> 2.run_nhc -> 3.hcu -> 4.内存

# 1. sinfo -R 检查（始终执行，作为第一道筛选）
echo -e "\n${YELLOW}========== [1] sinfo -R 检查 ==========${NC}"
echo -e "${YELLOW}当前检查节点数: $(wc -l < "${CURRENT_HOSTFILE}")${NC}"
run_python_check "sinfo"

if [[ -f "${HEALTHY_FILE}" ]]; then
    healthy_count=$(wc -l < "${HEALTHY_FILE}")
    echo -e "\n${GREEN}健康节点数: ${healthy_count}${NC}"
fi

if [[ -f "${FAULT_FILE}" ]]; then
    fault_count=$(wc -l < "${FAULT_FILE}")
    if [[ ${fault_count} -gt 0 ]]; then
        echo -e "${RED}异常节点数: ${fault_count}${NC}"
        echo -e "${RED}异常节点列表:${NC}"
        cat "${FAULT_FILE}" | while read -r node; do
            echo -e "${RED}  - ${node}${NC}"
        done
    fi
fi

# 更新 hostfile 并累积故障节点
update_hostfile_and_accumulate_faults "${HEALTHY_FILE}" "${FAULT_FILE}" "sinfo"

# 检查是否有健康节点继续后续检查
if ! check_has_healthy_nodes; then
    echo -e "\n${RED}没有健康节点，跳过后续检查${NC}"
else
    # 2. NHC 检查（默认执行）
    if [[ "${NHC_CHECK}" == "true" ]]; then
        echo -e "\n${YELLOW}========== [2] NHC 检查 ==========${NC}"
        echo -e "${YELLOW}当前检查节点数: $(wc -l < "${CURRENT_HOSTFILE}")${NC}"
        run_python_check "nhc"
        
        if [[ -f "${HEALTHY_FILE}" ]]; then
            healthy_count=$(wc -l < "${HEALTHY_FILE}")
            echo -e "\n${GREEN}健康节点数: ${healthy_count}${NC}"
        fi
        
        if [[ -f "${FAULT_FILE}" ]]; then
            fault_count=$(wc -l < "${FAULT_FILE}")
            if [[ ${fault_count} -gt 0 ]]; then
                echo -e "${RED}异常节点数: ${fault_count}${NC}"
                echo -e "${RED}异常节点列表:${NC}"
                cat "${FAULT_FILE}" | while read -r node; do
                    echo -e "${RED}  - ${node}${NC}"
                done
            fi
        fi
        
        # 更新 hostfile 并累积故障节点
        update_hostfile_and_accumulate_faults "${HEALTHY_FILE}" "${FAULT_FILE}" "NHC"
    fi

    # 检查是否有健康节点继续后续检查
    if ! check_has_healthy_nodes; then
        echo -e "\n${RED}没有健康节点，跳过后续检查${NC}"
    else
        # 3. HCU 信息检查（默认执行）
        if [[ "${HCU_CHECK}" == "true" ]]; then
            echo -e "\n${YELLOW}========== [3] HCU 信息检查 ==========${NC}"
            echo -e "${YELLOW}当前检查节点数: $(wc -l < "${CURRENT_HOSTFILE}")${NC}"
            run_python_check "hcu_info"
        fi

        # 4. 内存信息检查（默认执行）
        if [[ "${MEM_CHECK}" == "true" ]]; then
            echo -e "\n${YELLOW}========== [4] 内存信息检查 ==========${NC}"
            echo -e "${YELLOW}当前检查节点数: $(wc -l < "${CURRENT_HOSTFILE}")${NC}"
            run_python_check "mem_info"
        fi
    fi
fi

# ==================== 结果汇总 ====================
end_time=$(date +%s)
elapsed=$((end_time - start_time))

# 去重累积的故障节点
sort -u "${ACCUMULATED_FAULT_FILE}" -o "${FAULT_FILE}"

# 计算最终健康节点（原始节点 - 累积故障节点）
grep -vve '^\s*$' "${HOSTFILE}" | awk '{print $1}' | sort -u > "${OUTPUT_DIR}/all_nodes.txt"
comm -23 "${OUTPUT_DIR}/all_nodes.txt" "${FAULT_FILE}" > "${HEALTHY_FILE}"

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}          检查结果汇总${NC}"
echo -e "${BLUE}========================================${NC}"

if [[ -f "${HEALTHY_FILE}" ]]; then
    healthy_count=$(wc -l < "${HEALTHY_FILE}")
    echo -e "${GREEN}最终健康节点数: ${healthy_count}${NC}"
    echo -e "健康节点列表: ${HEALTHY_FILE}"
fi

if [[ -f "${FAULT_FILE}" ]]; then
    fault_count=$(wc -l < "${FAULT_FILE}")
    echo -e "${RED}累积故障节点数: ${fault_count}${NC}"
    if [[ ${fault_count} -gt 0 ]]; then
        echo -e "${RED}故障节点列表:${NC}"
        cat "${FAULT_FILE}" | while read -r node; do
            echo -e "${RED}  - ${node}${NC}"
        done
    fi
    echo -e "故障节点列表: ${FAULT_FILE}"
fi

# 显示正在使用的节点（非故障节点，仅表示资源占用状态）
echo -e "\n${YELLOW}---------- 资源使用状态 ----------${NC}"

if [[ -f "${HCU_IN_USE_FILE}" ]] && [[ $(wc -l < "${HCU_IN_USE_FILE}") -gt 0 ]]; then
    hcu_in_use_count=$(wc -l < "${HCU_IN_USE_FILE}")
    echo -e "${YELLOW}HCU 正在使用节点数: ${hcu_in_use_count}${NC}"
    echo -e "${YELLOW}HCU 正在使用节点列表:${NC}"
    cat "${HCU_IN_USE_FILE}" | while read -r node; do
        echo -e "${YELLOW}  - ${node}${NC}"
    done
    echo -e "HCU 正在使用节点列表: ${HCU_IN_USE_FILE}"
else
    echo -e "${GREEN}HCU 空闲节点: 所有检查节点 HCU 均空闲${NC}"
fi

if [[ -f "${MEM_IN_USE_FILE}" ]] && [[ $(wc -l < "${MEM_IN_USE_FILE}") -gt 0 ]]; then
    mem_in_use_count=$(wc -l < "${MEM_IN_USE_FILE}")
    echo -e "${YELLOW}内存正在使用节点数: ${mem_in_use_count}${NC}"

    echo -e "内存正在使用节点列表: ${MEM_IN_USE_FILE}"
else
    echo -e "${GREEN}内存空闲节点: 所有检查节点内存均空闲${NC}"
fi

echo -e "\n检查日志: ${CHECK_LOG}"
printf "\n总耗时: %02d 分 %02d 秒\n" $((elapsed/60)) $((elapsed%60))

# ==================== 退出状态 ====================
if [[ -f "${FAULT_FILE}" ]] && [[ $(wc -l < "${FAULT_FILE}") -gt 0 ]]; then
    echo -e "\n${RED}警告：检测到异常节点，建议在训练前处理！${NC}"
    exit 1
else
    echo -e "\n${GREEN}所有节点检查通过，可以开始训练。${NC}"
    exit 0
fi
