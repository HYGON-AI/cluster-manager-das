#!/bin/bash 
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
start_time=$(date +%s)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 获取脚本所在目录，用于解析相对路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 默认值
CLUSHNODE="/workspace/hostfile"          # 必填：clush 节点列表文件
OUTPUT="${SCRIPT_DIR}/dual_stage_results"
HEALTHY_FILE="${SCRIPT_DIR}/node_checklog/host_check_pass"  # 健康节点默认路径
FAULT_FILE="${SCRIPT_DIR}/node_checklog/host_check_error"   # 异常节点默认路径
nodenum=4             # 可选：每组节点数（默认4）
HOSTFILE="${SCRIPT_DIR}/hostfile" # 生成的hostfile路径
ONLY_HORIZONTAL=0     # 新增：是否仅执行横向检测（0=否，1=是）
average_tflops=100    # 可选：TFLOPs阈值（默认185）

# 显示帮助信息
usage() {
    echo -e "\n${BLUE}Usage: $0 [OPTIONS]${NC}"
    echo "Options:"
    echo "  -c, --clushnode <file>   必选：指定检查节点的文件名"
    echo "  -n, --nodenum <num>      可选：每组节点数（默认4）"
    echo "  -t, --tflops <value>     可选：TFLOPs阈值（默认100）"
    echo "  -g, --healthy <file>     可选：指定健康节点保存路径（默认：./dual_stage_results/normal_nodes.txt）"
    echo "  -f, --fault <file>       可选：指定异常节点保存路径（默认：./dual_stage_results/fault_nodes.txt）"
    echo "  -o, --only-horizontal    可选：仅执行横向检测，跳过纵向检测（默认：否）"
    echo "  -h, --help               显示帮助信息"
    echo -e "\n${YELLOW}Example:${NC}"
    echo "  # 模式1：简化位置参数（推荐快速使用）"
    echo "  $0 ./clushnode 4"
    echo "  # 模式2：传统选项参数（支持全功能）"
    echo "  $0 -c ./clushnode -n 4 -g ./healthy_nodes.txt -f ./fault_nodes.txt"
    exit 1
}

# 如果第一个参数不以 - 开头，判定为简化模式（<clushnode_file> <nodenum>）
if [[ $# -ge 1 && ! "$1" =~ ^- ]]; then
    CLUSHNODE="$1"  # 第一个位置参数 = 节点文件
    # 第二个位置参数 = 分组数（可选，未传则用默认值4）
    if [[ $# -ge 2 && "$2" =~ ^[0-9]+$ ]]; then
        nodenum="$2"
        shift 2  # 跳过已解析的两个位置参数
    else
        shift 1  # 仅跳过节点文件参数
    fi
fi

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--clushnode)
            CLUSHNODE="$2"
            shift 2
            ;;
        -n|--nodenum)
            nodenum="$2"
            shift 2
            ;;
        -t|--tflops)
            average_tflops="$2"
            shift 2
            ;;
        -g|--healthy)  # 新增：处理健康节点路径
            HEALTHY_FILE="$2"
            shift 2
            ;;
        -f|--fault)    # 新增：处理异常节点路径
            FAULT_FILE="$2"
            shift 2
            ;;
	-o|--only-horizontal)  # 新增：仅横向检测参数
            ONLY_HORIZONTAL=1
            shift 1
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Error: 无效参数 $1${NC}"
            usage
            ;;
    esac
done


# 执行单机或多机分组
cat ${CLUSHNODE} |sed 's/$/ slots=8/' > ${HOSTFILE}

rm -rf "${OUTPUT}/horizontal" "${OUTPUT}/vertical"
rm -f "${HEALTHY_FILE}" "${FAULT_FILE}" "${OUTPUT}/diagnosis_report.txt"
rm -rf "${SCRIPT_DIR}/hostslice" "${SCRIPT_DIR}/node_checklog"
mkdir -p "${SCRIPT_DIR}/hostslice" "${SCRIPT_DIR}/node_checklog" "${OUTPUT}"/{horizontal,vertical}

# 统计hostfile中的节点数量
total_nodes=`grep -cve '^\s*$' ${HOSTFILE}`
echo -e "${BLUE}Total nodes: ${total_nodes}${NC}"
echo -e "${BLUE}Nodes per group: ${nodenum}${NC}"
echo -e "${BLUE}Average TFLOPs baseline: ${average_tflops}${NC}"
if [[ ${ONLY_HORIZONTAL} -eq 1 ]]; then
    echo -e "${YELLOW}Mode: Only horizontal testing (skip vertical phase)${NC}"
fi

# 计算可以完整分组的节点数
valid_nodes=$((total_nodes / nodenum * nodenum))
echo -e "${BLUE}Nodes participating in test: ${valid_nodes}${NC}"

# Read all node list
mapfile -t all_nodes < <(awk '{print $1}' ${HOSTFILE} | head -${valid_nodes})

# Create Horizontal Groups
echo -e "\n${YELLOW}========== Phase 1: Horizontal Group Testing ==========${NC}"
pids=()
for((i=1;i<=${valid_nodes};i=i+${nodenum}))
do
    ((j=i+${nodenum}-1))
    group_index=$(( (i-1)/nodenum + 1 ))
    echo "Creating horizontal group H${group_index}: nodes ${i}-${j}"
    sed -n "${i},${j}p" ${HOSTFILE} > ${SCRIPT_DIR}/hostslice/horizontal_host_${group_index}

    ${SCRIPT_DIR}/check_nodes.sh ${SCRIPT_DIR}/hostslice/horizontal_host_${group_index} \
        > ${SCRIPT_DIR}/node_checklog/horizontal_H${group_index}.log 2>&1 &
    pids+=($!)
done

for pid in "${pids[@]}"; do
    wait $pid
done

# 检查性能
MAX_JOBS=1000
job_count=0
pids=()

echo -e "\n${YELLOW}Analyzing horizontal group results...${NC}"
declare -a failed_horizontal=()
declare -A horizontal_failed_all_nodes=()

for log in ${SCRIPT_DIR}/node_checklog/horizontal_H*.log; do
{   
    group_name=$(basename "$log" .log | sed 's/horizontal_//')
    group_index=${group_name#H}
    host_file="${SCRIPT_DIR}/hostslice/horizontal_host_${group_index}"

    tflops=$(grep "TFLOP/s/GPU" "$log" | awk -F':' '{print $6}' | awk '{print $1}' | tail -n 1)
    node_list=$(awk '{print $1}' "${host_file}" | paste -sd,)

    if [ -z "${tflops}" ]; then
        echo -e "${RED}[FAIL]${NC} Horizontal group ${group_name} [${node_list}]: Test failed or no output"
        failed_horizontal+=(${group_index})
        # Record all nodes in this group
        for node in $(awk '{print $1}' "${host_file}"); do
            echo "${node} > horizontal_${group_name}" >> ${OUTPUT}/horizontal/failed_nodes_${group_name}.txt
            horizontal_failed_all_nodes["$node"]=1
        done
    elif [ "$(echo "${tflops} < ${average_tflops}" | bc)" -eq 1 ]; then
        echo -e "${RED}[FAIL]${NC} Horizontal group ${group_name} [${node_list}]: ${tflops} TFLOPs < ${average_tflops} TFLOPs"
        failed_horizontal+=(${group_index})
        for node in $(awk '{print $1}' "${host_file}"); do
            echo "${node} > horizontal_${group_name}" >> ${OUTPUT}/horizontal/failed_nodes_${group_name}.txt
            horizontal_failed_all_nodes["$node"]=1
        done
    else
        echo -e "${GREEN}[PASS]${NC} Horizontal group ${group_name} [${node_list}]: ${tflops} TFLOPs"
    fi
}
done

echo -e "\n${YELLOW}Failed horizontal groups: ${failed_horizontal[*]}${NC}"

if [[ ${ONLY_HORIZONTAL} -eq 1 ]]; then
    echo -e "\n${YELLOW}========== Only Horizontal Testing Mode ==========${NC}"
    
    # 无失败组的情况
    if [ ${#failed_horizontal[@]} -eq 0 ]; then
        echo -e "\n${GREEN}═══════════════════════════════════════════════════════════════${NC}"
        echo -e "${GREEN}                   ✅ ALL HORIZONTAL TESTS PASSED               ${NC}"
        echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
        
        echo -e "\n📋 ${BLUE}Test Summary (Horizontal Only):${NC}"
        echo "  • Nodes tested: ${valid_nodes}"
        echo "  • Horizontal groups: $((valid_nodes / nodenum))"
        echo "  • Failed groups: 0"
        echo "  • Faulty nodes: None"
        
        # 保存正常节点
        printf '%s\n' "${all_nodes[@]}" | sort -u > "${HEALTHY_FILE}"
        echo -e "\nNormal node list saved to: ${HEALTHY_FILE}"
    else
        # 有失败组的情况：将横向失败组所有节点标记为异常
        intersection_nodes=("${!horizontal_failed_all_nodes[@]}")
        
        echo -e "\n${RED}❌ Detected faulty nodes (horizontal only):${NC}"
        printf '%s\n' "${intersection_nodes[@]}" | sort -u | while read -r node; do
            echo -e "${RED}  - ${node}${NC}"
        done

        # 保存异常节点
        printf '%s\n' "${intersection_nodes[@]}" | sort -u > "${FAULT_FILE}"
        echo -e "\nFault node list saved to: ${FAULT_FILE}"

        # 保存正常节点
        echo -e "\n${GREEN}🔍 Filtering normal nodes...${NC}"
        declare -A fault_node_map
        for node in "${intersection_nodes[@]}"; do
            fault_node_map["$node"]=1
        done
        normal_nodes=()
        for node in "${all_nodes[@]}"; do
            if [[ -z "${fault_node_map[$node]}" ]]; then
                normal_nodes+=("$node")
            fi
        done
        printf '%s\n' "${normal_nodes[@]}" | sort -u > "${HEALTHY_FILE}"
        echo -e "Normal node list saved to: ${HEALTHY_FILE}"
    fi
    total_passed=$((valid_nodes - ${#horizontal_failed_all_nodes[@]}))
    echo -e "\n${BLUE}========== Final Statistics (Horizontal Only) ==========${NC}"
    echo -e "Total nodes checked: ${valid_nodes}"
    echo -e "${GREEN}Nodes passed: ${total_passed}${NC}"
    echo -e "${RED}Faulty nodes: ${#horizontal_failed_all_nodes[@]}${NC}"

    # 计算耗时并退出
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))
    printf "\nTotal time: %02d min %02d sec\n" $((elapsed/60)) $((elapsed%60))
    echo -e "${BLUE}================================${NC}"

    exit 0
fi

# Exit early if no failed groups
if [ ${#failed_horizontal[@]} -eq 0 ]; then
    echo -e "\n${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}                   ✅ ALL TESTS PASSED                          ${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    
    echo -e "\n📋 ${BLUE}Test Summary:${NC}"
    echo "  • Nodes tested: ${valid_nodes}"
    echo "  • Horizontal groups: $((valid_nodes / nodenum))"
    echo "  • Failed groups: 0"
    echo "  • Faulty nodes: None"
    
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))
    printf "\n⏱️  Execution time: %02d min %02d sec\n" $((elapsed/60)) $((elapsed%60))
    
    echo -e "\n${GREEN}✅ No faulty nodes detected. All nodes are healthy.${NC}"
    
    # 【新增】保存正常节点（所有参与测试的节点都是正常节点）
    printf '%s\n' "${all_nodes[@]}" | sort -u > "${HEALTHY_FILE}"
    echo -e "\nNormal node list saved to: ${HEALTHY_FILE}"

    exit 0

fi

# Create Vertical Groups
echo -e "\n${YELLOW}========== Phase 2: Vertical Group Testing ==========${NC}"

# Calculate vertical group size (vertical group size = nodenum)
vertical_groups_num=$((valid_nodes / nodenum))

echo -e "Vertical group size: ${nodenum} nodes per group"
echo -e "Number of vertical groups: ${vertical_groups_num}"

pids=()
for ((v=0; v<vertical_groups_num; v++)); do
    group_index=$((v+1))
    group_name="V${group_index}"
    vertical_hostfile="${SCRIPT_DIR}/hostslice/vertical_host_${group_index}"

    > "${vertical_hostfile}"

    # Vertical grouping: take one node every nodenum nodes
    for ((i=v; i<valid_nodes; i+=vertical_groups_num)); do
        node_idx=$((i + 1))
        sed -n "${node_idx}p" ${HOSTFILE} >> "${vertical_hostfile}"
    done

    node_count=$(wc -l < "${vertical_hostfile}")
    echo "Creating vertical group ${group_name}: ${node_count} nodes"

    # Run test
    ${SCRIPT_DIR}/check_nodes.sh "${vertical_hostfile}" \
        > ${SCRIPT_DIR}/node_checklog/vertical_${group_name}.log 2>&1 &
    pids+=($!)
done

# Wait for all vertical tests to complete
for pid in "${pids[@]}"; do
    wait $pid
done

# Analyze vertical group results
echo -e "\n${YELLOW}Analyzing vertical group results...${NC}"
declare -a failed_vertical=()

for log in ${SCRIPT_DIR}/node_checklog/vertical_V*.log; do
    if [[ ! -f "$log" ]]; then continue; fi
    
    group_name=$(basename "$log" .log | sed 's/vertical_//')
    group_index=${group_name#V}
    host_file="./hostslice/vertical_host_${group_index}"
    
    # Extract TFLOPs value
    tflops=$(grep "TFLOP/s/GPU" "$log" | awk -F':' '{print $6}' | awk '{print $1}' | tail -n 1)
    node_list=$(awk '{print $1}' "${host_file}" | paste -sd,)
    
    if [ -z "${tflops}" ]; then
        echo -e "${RED}[FAIL]${NC} Vertical group ${group_name} [${node_list}]: Test failed or no output"
        failed_vertical+=(${group_index})
        for node in $(awk '{print $1}' "${host_file}"); do
            echo "${node} > vertical_${group_name}" >> ${OUTPUT}/vertical/failed_nodes_${group_name}.txt
        done
    elif [ "$(echo "${tflops} < ${average_tflops}" | bc)" -eq 1 ]; then
        echo -e "${RED}[FAIL]${NC} Vertical group ${group_name} [${node_list}]: ${tflops} TFLOPs < ${average_tflops} TFLOPs"
        failed_vertical+=(${group_index})
        for node in $(awk '{print $1}' "${host_file}"); do
            echo "${node} > vertical_${group_name}" >> ${OUTPUT}/vertical/failed_nodes_${group_name}.txt
        done
    else
        echo -e "${GREEN}[PASS]${NC} Vertical group ${group_name} [${node_list}]: ${tflops} TFLOPs"
    fi
done

# Calculate fault node intersection
echo -e "\n${YELLOW}========== Calculating Fault Node Intersection ==========${NC}"

# Collect all nodes from failed horizontal groups
declare -A horizontal_failed_nodes
if [ ${#failed_horizontal[@]} -gt 0 ]; then
    for h in "${failed_horizontal[@]}"; do
        failed_file="${OUTPUT}/horizontal/failed_nodes_H${h}.txt"
        if [ -f "${failed_file}" ]; then
            while read -r line; do
                node=$(echo "$line" | awk '{print $1}')
                horizontal_failed_nodes["$node"]=1
            done < "${failed_file}"
        fi
    done
fi

# Collect all nodes from failed vertical groups
declare -A vertical_failed_nodes
if [ ${#failed_vertical[@]} -gt 0 ]; then
    for v in "${failed_vertical[@]}"; do
        failed_file="${OUTPUT}/vertical/failed_nodes_V${v}.txt"
        if [ -f "${failed_file}" ]; then
            while read -r line; do
                node=$(echo "$line" | awk '{print $1}')
                vertical_failed_nodes["$node"]=1
            done < "${failed_file}"
        fi
    done
fi

# Calculate intersection
declare -a intersection_nodes=()
for node in "${!horizontal_failed_nodes[@]}"; do
    if [[ -n "${vertical_failed_nodes[$node]}" ]]; then
        intersection_nodes+=("$node")
    fi
done

# Output results
echo -e "\n${BLUE}========== Dual-Stage Test Results Summary ==========${NC}"
echo -e "Failed horizontal groups: ${#failed_horizontal[@]}"
echo -e "Failed vertical groups: ${#failed_vertical[@]}"
echo -e "Fault node candidates: ${#intersection_nodes[@]}"


if [ ${#intersection_nodes[@]} -gt 0 ]; then
    echo -e "\n${RED}❌ Detected faulty nodes:${NC}"
    printf '%s\n' "${intersection_nodes[@]}" | sort -u | while read -r node; do
        echo -e "${RED}  - ${node}${NC}"
    done

    # Save fault node list
    printf '%s\n' "${intersection_nodes[@]}" | sort -u > "${FAULT_FILE}"
    echo -e "\nFault node list saved to: ${FAULT_FILE}"

    # Save normal node list
    echo -e "\n${GREEN}🔍 Filtering normal nodes...${NC}"
    declare -A fault_node_map
    for node in "${intersection_nodes[@]}"; do
        fault_node_map["$node"]=1
    done
    # 遍历所有参与测试的节点，排除故障节点
    normal_nodes=()
    for node in "${all_nodes[@]}"; do
        if [[ -z "${fault_node_map[$node]}" ]]; then
            normal_nodes+=("$node")
        fi
    done
    # 写入正常节点文件（去重+排序）
    printf '%s\n' "${normal_nodes[@]}" | sort -u > "${HEALTHY_FILE}"
    echo -e "Normal node list saved to: ${HEALTHY_FILE}"

    # Generate eviction suggestion
    echo -e "\n${YELLOW}Suggested nodes to evict:${NC}"
    cat ${FAULT_FILE} | while read -r node; do
        echo "  ${node}"
    done
else
    echo -e "\n${GREEN}✅ No clear faulty nodes detected (may be group-level issues)${NC}"
    printf '%s\n' "${all_nodes[@]}" | sort -u > "${HEALTHY_FILE}"
    echo -e "\nNormal node list saved to: ${HEALTHY_FILE}"

    # If there are failed groups but no intersection, output group information
    if [ ${#failed_horizontal[@]} -gt 0 ] || [ ${#failed_vertical[@]} -gt 0 ]; then
        echo -e "\n${YELLOW}Warning: Failed groups detected but no intersecting nodes${NC}"
        echo "Failed horizontal groups: ${failed_horizontal[*]}"
        echo "Failed vertical groups: ${failed_vertical[*]}"
    fi
fi

# Generate detailed report
cat > ${OUTPUT}/diagnosis_report.txt << EOF
Dual-Stage Fault Diagnosis Report
==================================
Test Time: $(date)
Total Nodes: ${total_nodes}
Nodes Participating in Test: ${valid_nodes}
Nodes Per Group: ${nodenum}
Baseline TFLOPs: ${average_tflops}

Phase 1 - Horizontal Groups:
  Total Groups: $((valid_nodes / nodenum))
  Failed Groups: ${failed_horizontal[*]}

Phase 2 - Vertical Groups:
  Total Groups: $((valid_nodes / nodenum))
  Failed Groups: ${failed_vertical[*]}

Fault Nodes (Intersection):
$(printf '%s\n' "${intersection_nodes[@]}" | sort -u | sed 's/^/  /')

Diagnosis Conclusion:
$([ ${#intersection_nodes[@]} -gt 0 ] && echo "Faulty nodes detected,建议驱逐" || echo "No clear faulty nodes detected")
EOF

echo -e "\nDetailed report saved to: ${OUTPUT}/diagnosis_report.txt"

# Final statistics
total_passed=$((valid_nodes - ${#intersection_nodes[@]}))
echo -e "\n${BLUE}========== Final Statistics ==========${NC}"
echo -e "Total nodes checked: ${valid_nodes}"
echo -e "${GREEN}Nodes passed: ${total_passed}${NC}"
echo -e "${RED}Faulty nodes: ${#intersection_nodes[@]}${NC}"

end_time=$(date +%s)
elapsed=$((end_time - start_time))
printf "\nTotal time: %02d min %02d sec\n" $((elapsed/60)) $((elapsed%60))
echo -e "${BLUE}================================${NC}"



