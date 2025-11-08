#!/bin/bash

echo "=== MapReduce Test ==="

# 清理旧文件
rm -f mr-out-* mr-*-*

# 启动 Coordinator
echo "Starting Coordinator..."
go run main/mrcoordinator.go pg-*.txt &
COORDINATOR_PID=$!

sleep 1

# 启动 3 个 Worker
echo "Starting Workers..."
go run main/mrworker.go wc.so &
WORKER1_PID=$!

go run main/mrworker.go wc.so &
WORKER2_PID=$!

go run main/mrworker.go wc.so &
WORKER3_PID=$!

# 等待完成
wait $COORDINATOR_PID

echo "Job completed!"

# 合并输出并按计数值排序
cat mr-out-* | sort | uniq -c | sort -rn > mr-output.txt

echo "Results written to mr-output.txt"

# 清理
kill $WORKER1_PID $WORKER2_PID $WORKER3_PID 2>/dev/null

# 显示前 10 行结果
echo "=== Top 100 words ==="
head -100 mr-output.txt