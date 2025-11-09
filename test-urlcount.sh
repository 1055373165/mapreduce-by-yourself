#!/bin/bash

echo "=== MapReduce URL Access Statistics Test ==="

# Clean up old files
rm -f mr-out-* mr-*-*

# Start Coordinator with access log files
echo "Starting Coordinator..."
./mrcoordinator access-log-*.txt &
COORDINATOR_PID=$!

sleep 1

# Start 3 Workers for URL count application
echo "Starting Workers..."
./mrworker urlcount &
WORKER1_PID=$!

./mrworker urlcount &
WORKER2_PID=$!

./mrworker urlcount &
WORKER3_PID=$!

# Wait for completion
wait $COORDINATOR_PID

echo "Job completed!"

# Merge output and sort by count
cat mr-out-* | sort -t$'\t' -k2 -nr > urlcount-output.txt

echo "Results written to urlcount-output.txt"

# Clean up
kill $WORKER1_PID $WORKER2_PID $WORKER3_PID 2>/dev/null

# Display results
echo "=== URL Access Statistics ==="
cat urlcount-output.txt
