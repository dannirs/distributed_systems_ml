#!/bin/bash

MASTER_CONFIG="C:\Users\Danni\Config4\config.json"
WORKER_CONFIG_DIR=["C:\Users\Danni\Config1\config.json","C:\Users\Danni\Config2\config.json","C:\Users\Danni\Config3\config.json\"]
PYTHON_SCRIPT="C:\Users\Danni\distributed_systems_ml\MasterNode.py"

start_master() {
    echo "Starting Master Node..."
    python3 $PYTHON_SCRIPT --config $MASTER_CONFIG --role master &
    MASTER_PID=$!
    echo "Master Node started with PID $MASTER_PID"
}

start_workers() {
    echo "Starting Worker Nodes..."
    for WORKER_CONFIG in "$WORKER_CONFIG_DIR"/*.json; do
        echo "Starting Worker with config $WORKER_CONFIG"
        python3 $PYTHON_SCRIPT --config $WORKER_CONFIG --role worker &
        echo "Worker Node started with PID $!"
    done
}

echo "Initializing Distributed System"
start_master
start_workers
echo "Distributed System Started"
