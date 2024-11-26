#!/bin/bash

# Paths to configuration files
MASTER_CONFIG="/path/to/master/config.json"
WORKER_CONFIG_DIR="/path/to/worker/configs/"

# Path to Python scripts for Master and Worker nodes
PYTHON_SCRIPT="/path/to/your/python_script.py"

# Function to start the master node
start_master() {
    echo "Starting Master Node..."
    python3 $PYTHON_SCRIPT --config $MASTER_CONFIG --role master &
    MASTER_PID=$!
    echo "Master Node started with PID $MASTER_PID"
}

# Function to start worker nodes
start_workers() {
    echo "Starting Worker Nodes..."
    for WORKER_CONFIG in "$WORKER_CONFIG_DIR"/*.json; do
        echo "Starting Worker with config $WORKER_CONFIG"
        python3 $PYTHON_SCRIPT --config $WORKER_CONFIG --role worker &
        echo "Worker Node started with PID $!"
    done
}

# Main script execution
echo "Initializing Distributed System"
start_master
start_workers
echo "Distributed System Started"
