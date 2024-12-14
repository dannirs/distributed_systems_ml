#!/bin/bash

# Define directories where config files are stored
CONFIG_DIRS=(
    "/c/Users/Danni/Config1"
    "/c/Users/Danni/Config2"
    "/c/Users/Danni/Config3"
)

# Define job files to execute
JOB_FILES=(
    "C:\Users\Danni\distributed_systems_ml\job3.json"
)

which python

# Step 1: Start the MasterNode and capture its IP and port
echo "Starting MasterNode..."
MASTER_OUTPUT_FILE="/c/Users/Danni/Config4/master_output.log"
(cd /c/Users/Danni/Config4 && python MasterNode.py > "$MASTER_OUTPUT_FILE" &)

# Wait for MasterNode to initialize
sleep 2

# Capture MasterNode output
if [ ! -s "$MASTER_OUTPUT_FILE" ]; then
    echo "MasterNode failed to start. Exiting."
    exit 1
fi
MASTER_INFO=$(cat "$MASTER_OUTPUT_FILE")
echo "MASTER_INFO: $MASTER_INFO"

# Extract MasterNode IP and Port
MASTER_IP=$(echo "$MASTER_INFO" | grep -oP '(?<=IP: ).*(?= Port:)')
MASTER_PORT=$(echo "$MASTER_INFO" | grep -oP '(?<=Port: ).*')

if [ -z "$MASTER_IP" ] || [ -z "$MASTER_PORT" ]; then
    echo "Failed to retrieve MasterNode IP and Port. Exiting."
    exit 1
fi

echo "MasterNode started at IP: $MASTER_IP, Port: $MASTER_PORT"


# Step 2: Initialize a variable to hold all config file paths
CONFIG_FILES=""

echo "Gathering configuration files from specified directories..."

# Loop through directories to find config files
for DIR in "${CONFIG_DIRS[@]}"; do
    if [ -d "$DIR" ]; then
        echo "Processing directory: $DIR"
        FILES=$(find "$DIR" -type f -name "*.json")
        CONFIG_FILES="$CONFIG_FILES $FILES"
    else
        echo "Directory not found: $DIR"
    fi
done

# Trim leading and trailing spaces from CONFIG_FILES
CONFIG_FILES=$(echo "$CONFIG_FILES" | xargs)

# Check if config files were found
if [ -z "$CONFIG_FILES" ]; then
    echo "No configuration files found. Exiting."
    exit 1
fi

echo "Found Config Files: $CONFIG_FILES"

# Step 3: Start WorkerServers with MasterNode IP and Port
for CONFIG_FILE in $CONFIG_FILES; do
    CONFIG_DIR=$(dirname "$CONFIG_FILE")
    echo "Starting WorkerServer in directory: $CONFIG_DIR with MasterNode at $MASTER_IP:$MASTER_PORT"
    
    # Pass MasterNode IP and Port dynamically to Worker.py
    (cd "$CONFIG_DIR" && python Worker.py --master-ip "$MASTER_IP" --master-port "$MASTER_PORT" &)  # Run in background
done
SCRIPT_DIR=$(dirname "$(realpath "$0")")
# Start UserClient with MasterNode details and job files
echo "Starting Client..."
python "$SCRIPT_DIR/Client.py" \
    --master-ip "$MASTER_IP" \
    --master-port "$MASTER_PORT" \
    --job-files "${JOB_FILES[@]}"