#!/bin/bash

# Define directories where config files are stored
CONFIG_DIRS=(
    "/c/Users/Danni/Config1"
    "/c/Users/Danni/Config2"
    "/c/Users/Danni/Config3"
)

# Define job files to execute
JOB_FILES=(
    "C:/Users/Danni/distributed_systems_ml/job3.json"
)

which python

# Step 1: Start the MasterNode and print output directly to the terminal
echo "Starting MasterNode..."
MASTER_OUTPUT_FILE="/c/Users/Danni/Config4/master_output.log"  # Temporary file for MasterNode output
(cd /c/Users/Danni/Config4 && python -u MasterNode.py | tee "$MASTER_OUTPUT_FILE" 2>&1 &)

# Wait for the file to be created
sleep 2

# Check if the file exists
if [ ! -f "$MASTER_OUTPUT_FILE" ]; then
    echo "Error: MasterNode output file '$MASTER_OUTPUT_FILE' was not created."
    exit 1
fi

echo "MasterNode output is being written to: $MASTER_OUTPUT_FILE"

# Extract MasterNode IP and Port
MASTER_IP="localhost"
MASTER_PORT=$(grep -oP 'Port: \K\d+' "$MASTER_OUTPUT_FILE")

if [ -z "$MASTER_PORT" ]; then
    echo "Failed to retrieve MasterNode port. Exiting."
    exit 1
fi

echo "MasterNode started at IP: $MASTER_IP, Port: $MASTER_PORT"

# Verify MasterNode startup with a simple connectivity check
echo "Checking if MasterNode is listening..."
python - <<EOF
import socket, sys
MASTER_IP = "$MASTER_IP"
MASTER_PORT = int("$MASTER_PORT")

try:
    with socket.create_connection((MASTER_IP, MASTER_PORT), timeout=3):
        print(f"MasterNode is up and listening on {MASTER_IP}:{MASTER_PORT}")
except Exception as e:
    print(f"Failed to connect to MasterNode on {MASTER_IP}:{MASTER_PORT}: {e}")
    sys.exit(1)
EOF

if [ $? -ne 0 ]; then
    echo "MasterNode is not responding. Exiting."
    exit 1
fi

# Define a function to test server connectivity
test_server_connection() {
    local server_ip=$1
    local server_port=$2

    echo "Testing connection to server at $server_ip:$server_port"
    python - <<EOF
import socket, sys
SERVER_IP = "$server_ip"
SERVER_PORT = int("$server_port")

try:
    with socket.create_connection((SERVER_IP, SERVER_PORT), timeout=3):
        print(f"✅ Successfully connected to server at {SERVER_IP}:{SERVER_PORT}")
except Exception as e:
    print(f"❌ Failed to connect to server at {SERVER_IP}:{SERVER_PORT}: {e}")
    sys.exit(1)
EOF

    if [ $? -ne 0 ]; then
        echo "Server connectivity test failed. Exiting."
        exit 1
    fi
}

# Step 2: Start WorkerServers with MasterNode's IP and Port
for DIR in "${CONFIG_DIRS[@]}"; do
    CONFIG_FILE="$DIR/config.json"  # Explicitly reference config.json in each directory

    if [ ! -f "$CONFIG_FILE" ]; then
        echo "No config.json found in directory: $DIR. Skipping."
        continue
    fi

    echo "Starting WorkerServer with configuration: $CONFIG_FILE in directory: $DIR with MasterNode at $MASTER_IP:$MASTER_PORT"

    # Run WorkerServer and explicitly pass the config file
    (cd "$DIR" && python -u Worker.py \
        --master-ip "$MASTER_IP" \
        --master-port "$MASTER_PORT" \
        --config "$CONFIG_FILE" 2>&1) &
done

# Step 3: Start UserClient with MasterNode details and job files
SCRIPT_DIR=$(dirname "$(realpath "$0")")
echo "Starting Client..."
python -u "$SCRIPT_DIR/Client.py" \
    --master-ip "$MASTER_IP" \
    --master-port "$MASTER_PORT" \
    --job-files "${JOB_FILES[@]}"

CLIENT_IP="localhost"
CLIENT_PORT=2001  # Replace with your actual client port if different
test_server_connection "$CLIENT_IP" "$CLIENT_PORT"
echo "Ran job"
