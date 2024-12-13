#!/bin/bash

# Define directories where config files are stored
CONFIG_DIRS=(
    "/c/Users/Danni/Config1"
    "/c/Users/Danni/Config2"
    "/c/Users/Danni/Config3"
    "/c/Users/Danni/Config4"
)

# Define job files to execute
JOB_FILES=(
    "C:\Users\Danni\distributed_systems_ml\job3.json"
)


# Initialize a variable to hold all config file paths
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

# Run UserClient with the config files and job files
echo "Running UserClient with job files: $JOB_FILES"
python UserClient.py --config_files $CONFIG_FILES <<EOF
file
$JOB_FILES
exit
EOF

