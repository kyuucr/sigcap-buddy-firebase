#!/bin/bash

# Check for necessary arguments
if [ $# -ne 3 ]; then
    echo "Usage: $0 <source_dir> <destination_dir> <json_file>"
    exit 1
fi

# set -x

source_dir="$1"
destination_dir="$2"
json_file="$3"

# Extract key-value pairs from JSON
key_values=$(jq -r 'to_entries[] | "\(.key)=\(.value)"' "$json_file")

if [[ -f "$source_dir"/last_update.json ]]; then
    last_update=$(jq -r 'to_entries[] | "\(.key)=\(.value)"' "$source_dir"/last_update.json)
    echo "$last_update" > "$destination_dir/__last_update.txt"
fi

# Iterate through files in the source directory
for file in "$source_dir"/*.csv; do
    filename=$(basename "$file")
    if [[ $filename == "device_list.csv" ]]; then
        # Copy device_list.csv directly
        cp "$file" "$destination_dir/__${filename}"
    else
        # Check for matching keys and perform substitution
        for kv in $key_values; do
            key=$(echo "$kv" | cut -d= -f1)
            value=$(echo "$kv" | cut -d= -f2)

            if [[ $filename == *"$value"* ]]; then
                new_filename=${filename/$value/$key}
                cp "$file" "$destination_dir/$new_filename"
                break  # Stop processing once a match is found
            fi
        done
    fi
done