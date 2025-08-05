#!/bin/bash

# Script to delete all .log files recursively
# Usage: ./delete_logs.sh [directory_path]

# Set target directory (default to current directory)
TARGET_DIR="${1:-.}"

echo "Searching for .log files in: $TARGET_DIR"

# Count files first
LOG_COUNT=$(find "$TARGET_DIR" -name "*.log" -type f | wc -l)

if [ "$LOG_COUNT" -eq 0 ]; then
    echo "No .log files found."
    exit 0
fi

echo "Found $LOG_COUNT .log files:"
find "$TARGET_DIR" -name "*.log" -type f

# Ask for confirmation
read -p "Do you want to delete all these .log files? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    find "$TARGET_DIR" -name "*.log" -type f -delete
    echo "All .log files have been deleted."
else
    echo "Operation cancelled."
fi