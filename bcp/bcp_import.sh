#!/bin/bash

# BCP Import Script for Linux/Unix systems
# This script imports data from Azure Blob Storage into SQL Server using BCP

# Load configuration
CONFIG_FILE="../config/azure_config.json"

# Parse JSON configuration (requires jq)
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required to parse configuration. Please install jq."
    exit 1
fi

# Extract configuration values
SQL_SERVER=$(jq -r '.sql_server.server' "$CONFIG_FILE")
DATABASE=$(jq -r '.sql_server.database' "$CONFIG_FILE")
USERNAME=$(jq -r '.sql_server.username' "$CONFIG_FILE")
PASSWORD=$(jq -r '.sql_server.password' "$CONFIG_FILE")
TABLE=$(jq -r '.sql_server.table' "$CONFIG_FILE")
STORAGE_ACCOUNT=$(jq -r '.storage_account.name' "$CONFIG_FILE")
CONTAINER=$(jq -r '.storage_account.container' "$CONFIG_FILE")
SAS_TOKEN=$(jq -r '.storage_account.sas_token' "$CONFIG_FILE")

# BCP configuration
BATCH_SIZE=$(jq -r '.pipeline.batch_size' "$CONFIG_FILE")
MAX_ERRORS=0
FIELD_TERMINATOR="|"
ROW_TERMINATOR="\n"
FORMAT_FILE="../sql/format_file.fmt"
ERROR_LOG="bcp_import_errors_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "bcp_import.log"
}

# Function to check if BCP is installed
check_bcp() {
    if ! command -v bcp &> /dev/null; then
        log_message "ERROR: BCP utility not found. Please install SQL Server command-line tools."
        echo "To install on Ubuntu/Debian:"
        echo "curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -"
        echo "curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list"
        echo "sudo apt-get update"
        echo "sudo apt-get install mssql-tools18 unixodbc-dev"
        echo "echo 'export PATH=\"\$PATH:/opt/mssql-tools18/bin\"' >> ~/.bashrc"
        exit 1
    fi
}

# Function to get the latest blob file
get_latest_blob_file() {
    log_message "Finding latest data file in blob storage..."
    
    # This is a simplified approach - in production, you'd use Azure CLI or SDK
    # For now, we'll assume the file pattern based on the Spark export
    BLOB_PATTERN="sales_data_aggregated_*/*.csv"
    
    # Construct blob URL with SAS token
    BLOB_URL="https://${STORAGE_ACCOUNT}.blob.core.windows.net/${CONTAINER}/${BLOB_PATTERN}${SAS_TOKEN}"
    
    log_message "Blob URL: ${BLOB_URL}"
    echo "$BLOB_URL"
}

# Function to download blob file locally (alternative approach)
download_blob_file() {
    local blob_url="$1"
    local local_file="temp_data_$(date +%Y%m%d_%H%M%S).csv"
    
    log_message "Downloading blob file to local storage: $local_file"
    
    # Use curl to download the file
    if curl -s -o "$local_file" "$blob_url"; then
        log_message "File downloaded successfully"
        echo "$local_file"
    else
        log_message "ERROR: Failed to download blob file"
        return 1
    fi
}

# Function to run BCP import
run_bcp_import() {
    local data_source="$1"
    local use_url="$2"
    
    log_message "Starting BCP import..."
    log_message "Data source: $data_source"
    log_message "Target: $SQL_SERVER.$DATABASE.dbo.$TABLE"
    
    # Build BCP command
    BCP_CMD="bcp $DATABASE.dbo.$TABLE in"
    
    if [ "$use_url" = "true" ]; then
        BCP_CMD="$BCP_CMD \"$data_source\""
    else
        BCP_CMD="$BCP_CMD $data_source"
    fi
    
    BCP_CMD="$BCP_CMD -S $SQL_SERVER -d $DATABASE -U $USERNAME -P $PASSWORD"
    BCP_CMD="$BCP_CMD -c -t \"$FIELD_TERMINATOR\" -r \"$ROW_TERMINATOR\""
    BCP_CMD="$BCP_CMD -b $BATCH_SIZE -m $MAX_ERRORS -e $ERROR_LOG"
    BCP_CMD="$BCP_CMD -C 65001"  # UTF-8 encoding
    
    # Add format file if it exists
    if [ -f "$FORMAT_FILE" ]; then
        BCP_CMD="$BCP_CMD -f $FORMAT_FILE"
        log_message "Using format file: $FORMAT_FILE"
    fi
    
    log_message "Executing BCP command..."
    log_message "Command: $BCP_CMD"
    
    # Execute BCP command
    if eval "$BCP_CMD"; then
        log_message "BCP import completed successfully"
        
        # Check for errors
        if [ -f "$ERROR_LOG" ] && [ -s "$ERROR_LOG" ]; then
            log_message "WARNING: Errors occurred during import. Check $ERROR_LOG"
            cat "$ERROR_LOG"
        fi
        
        return 0
    else
        log_message "ERROR: BCP import failed"
        if [ -f "$ERROR_LOG" ]; then
            log_message "Error details:"
            cat "$ERROR_LOG"
        fi
        return 1
    fi
}

# Function to verify import
verify_import() {
    log_message "Verifying import..."
    
    # Create a simple verification query
    VERIFY_QUERY="SELECT COUNT(*) as record_count, MIN(sale_date) as min_date, MAX(sale_date) as max_date FROM dbo.$TABLE"
    
    # Use sqlcmd to verify (if available)
    if command -v sqlcmd &> /dev/null; then
        log_message "Running verification query..."
        sqlcmd -S "$SQL_SERVER" -d "$DATABASE" -U "$USERNAME" -P "$PASSWORD" \
               -Q "$VERIFY_QUERY" -h -1 -s "," | tee -a "bcp_import.log"
    else
        log_message "sqlcmd not available. Please verify import manually."
    fi
}

# Function to cleanup temporary files
cleanup() {
    log_message "Cleaning up temporary files..."
    rm -f temp_data_*.csv
    log_message "Cleanup completed"
}

# Main execution
main() {
    log_message "=== BCP Import Process Started ==="
    
    # Check prerequisites
    check_bcp
    
    # Get blob file URL
    BLOB_URL=$(get_latest_blob_file)
    
    # Option 1: Try direct import from blob URL
    log_message "Attempting direct import from blob URL..."
    if run_bcp_import "$BLOB_URL" "true"; then
        log_message "Direct import successful"
    else
        log_message "Direct import failed, trying download approach..."
        
        # Option 2: Download file locally then import
        LOCAL_FILE=$(download_blob_file "$BLOB_URL")
        if [ $? -eq 0 ] && [ -n "$LOCAL_FILE" ]; then
            if run_bcp_import "$LOCAL_FILE" "false"; then
                log_message "Local file import successful"
            else
                log_message "Local file import failed"
                cleanup
                exit 1
            fi
        else
            log_message "Failed to download file"
            exit 1
        fi
    fi
    
    # Verify import
    verify_import
    
    # Cleanup
    cleanup
    
    log_message "=== BCP Import Process Completed ==="
}

# Handle script interruption
trap cleanup EXIT

# Run main function
main "$@"
