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
# SAS_TOKEN is now optional - prefer Azure CLI authentication
SAS_TOKEN=$(jq -r '.storage_account.sas_token // empty' "$CONFIG_FILE")

# BCP configuration
BATCH_SIZE=$(jq -r '.pipeline.batch_size' "$CONFIG_FILE")
MAX_ERRORS=0
FIELD_TERMINATOR="|"  # Use pipe delimiter for BCP (CSV will be converted)
ROW_TERMINATOR="\n"
FORMAT_FILE="../sql/format_file.fmt"
ERROR_LOG="bcp_import_errors_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "bcp_import.log"
}

convert_csv_format() {
    local file_path="$1"
    local converted_file="${file_path}.converted"
    
    # Use direct logging to avoid stdout interference
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Converting CSV format for BCP compatibility..." >> "bcp_import.log"
    
    # Check if file has headers by looking for common column names
    local first_line=$(head -1 "$file_path")
    if echo "$first_line" | grep -qi "category.*region.*sale_date\|category,region,sale_date"; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Detected header row, skipping it..." >> "bcp_import.log"
        # Skip header row and convert commas to pipes
        tail -n +2 "$file_path" | sed 's/,/|/g' > "$converted_file"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] No header detected, converting entire file..." >> "bcp_import.log"
        # Just convert commas to pipes
        sed 's/,/|/g' "$file_path" > "$converted_file"
    fi
    
    # Validate converted file
    if [ ! -f "$converted_file" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to create converted file" >> "bcp_import.log"
        return 1
    fi
    
    local converted_lines=$(wc -l < "$converted_file" 2>/dev/null || echo "0")
    local original_lines=$(wc -l < "$file_path" 2>/dev/null || echo "0")
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Original file lines: $original_lines" >> "bcp_import.log"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Converted file lines: $converted_lines" >> "bcp_import.log"
    
    if [ "$converted_lines" -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Converted file is empty" >> "bcp_import.log"
        return 1
    fi
    
    # Show first few lines of converted file for verification
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] First 3 lines of converted file:" >> "bcp_import.log"
    head -3 "$converted_file" | cat -A >> "bcp_import.log"
    
    # Verify pipe delimiters were added correctly
    local pipe_count=$(head -1 "$converted_file" | tr -cd '|' | wc -c)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Pipe delimiters in first line: $pipe_count" >> "bcp_import.log"
    
    # Output the converted filename to stdout (this should be the only stdout output)
    echo "$converted_file"
    return 0
}

# Function to check Azure CLI authentication
check_azure_auth() {
    log_message "Checking Azure CLI authentication..."
    
    # Check if Azure CLI is installed
    if ! command -v az &> /dev/null; then
        log_message "ERROR: Azure CLI is not installed. Please install Azure CLI first."
        echo "To install Azure CLI:"
        echo "curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash"
        exit 1
    fi
    
    # Check if user is logged in
    if ! az account show &> /dev/null; then
        log_message "ERROR: Not logged into Azure CLI. Please run 'az login' first."
        echo "To authenticate with Azure:"
        echo "az login"
        echo ""
        echo "Or for service principal authentication:"
        echo "az login --service-principal -u <app-id> -p <password> --tenant <tenant-id>"
        exit 1
    fi
    
    # Get current account information
    CURRENT_ACCOUNT=$(az account show --query "{subscriptionId:id, tenantId:tenantId, user:user.name}" --output json 2>/dev/null)
    if [ $? -eq 0 ]; then
        SUBSCRIPTION_ID=$(echo "$CURRENT_ACCOUNT" | jq -r '.subscriptionId')
        TENANT_ID=$(echo "$CURRENT_ACCOUNT" | jq -r '.tenantId')
        USER_NAME=$(echo "$CURRENT_ACCOUNT" | jq -r '.user')
        
        log_message "Authenticated as: $USER_NAME"
        log_message "Subscription: $SUBSCRIPTION_ID"
        log_message "Tenant: $TENANT_ID"
    else
        log_message "WARNING: Could not retrieve account details, but authentication appears valid"
    fi
    
    # Test storage account access
    log_message "Testing storage account access..."
    STORAGE_ACCESS_OUTPUT=$(az storage account show --name "$STORAGE_ACCOUNT" --output json 2>&1)
    STORAGE_ACCESS_EXIT_CODE=$?
    
    if [ $STORAGE_ACCESS_EXIT_CODE -eq 0 ]; then
        log_message "Successfully verified access to storage account: $STORAGE_ACCOUNT"
        
        # Get storage account details for troubleshooting
        RESOURCE_GROUP=$(echo "$STORAGE_ACCESS_OUTPUT" | jq -r '.resourceGroup')
        LOCATION=$(echo "$STORAGE_ACCESS_OUTPUT" | jq -r '.location')
        log_message "Storage account resource group: $RESOURCE_GROUP"
        log_message "Storage account location: $LOCATION"
    else
        log_message "WARNING: Cannot access storage account '$STORAGE_ACCOUNT' with current credentials"
        log_message "Storage account access error:"
        echo "$STORAGE_ACCESS_OUTPUT" | tee -a "bcp_import.log"
        log_message "This could be due to insufficient permissions or the account doesn't exist"
        log_message "Required permissions: Storage Blob Data Reader or Storage Blob Data Contributor"
    fi
    
    # Test container access specifically
    log_message "Testing container access..."
    CONTAINER_ACCESS_OUTPUT=$(az storage container show \
        --account-name "$STORAGE_ACCOUNT" \
        --name "$CONTAINER" \
        --auth-mode login \
        --output json 2>&1)
    CONTAINER_ACCESS_EXIT_CODE=$?
    
    if [ $CONTAINER_ACCESS_EXIT_CODE -eq 0 ]; then
        log_message "Successfully verified access to container: $CONTAINER"
    else
        log_message "WARNING: Cannot access container '$CONTAINER' in storage account '$STORAGE_ACCOUNT'"
        log_message "Container access error:"
        echo "$CONTAINER_ACCESS_OUTPUT" | tee -a "bcp_import.log"
        log_message "Check if the container exists and you have the required permissions"
    fi
}

# Function to diagnose Azure CLI storage permissions
diagnose_storage_permissions() {
    log_message "=== Diagnosing Azure CLI Storage Permissions ==="
    
    # Test basic Azure CLI functionality
    log_message "Testing basic Azure CLI functionality..."
    az version --output table 2>/dev/null | tee -a "bcp_import.log"
    
    # Check current subscription and user context
    log_message "Current Azure context:"
    az account show --output table 2>/dev/null | tee -a "bcp_import.log"
    
    # List all storage accounts user has access to
    log_message "Storage accounts accessible to current user:"
    ACCESSIBLE_ACCOUNTS=$(az storage account list --query "[].{name:name, resourceGroup:resourceGroup, location:location}" --output table 2>&1)
    if [ $? -eq 0 ]; then
        echo "$ACCESSIBLE_ACCOUNTS" | tee -a "bcp_import.log"
        
        # Check if our target storage account is in the list
        if echo "$ACCESSIBLE_ACCOUNTS" | grep -q "$STORAGE_ACCOUNT"; then
            log_message "✓ Target storage account '$STORAGE_ACCOUNT' found in accessible accounts"
        else
            log_message "✗ Target storage account '$STORAGE_ACCOUNT' NOT found in accessible accounts"
            log_message "This indicates a permission or naming issue"
        fi
    else
        log_message "Failed to list storage accounts:"
        echo "$ACCESSIBLE_ACCOUNTS" | tee -a "bcp_import.log"
    fi
    
    # Test storage account existence and permissions
    log_message "Testing storage account permissions..."
    STORAGE_PERMS_OUTPUT=$(az role assignment list \
        --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/*/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT" \
        --assignee "$USER_NAME" \
        --query "[].{roleDefinitionName:roleDefinitionName, scope:scope}" \
        --output table 2>&1)
    
    if [ $? -eq 0 ]; then
        log_message "Role assignments for storage account:"
        echo "$STORAGE_PERMS_OUTPUT" | tee -a "bcp_import.log"
    else
        log_message "Could not retrieve role assignments (this may be normal):"
        echo "$STORAGE_PERMS_OUTPUT" | tee -a "bcp_import.log"
    fi
    
    # Test specific blob operations
    log_message "Testing blob list operation with detailed error output..."
    BLOB_TEST_OUTPUT=$(az storage blob list \
        --account-name "$STORAGE_ACCOUNT" \
        --container-name "$CONTAINER" \
        --auth-mode login \
        --output json \
        --debug 2>&1)
    BLOB_TEST_EXIT_CODE=$?
    
    log_message "Blob list test exit code: $BLOB_TEST_EXIT_CODE"
    if [ $BLOB_TEST_EXIT_CODE -ne 0 ]; then
        log_message "Detailed error output:"
        echo "$BLOB_TEST_OUTPUT" | tail -20 | tee -a "bcp_import.log"
    fi
    
    log_message "=== End Diagnosis ==="
}

# Function to check if BCP is installed
check_bcp() {
    # Check for SQL Server BCP utility specifically
    BCP_PATH=""
    
    # Common paths for SQL Server BCP
    POSSIBLE_PATHS=(
        "/opt/mssql-tools/bin/bcp"
        "/opt/mssql-tools18/bin/bcp"
        "/usr/local/bin/bcp"
        "$(which bcp 2>/dev/null)"
    )
    
    for path in "${POSSIBLE_PATHS[@]}"; do
        if [ -n "$path" ] && [ -x "$path" ]; then
            # Test if this is SQL Server BCP by checking help output
            if "$path" -? 2>&1 | grep -q "Microsoft"; then
                BCP_PATH="$path"
                log_message "Found SQL Server BCP at: $BCP_PATH"
                break
            fi
        fi
    done
    
    if [ -z "$BCP_PATH" ]; then
        log_message "ERROR: SQL Server BCP utility not found. Found different bcp command:"
        bcp --help 2>&1 | head -5 | tee -a "bcp_import.log"
        log_message "Please install SQL Server command-line tools."
        echo "To install on Ubuntu/Debian:"
        echo "curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -"
        echo "curl https://packages.microsoft.com/config/ubuntu/\$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list"
        echo "sudo apt-get update"
        echo "sudo apt-get install mssql-tools18 unixodbc-dev"
        echo "echo 'export PATH=\"\$PATH:/opt/mssql-tools18/bin\"' >> ~/.bashrc"
        exit 1
    fi
}

# Function to get the latest blob file
get_latest_blob_file() {
    # Primary approach: Use Azure CLI with logged-in context
    if command -v az &> /dev/null; then
        # Try Azure CLI without explicit authentication first (uses logged-in context)
        BLOB_LIST_OUTPUT=$(az storage blob list \
            --account-name "$STORAGE_ACCOUNT" \
            --container-name "$CONTAINER" \
            --prefix "sales_data_aggregated_" \
            --query "[?ends_with(name, '.csv')].{name:name, lastModified:properties.lastModified}" \
            --output json \
            --auth-mode login 2>&1)
        BLOB_LIST_EXIT_CODE=$?
        
        if [ $BLOB_LIST_EXIT_CODE -eq 0 ] && [ -n "$BLOB_LIST_OUTPUT" ] && [ "$BLOB_LIST_OUTPUT" != "[]" ]; then
            BLOB_LIST="$BLOB_LIST_OUTPUT"
            # Get the most recent file
            LATEST_BLOB=$(echo "$BLOB_LIST" | jq -r 'sort_by(.lastModified) | reverse | .[0].name')
            if [ -n "$LATEST_BLOB" ] && [ "$LATEST_BLOB" != "null" ]; then
                # Return blob name only - download function will handle the URL
                echo "$LATEST_BLOB"
                return 0
            fi
        else
            # Log errors to stderr so they don't interfere with return value
            echo "Azure CLI with logged-in context failed. Exit code: $BLOB_LIST_EXIT_CODE" >&2
            echo "Azure CLI output/error:" >&2
            echo "$BLOB_LIST_OUTPUT" >&2
            
            # If it's a permissions error, offer to run diagnostics
            if echo "$BLOB_LIST_OUTPUT" | grep -i "permission\|forbidden\|unauthorized\|access.*denied" > /dev/null; then
                echo "Detected permission-related error. Running diagnostics..." >&2
                diagnose_storage_permissions >&2
            fi
            
            echo "Trying SAS token fallback..." >&2
            
            # Fallback: Use SAS token if available
            if [[ -n "$SAS_TOKEN" && "$SAS_TOKEN" != "null" && "$SAS_TOKEN" != "empty" ]]; then
                # Remove leading ? from SAS token if present
                CLEAN_SAS_TOKEN="${SAS_TOKEN#?}"
                
                BLOB_LIST=$(az storage blob list \
                    --account-name "$STORAGE_ACCOUNT" \
                    --container-name "$CONTAINER" \
                    --sas-token "$CLEAN_SAS_TOKEN" \
                    --prefix "sales_data_aggregated_" \
                    --query "[?ends_with(name, '.csv')].{name:name, lastModified:properties.lastModified}" \
                    --output json 2>/dev/null)
                
                if [ $? -eq 0 ] && [ -n "$BLOB_LIST" ] && [ "$BLOB_LIST" != "[]" ]; then
                    LATEST_BLOB=$(echo "$BLOB_LIST" | jq -r 'sort_by(.lastModified) | reverse | .[0].name')
                    if [ -n "$LATEST_BLOB" ] && [ "$LATEST_BLOB" != "null" ]; then
                        echo "$LATEST_BLOB"
                        return 0
                    fi
                fi
            fi
        fi
    fi
    
    # Fallback: construct a pattern-based blob name
    local current_date=$(date +%Y%m%d)
    
    # Try a few recent dates
    for days_back in 0 1 2 3 4 5 6 7; do
        local target_date=$(date -d "$days_back days ago" +%Y%m%d 2>/dev/null || date -v-${days_back}d +%Y%m%d 2>/dev/null)
        local blob_pattern="sales_data_aggregated_${target_date}_000000/part-00000.csv"
        
        echo "$blob_pattern"
        return 0
    done
}

# Function to validate downloaded file
validate_data_file() {
    local file_path="$1"
    
    log_message "Validating downloaded file: $file_path"
    
    # Check if file exists and is not empty
    if [ ! -f "$file_path" ]; then
        log_message "ERROR: File does not exist: $file_path"
        return 1
    fi
    
    if [ ! -s "$file_path" ]; then
        log_message "ERROR: File is empty: $file_path"
        return 1
    fi
    
    # Get file size and basic info
    local file_size=$(stat -c%s "$file_path" 2>/dev/null || stat -f%z "$file_path" 2>/dev/null)
    local line_count=$(wc -l < "$file_path" 2>/dev/null || echo "unknown")
    
    log_message "File size: $file_size bytes"
    log_message "Line count: $line_count"
    
    # Check for common file corruption indicators
    if [ "$file_size" -lt 10 ]; then
        log_message "WARNING: File appears too small ($file_size bytes)"
    fi
    
    # Check file encoding and format
    log_message "File type information:"
    file "$file_path" | tee -a "bcp_import.log"
    
    # Show first few lines for format validation
    log_message "First 5 lines of file:"
    head -5 "$file_path" | cat -A | tee -a "bcp_import.log"
    
    # Show last few lines to check for proper termination
    log_message "Last 5 lines of file:"
    tail -5 "$file_path" | cat -A | tee -a "bcp_import.log"
    
    # Check for proper line terminators
    local line_endings=$(file "$file_path" | grep -o "CRLF\|LF\|CR" || echo "unknown")
    log_message "Line endings detected: $line_endings"
    
    # Validate field separators on first data line
    if [ "$line_count" != "unknown" ] && [ "$line_count" -gt 0 ]; then
        local first_line=$(head -1 "$file_path")
        local field_count=$(echo "$first_line" | tr -cd "$FIELD_TERMINATOR" | wc -c)
        log_message "Field separators ('$FIELD_TERMINATOR') found in first line: $field_count"
        
        # Show the first line with visible separators
        log_message "First line with visible separators:"
        echo "$first_line" | sed "s/$FIELD_TERMINATOR/[SEP]/g" | tee -a "bcp_import.log"
    fi
    
    # Check for binary content that might indicate corruption
    if file "$file_path" | grep -q "binary\|executable\|archive"; then
        log_message "WARNING: File appears to contain binary data, which may cause BCP issues"
        log_message "First 100 bytes in hex:"
        head -c 100 "$file_path" | hexdump -C | tee -a "bcp_import.log"
        return 1
    fi
    
    # Check for common CSV issues
    local null_bytes=$(tr -d '\0' < "$file_path" | wc -c)
    local original_size=$(wc -c < "$file_path")
    if [ "$null_bytes" -ne "$original_size" ]; then
        log_message "WARNING: File contains null bytes which can cause BCP EOF errors"
        log_message "Removing null bytes from file..."
        tr -d '\0' < "$file_path" > "${file_path}.clean"
        mv "${file_path}.clean" "$file_path"
        log_message "Null bytes removed. New file size: $(stat -c%s "$file_path" 2>/dev/null || stat -f%z "$file_path" 2>/dev/null) bytes"
    fi
    
    return 0
}

# Function to download blob file locally
download_blob_file() {
    local blob_name="$1"
    local local_file="temp_data_$(date +%Y%m%d_%H%M%S).csv"
    
    # Log messages to stderr to avoid mixing with return value
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Downloading blob file '$blob_name' to local storage: $local_file" | tee -a "bcp_import.log" >&2
    
    # Primary approach: Use Azure CLI with logged-in context
    if command -v az &> /dev/null; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Attempting download using Azure CLI with logged-in context..." | tee -a "bcp_import.log" >&2
        
        # Try Azure CLI download with logged-in context first
        if az storage blob download \
            --account-name "$STORAGE_ACCOUNT" \
            --container-name "$CONTAINER" \
            --name "$blob_name" \
            --file "$local_file" \
            --auth-mode login \
            --output none >/dev/null 2>&1; then
            
            if [ -f "$local_file" ] && [ -s "$local_file" ]; then
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] File downloaded successfully using Azure CLI. Size: $(stat -c%s "$local_file") bytes" | tee -a "bcp_import.log" >&2
                echo "$local_file"
                return 0
            fi
        fi
        
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Azure CLI with logged-in context failed, trying SAS token fallback..." | tee -a "bcp_import.log" >&2
        
        # Fallback: Use SAS token if available
        if [[ -n "$SAS_TOKEN" && "$SAS_TOKEN" != "null" && "$SAS_TOKEN" != "empty" ]]; then
            # Clean SAS token
            local clean_sas_token="${SAS_TOKEN#?}"
            
            # Try Azure CLI download with SAS token
            if az storage blob download \
                --account-name "$STORAGE_ACCOUNT" \
                --container-name "$CONTAINER" \
                --name "$blob_name" \
                --file "$local_file" \
                --sas-token "$clean_sas_token" \
                --output none >/dev/null 2>&1; then
                
                if [ -f "$local_file" ] && [ -s "$local_file" ]; then
                    echo "[$(date '+%Y-%m-%d %H:%M:%S')] File downloaded successfully using Azure CLI with SAS token. Size: $(stat -c%s "$local_file") bytes" | tee -a "bcp_import.log" >&2
                    echo "$local_file"
                    return 0
                fi
            fi
        fi
        
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] All Azure CLI download methods failed, trying curl as last resort..." | tee -a "bcp_import.log" >&2
    fi
    
    # Last resort: Fallback to curl if SAS token is available
    if [[ -n "$SAS_TOKEN" && "$SAS_TOKEN" != "null" && "$SAS_TOKEN" != "empty" ]]; then
        local blob_url="https://${STORAGE_ACCOUNT}.blob.core.windows.net/${CONTAINER}/${blob_name}${SAS_TOKEN}"
        
        # Test connectivity first
        local host_only="${STORAGE_ACCOUNT}.blob.core.windows.net"
        if ! nslookup "$host_only" &>/dev/null; then
            log_message "ERROR: Cannot resolve hostname: $host_only"
            return 1
        fi
        
        # Try curl download
        if curl -L --fail -o "$local_file" "$blob_url" >/dev/null 2>&1; then
            if [ -f "$local_file" ] && [ -s "$local_file" ]; then
                log_message "File downloaded successfully using curl. Size: $(stat -c%s "$local_file") bytes"
                echo "$local_file"
                return 0
            fi
        fi
        
        log_message "ERROR: Failed to download blob file using curl"
        # Clean up failed download
        rm -f "$local_file"
    else
        log_message "ERROR: No SAS token available for curl fallback"
    fi
    
    return 1
}

# Function to run BCP import
run_bcp_import() {
    local data_source="$1"
    local use_url="$2"
    
    log_message "Starting BCP import..."
    log_message "Data source: $data_source"
    log_message "Target: $SQL_SERVER.$DATABASE.dbo.$TABLE"
    
    # Build BCP command using the detected path
    BCP_CMD="\"$BCP_PATH\" $TABLE in"
    
    if [ "$use_url" = "true" ]; then
        BCP_CMD="$BCP_CMD \"$data_source\""
    else
        BCP_CMD="$BCP_CMD $data_source"
    fi
    
    BCP_CMD="$BCP_CMD -S $SQL_SERVER -d $DATABASE -U $USERNAME -P $PASSWORD"
    BCP_CMD="$BCP_CMD -t \"$FIELD_TERMINATOR\" -r \"$ROW_TERMINATOR\""
    BCP_CMD="$BCP_CMD -b $BATCH_SIZE -m $MAX_ERRORS -e $ERROR_LOG"
    BCP_CMD="$BCP_CMD -C 65001"  # UTF-8 encoding
    
    # Add format file if it exists, otherwise use character mode
    if [ -f "$FORMAT_FILE" ]; then
        BCP_CMD="$BCP_CMD -f $FORMAT_FILE"
        log_message "Using format file: $FORMAT_FILE"
    else
        BCP_CMD="$BCP_CMD -c"
        log_message "Using character mode (no format file found)"
    fi
    
    log_message "Executing BCP command..."
    log_message "Command: $BCP_CMD"
    
    # Execute BCP command and capture output
    BCP_OUTPUT=$(eval "$BCP_CMD" 2>&1)
    BCP_EXIT_CODE=$?
    
    if [ $BCP_EXIT_CODE -eq 0 ]; then
        log_message "BCP import completed successfully"
        log_message "BCP output:"
        echo "$BCP_OUTPUT" | tee -a "bcp_import.log"
        
        # Check for errors
        if [ -f "$ERROR_LOG" ] && [ -s "$ERROR_LOG" ]; then
            log_message "WARNING: Errors occurred during import. Check $ERROR_LOG"
            cat "$ERROR_LOG"
        fi
        
        return 0
    else
        log_message "ERROR: BCP import failed with exit code: $BCP_EXIT_CODE"
        log_message "BCP output:"
        echo "$BCP_OUTPUT" | tee -a "bcp_import.log"
        
        if [ -f "$ERROR_LOG" ]; then
            log_message "Error log contents:"
            cat "$ERROR_LOG" | tee -a "bcp_import.log"
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
    rm -f temp_data_*.csv.converted
    log_message "Cleanup completed"
}

# Main execution
main() {
    log_message "=== BCP Import Process Started ==="
    
    # Check Azure CLI authentication first
    check_azure_auth
    
    # Check prerequisites
    check_bcp
    
    # Get blob file name
    log_message "Finding latest data file in blob storage..."
    BLOB_NAME=$(get_latest_blob_file)
    
    if [ -n "$BLOB_NAME" ]; then
        log_message "Found latest blob: $BLOB_NAME"
        
        # Download the blob file once
        log_message "Downloading blob file to local storage..."
        LOCAL_FILE=$(download_blob_file "$BLOB_NAME")
        DOWNLOAD_EXIT_CODE=$?
        
        if [ $DOWNLOAD_EXIT_CODE -eq 0 ] && [ -n "$LOCAL_FILE" ] && [ -f "$LOCAL_FILE" ]; then
            log_message "Successfully downloaded file: $LOCAL_FILE"
            
            # Validate the downloaded file before BCP import
            if validate_data_file "$LOCAL_FILE"; then
                log_message "File validation passed. Converting format for BCP compatibility..."
                
                # Convert CSV format to pipe-delimited format
                CONVERTED_FILE=$(convert_csv_format "$LOCAL_FILE")
                CONVERT_EXIT_CODE=$?
                
                log_message "Conversion function returned: '$CONVERTED_FILE', exit code: $CONVERT_EXIT_CODE"
                
                if [ $CONVERT_EXIT_CODE -eq 0 ] && [ -n "$CONVERTED_FILE" ] && [ -f "$CONVERTED_FILE" ] && [ -s "$CONVERTED_FILE" ]; then
                    log_message "Format conversion successful. Proceeding with BCP import..."
                    
                    # Run BCP import with the converted file
                    if run_bcp_import "$CONVERTED_FILE" "false"; then
                        log_message "BCP import successful"
                        
                        # Verify import
                        verify_import
                    else
                        log_message "BCP import failed"
                        cleanup
                        exit 1
                    fi
                else
                    log_message "Format conversion failed"
                    cleanup
                    exit 1
                fi
            else
                log_message "File validation failed - cannot proceed with BCP import"
                cleanup
                exit 1
            fi
        else
            log_message "Failed to download file - cannot proceed with BCP import"
            log_message "Download exit code: $DOWNLOAD_EXIT_CODE, Local file: '$LOCAL_FILE', File exists: $([ -f "$LOCAL_FILE" ] && echo "yes" || echo "no")"
            exit 1
        fi
    else
        log_message "Failed to find blob file"
        exit 1
    fi
    
    # Cleanup
    cleanup
    
    log_message "=== BCP Import Process Completed ==="
}

# Handle script interruption
trap cleanup EXIT

# Run main function
main "$@"
