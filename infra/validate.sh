#!/bin/bash

# Infrastructure Validation Script
# This script validates that the Azure infrastructure is properly deployed

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/../config/azure_config.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to check if jq is installed
check_jq() {
    if ! command -v jq &> /dev/null; then
        print_message $RED "❌ jq is required but not installed"
        exit 1
    fi
}

# Function to validate configuration file
validate_config() {
    print_message $BLUE "Validating configuration file..."
    
    if [ ! -f "$CONFIG_FILE" ]; then
        print_message $RED "❌ Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
    
    # Extract configuration values
    STORAGE_ACCOUNT_NAME=$(jq -r '.storage_account.name' "$CONFIG_FILE")
    CONTAINER_NAME=$(jq -r '.storage_account.container' "$CONFIG_FILE")
    SQL_SERVER=$(jq -r '.sql_server.server' "$CONFIG_FILE")
    SQL_DATABASE=$(jq -r '.sql_server.database' "$CONFIG_FILE")
    
    if [ "$STORAGE_ACCOUNT_NAME" == "null" ] || [ "$STORAGE_ACCOUNT_NAME" == "YOUR_STORAGE_ACCOUNT_NAME" ]; then
        print_message $RED "❌ Storage account name not configured"
        return 1
    fi
    
    if [ "$SQL_SERVER" == "null" ] || [ "$SQL_SERVER" == "your-server.database.windows.net" ]; then
        print_message $RED "❌ SQL server not configured"
        return 1
    fi
    
    print_message $GREEN "✅ Configuration file is valid"
    print_message $GREEN "   Storage Account: $STORAGE_ACCOUNT_NAME"
    print_message $GREEN "   Container: $CONTAINER_NAME"
    print_message $GREEN "   SQL Server: $SQL_SERVER"
    print_message $GREEN "   Database: $SQL_DATABASE"
    
    return 0
}

# Function to test storage account access
test_storage_access() {
    print_message $BLUE "Testing storage account access..."
    
    CONNECTION_STRING=$(jq -r '.storage_account.connection_string' "$CONFIG_FILE")
    
    if [ "$CONNECTION_STRING" == "null" ] || [ -z "$CONNECTION_STRING" ]; then
        print_message $YELLOW "⚠️  Storage connection string not found, skipping storage test"
        return 0
    fi
    
    # Test with Azure CLI if available
    if command -v az &> /dev/null; then
        if az storage container show --name "$CONTAINER_NAME" --connection-string "$CONNECTION_STRING" &> /dev/null; then
            print_message $GREEN "✅ Storage container is accessible"
        else
            print_message $RED "❌ Cannot access storage container"
            return 1
        fi
    else
        print_message $YELLOW "⚠️  Azure CLI not available, skipping storage test"
    fi
    
    return 0
}

# Function to test SQL server connection
test_sql_connection() {
    print_message $BLUE "Testing SQL server connection..."
    
    SQL_SERVER=$(jq -r '.sql_server.server' "$CONFIG_FILE")
    SQL_DATABASE=$(jq -r '.sql_server.database' "$CONFIG_FILE")
    SQL_USERNAME=$(jq -r '.sql_server.username' "$CONFIG_FILE")
    SQL_PASSWORD=$(jq -r '.sql_server.password' "$CONFIG_FILE")
    
    if [ "$SQL_PASSWORD" == "null" ] || [ "$SQL_PASSWORD" == "your_password" ]; then
        print_message $YELLOW "⚠️  SQL password not configured, skipping SQL test"
        return 0
    fi
    
    # Test with sqlcmd if available
    if command -v sqlcmd &> /dev/null; then
        if sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -Q "SELECT 1" -C &> /dev/null; then
            print_message $GREEN "✅ SQL Server connection is working"
            
            # Check if table exists
            TABLE_CHECK=$(sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USERNAME" -P "$SQL_PASSWORD" -Q "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'sales_data'" -h -1 -C 2>/dev/null | tr -d ' ' || echo "0")
            
            if [ "$TABLE_CHECK" == "1" ]; then
                print_message $GREEN "✅ Target table 'sales_data' exists"
            else
                print_message $YELLOW "⚠️  Target table 'sales_data' not found"
                print_message $YELLOW "   Run: sqlcmd -S $SQL_SERVER -d $SQL_DATABASE -U $SQL_USERNAME -P [password] -i ../sql/create_table.sql -C"
            fi
        else
            print_message $RED "❌ Cannot connect to SQL Server"
            return 1
        fi
    else
        print_message $YELLOW "⚠️  sqlcmd not available, skipping SQL connection test"
    fi
    
    return 0
}

# Function to validate pipeline prerequisites
validate_prerequisites() {
    print_message $BLUE "Validating pipeline prerequisites..."
    
    local all_good=true
    
    # Check Python
    if command -v python3 &> /dev/null; then
        print_message $GREEN "✅ Python 3 is available"
    else
        print_message $RED "❌ Python 3 not found"
        all_good=false
    fi
    
    # Check virtual environment
    if [ -d "$SCRIPT_DIR/../venv" ]; then
        print_message $GREEN "✅ Python virtual environment exists"
    else
        print_message $YELLOW "⚠️  Virtual environment not found (run setup_environment.sh)"
    fi
    
    # Check Java
    if command -v java &> /dev/null; then
        JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2)
        print_message $GREEN "✅ Java is available: $JAVA_VERSION"
    else
        print_message $RED "❌ Java not found (required for Spark)"
        all_good=false
    fi
    
    # Check BCP
    if command -v bcp &> /dev/null; then
        print_message $GREEN "✅ BCP utility is available"
    else
        print_message $YELLOW "⚠️  BCP utility not found (install mssql-tools)"
    fi
    
    if [ "$all_good" = true ]; then
        return 0
    else
        return 1
    fi
}

# Function to generate validation report
generate_report() {
    print_message $BLUE "Generating validation report..."
    
    REPORT_FILE="$SCRIPT_DIR/validation_report_$(date +%Y%m%d_%H%M%S).txt"
    
    cat > "$REPORT_FILE" << EOF
=================================================================
Infrastructure Validation Report
=================================================================
Validation Date: $(date)
Configuration File: $CONFIG_FILE

Configuration Status:
------------------------------------------------------------------
$(validate_config 2>&1 | sed 's/\x1b\[[0-9;]*m//g')

Storage Access:
------------------------------------------------------------------
$(test_storage_access 2>&1 | sed 's/\x1b\[[0-9;]*m//g')

SQL Connection:
------------------------------------------------------------------
$(test_sql_connection 2>&1 | sed 's/\x1b\[[0-9;]*m//g')

Prerequisites:
------------------------------------------------------------------
$(validate_prerequisites 2>&1 | sed 's/\x1b\[[0-9;]*m//g')

Next Steps:
------------------------------------------------------------------
If all validations passed:
  1. Run the pipeline: ../scripts/run_pipeline.sh
  2. Monitor logs for any issues
  3. Verify data in SQL Server after completion

If validations failed:
  1. Check Azure resource deployment
  2. Verify configuration file values
  3. Ensure all prerequisites are installed
  4. Re-run infrastructure deployment if needed

=================================================================
EOF
    
    print_message $GREEN "✅ Validation report created: $REPORT_FILE"
}

# Main validation function
main() {
    print_message $BLUE "=============================================="
    print_message $BLUE "Infrastructure Validation for Spark to BCP Pipeline"
    print_message $BLUE "=============================================="
    
    check_jq
    
    local validation_passed=true
    
    if ! validate_config; then
        validation_passed=false
    fi
    
    if ! test_storage_access; then
        validation_passed=false
    fi
    
    if ! test_sql_connection; then
        validation_passed=false
    fi
    
    if ! validate_prerequisites; then
        validation_passed=false
    fi
    
    generate_report
    
    print_message $BLUE "=============================================="
    if [ "$validation_passed" = true ]; then
        print_message $GREEN "🎉 All validations passed! Ready to run pipeline."
        print_message $GREEN "=============================================="
        exit 0
    else
        print_message $YELLOW "⚠️  Some validations failed. Check the report for details."
        print_message $YELLOW "=============================================="
        exit 1
    fi
}

# Handle script arguments
case "${1:-}" in
    "help"|"-h"|"--help")
        echo "Usage: $0 [help|config-only|storage-only|sql-only]"
        echo ""
        echo "Options:"
        echo "  help        Show this help message"
        echo "  config-only Test only configuration file"
        echo "  storage-only Test only storage account access"
        echo "  sql-only    Test only SQL server connection"
        echo "  (no args)   Run all validations"
        exit 0
        ;;
    "config-only")
        check_jq
        validate_config
        ;;
    "storage-only")
        check_jq
        test_storage_access
        ;;
    "sql-only")
        check_jq
        test_sql_connection
        ;;
    *)
        main
        ;;
esac
