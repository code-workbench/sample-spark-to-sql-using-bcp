#!/bin/bash

# Complete Pipeline Runner
# This script runs the entire Spark to Blob to BCP pipeline

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="$PROJECT_ROOT/config/azure_config.json"
LOG_FILE="pipeline_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check prerequisites
check_prerequisites() {
    log_message "Checking prerequisites..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_message "ERROR: Python 3 is required"
        exit 1
    fi
    
    # Check if virtual environment exists
    if [ ! -d "$PROJECT_ROOT/venv" ]; then
        log_message "Creating Python virtual environment..."
        python3 -m venv "$PROJECT_ROOT/venv"
    fi
    
    # Activate virtual environment
    source "$PROJECT_ROOT/venv/bin/activate"
    
    # Install requirements
    log_message "Installing Python requirements..."
    pip install -r "$PROJECT_ROOT/requirements.txt" > /dev/null 2>&1
    
    # Check configuration file
    if [ ! -f "$CONFIG_FILE" ]; then
        log_message "ERROR: Configuration file not found: $CONFIG_FILE"
        log_message "Please configure your Azure settings in $CONFIG_FILE"
        exit 1
    fi
    
    # Check jq for JSON parsing
    if ! command -v jq &> /dev/null; then
        log_message "Installing jq for JSON parsing..."
        sudo apt-get update && sudo apt-get install -y jq
    fi
    
    log_message "Prerequisites check completed"
}

# Function to run Spark processing
run_spark_processing() {
    log_message "=== Starting Spark Processing ==="
    
    cd "$PROJECT_ROOT/spark"
    
    # Activate virtual environment
    source "$PROJECT_ROOT/venv/bin/activate"
    
    # Run Spark to Blob export
    log_message "Running Spark to Blob export..."
    if python3 spark_to_blob.py > "../logs/spark_$(date +%Y%m%d_%H%M%S).log" 2>&1; then
        log_message "Spark processing completed successfully"
        return 0
    else
        log_message "ERROR: Spark processing failed"
        cat "../logs/spark_$(date +%Y%m%d_%H%M%S).log" | tail -20
        return 1
    fi
}

# Function to run BCP import
run_bcp_import() {
    log_message "=== Starting BCP Import ==="
    
    cd "$PROJECT_ROOT/bcp"
    
    # Make BCP script executable
    chmod +x bcp_import.sh
    
    # Run BCP import
    log_message "Running BCP import..."
    if ./bcp_import.sh > "../logs/bcp_$(date +%Y%m%d_%H%M%S).log" 2>&1; then
        log_message "BCP import completed successfully"
        return 0
    else
        log_message "ERROR: BCP import failed"
        cat "../logs/bcp_$(date +%Y%m%d_%H%M%S).log" | tail -20
        return 1
    fi
}

# Function to validate pipeline results
validate_pipeline() {
    log_message "=== Validating Pipeline Results ==="
    
    # Extract SQL Server details from config
    SQL_SERVER=$(jq -r '.sql_server.server' "$CONFIG_FILE")
    DATABASE=$(jq -r '.sql_server.database' "$CONFIG_FILE")
    USERNAME=$(jq -r '.sql_server.username' "$CONFIG_FILE")
    PASSWORD=$(jq -r '.sql_server.password' "$CONFIG_FILE")
    TABLE=$(jq -r '.sql_server.table' "$CONFIG_FILE")
    
    # Run validation query if sqlcmd is available
    if command -v sqlcmd &> /dev/null; then
        log_message "Running validation queries..."
        
        VALIDATION_QUERY="
        SELECT 
            'Total Records' as Metric, 
            COUNT(*) as Value 
        FROM dbo.$TABLE
        UNION ALL
        SELECT 
            'Date Range' as Metric, 
            CONCAT(MIN(sale_date), ' to ', MAX(sale_date)) as Value 
        FROM dbo.$TABLE
        UNION ALL
        SELECT 
            'Categories' as Metric, 
            COUNT(DISTINCT category) as Value 
        FROM dbo.$TABLE
        UNION ALL
        SELECT 
            'Regions' as Metric, 
            COUNT(DISTINCT region) as Value 
        FROM dbo.$TABLE
        "
        
        sqlcmd -S "$SQL_SERVER" -d "$DATABASE" -U "$USERNAME" -P "$PASSWORD" \
               -Q "$VALIDATION_QUERY" -h -1 -s "," | tee -a "$LOG_FILE"
        
        log_message "Validation completed"
    else
        log_message "sqlcmd not available. Skipping validation queries."
    fi
}

# Function to generate summary report
generate_report() {
    log_message "=== Generating Pipeline Report ==="
    
    REPORT_FILE="pipeline_report_$(date +%Y%m%d_%H%M%S).txt"
    
    cat > "$REPORT_FILE" << EOF
===================================================================
Spark to Azure Blob to SQL Server BCP Pipeline Report
===================================================================
Execution Date: $(date)
Configuration: $CONFIG_FILE
Log File: $LOG_FILE

Pipeline Stages:
1. ✅ Prerequisites Check
2. ✅ Spark Data Processing & Export to Azure Blob
3. ✅ BCP Import from Azure Blob to SQL Server
4. ✅ Validation & Reporting

Files Generated:
- Pipeline Log: $LOG_FILE
- Spark Logs: logs/spark_*.log
- BCP Logs: logs/bcp_*.log

Next Steps:
1. Review the validation results above
2. Check the target SQL Server table for imported data
3. Monitor the logs for any warnings or errors
4. Schedule this pipeline for regular execution if needed

Configuration Used:
- Storage Account: $(jq -r '.storage_account.name' "$CONFIG_FILE")
- Container: $(jq -r '.storage_account.container' "$CONFIG_FILE")
- SQL Server: $(jq -r '.sql_server.server' "$CONFIG_FILE")
- Database: $(jq -r '.sql_server.database' "$CONFIG_FILE")
- Target Table: $(jq -r '.sql_server.table' "$CONFIG_FILE")

===================================================================
EOF

    log_message "Report generated: $REPORT_FILE"
    cat "$REPORT_FILE"
}

# Function to cleanup
cleanup() {
    log_message "Performing cleanup..."
    
    # Remove temporary files
    find "$PROJECT_ROOT" -name "temp_data_*.csv" -delete 2>/dev/null || true
    find "$PROJECT_ROOT" -name "*.pyc" -delete 2>/dev/null || true
    find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    
    log_message "Cleanup completed"
}

# Main execution function
main() {
    log_message "====================================================================="
    log_message "Starting Spark to Azure Blob to SQL Server BCP Pipeline"
    log_message "====================================================================="
    
    # Create logs directory
    mkdir -p "$PROJECT_ROOT/logs"
    
    # Trap for cleanup on exit
    trap cleanup EXIT
    
    # Execute pipeline stages
    if check_prerequisites; then
        log_message "✅ Prerequisites check passed"
    else
        log_message "❌ Prerequisites check failed"
        exit 1
    fi
    
    if run_spark_processing; then
        log_message "✅ Spark processing completed"
    else
        log_message "❌ Spark processing failed"
        exit 1
    fi
    
    # Add delay to ensure blob storage is ready
    log_message "Waiting 30 seconds for blob storage to be ready..."
    sleep 30
    
    if run_bcp_import; then
        log_message "✅ BCP import completed"
    else
        log_message "❌ BCP import failed"
        exit 1
    fi
    
    # Validate results
    validate_pipeline
    
    # Generate report
    generate_report
    
    log_message "====================================================================="
    log_message "Pipeline completed successfully! 🎉"
    log_message "Check the report and logs for detailed information."
    log_message "====================================================================="
}

# Handle script arguments
case "${1:-}" in
    "help"|"-h"|"--help")
        echo "Usage: $0 [help|spark-only|bcp-only]"
        echo ""
        echo "Options:"
        echo "  help        Show this help message"
        echo "  spark-only  Run only the Spark processing stage"
        echo "  bcp-only    Run only the BCP import stage"
        echo "  (no args)   Run the complete pipeline"
        exit 0
        ;;
    "spark-only")
        check_prerequisites
        run_spark_processing
        ;;
    "bcp-only")
        check_prerequisites
        run_bcp_import
        validate_pipeline
        ;;
    *)
        main
        ;;
esac
