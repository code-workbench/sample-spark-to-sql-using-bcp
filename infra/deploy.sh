#!/bin/bash

# Azure Infrastructure Deployment Script
# This script deploys the Azure resources needed for the Spark to BCP pipeline

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOURCE_GROUP_NAME=""
LOCATION="usgovirginia"
DEPLOYMENT_NAME="sparkbcp-deployment-$(date +%Y%m%d-%H%M%S)"
SUBSCRIPTION_ID=""

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

# Function to check prerequisites
check_prerequisites() {
    print_message $BLUE "Checking prerequisites..."
    
    # Check if Azure CLI is installed
    if ! command -v az &> /dev/null; then
        print_message $RED "❌ Azure CLI is not installed. Please install it first."
        echo "Installation guide: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
        exit 1
    fi
    
    # Check if logged in to Azure
    if ! az account show &> /dev/null; then
        print_message $RED "❌ Not logged in to Azure. Please run 'az login' first."
        exit 1
    fi
    
    print_message $GREEN "✅ Prerequisites check passed"
}

# Function to get user input
get_user_input() {
    print_message $BLUE "Gathering deployment configuration..."
    
    # Get subscription ID
    if [ -z "$SUBSCRIPTION_ID" ]; then
        SUBSCRIPTION_ID=$(az account show --query id -o tsv)
        print_message $YELLOW "Using current subscription: $SUBSCRIPTION_ID"
    fi
    
    # Get resource group name
    if [ -z "$RESOURCE_GROUP_NAME" ]; then
        read -p "Enter Resource Group name (or press Enter for 'sparkbcp-rg'): " input_rg
        RESOURCE_GROUP_NAME=${input_rg:-sparkbcp-rg}
    fi
    
    # Get location
    read -p "Enter Azure region (or press Enter for 'usgovvirginia'): " input_location
    LOCATION=${input_location:-usgovvirginia}
    
    print_message $GREEN "Configuration:"
    print_message $GREEN "  Subscription: $SUBSCRIPTION_ID"
    print_message $GREEN "  Resource Group: $RESOURCE_GROUP_NAME"
    print_message $GREEN "  Location: $LOCATION"
    print_message $GREEN "  Deployment Name: $DEPLOYMENT_NAME"
}

# Function to create resource group
create_resource_group() {
    print_message $BLUE "Creating resource group '$RESOURCE_GROUP_NAME'..."
    
    if az group show --name "$RESOURCE_GROUP_NAME" &> /dev/null; then
        print_message $YELLOW "⚠️  Resource group '$RESOURCE_GROUP_NAME' already exists"
    else
        az group create \
            --name "$RESOURCE_GROUP_NAME" \
            --location "$LOCATION" \
            --tags Project="Spark-BCP-Pipeline" Environment="dev" \
            --output table
        
        print_message $GREEN "✅ Resource group created successfully"
    fi
}

# Function to validate Bicep template
validate_template() {
    print_message $BLUE "Validating Bicep template..."
    
    az deployment group validate \
        --resource-group "$RESOURCE_GROUP_NAME" \
        --template-file "$SCRIPT_DIR/main.bicep" \
        --parameters "$SCRIPT_DIR/main.parameters.json" \
        --output table
    
    print_message $GREEN "✅ Template validation passed"
}

# Function to deploy infrastructure
deploy_infrastructure() {
    print_message $BLUE "Deploying Azure infrastructure..."
    print_message $YELLOW "This may take 5-10 minutes..."
    
    # Generate a secure password for SQL Server
    SQL_PASSWORD=$(openssl rand -base64 24 | tr -d "=+/" | cut -c1-16)Aa1!
    
    az deployment group create \
        --resource-group "$RESOURCE_GROUP_NAME" \
        --name "$DEPLOYMENT_NAME" \
        --template-file "$SCRIPT_DIR/main.bicep" \
        --parameters "$SCRIPT_DIR/main.parameters.json" \
        --parameters sqlAdminPassword="$SQL_PASSWORD" \
        --parameters location="$LOCATION" \
        --output table
    
    print_message $GREEN "✅ Infrastructure deployment completed"
    
    # Save credentials securely
    echo "SQL_ADMIN_PASSWORD=$SQL_PASSWORD" > "$SCRIPT_DIR/.env"
    chmod 600 "$SCRIPT_DIR/.env"
    print_message $YELLOW "⚠️  SQL Server password saved to $SCRIPT_DIR/.env (keep this secure!)"
}

# Function to get deployment outputs
get_deployment_outputs() {
    print_message $BLUE "Retrieving deployment outputs..."
    
    # Get outputs from deployment
    OUTPUTS=$(az deployment group show \
        --resource-group "$RESOURCE_GROUP_NAME" \
        --name "$DEPLOYMENT_NAME" \
        --query 'properties.outputs' \
        --output json)
    
    # Extract values
    STORAGE_ACCOUNT_NAME=$(echo $OUTPUTS | jq -r '.storageAccountName.value')
    STORAGE_CONNECTION_STRING=$(echo $OUTPUTS | jq -r '.storageConnectionString.value')
    CONTAINER_NAME=$(echo $OUTPUTS | jq -r '.containerName.value')
    SQL_SERVER_FQDN=$(echo $OUTPUTS | jq -r '.sqlServerFqdn.value')
    SQL_DATABASE_NAME=$(echo $OUTPUTS | jq -r '.sqlDatabaseName.value')
    
    print_message $GREEN "✅ Deployment outputs retrieved"
}

# Function to update configuration file
update_config_file() {
    print_message $BLUE "Updating configuration file..."
    
    CONFIG_FILE="$SCRIPT_DIR/../config/azure_config.json"
    
    # Read SQL password from .env file
    source "$SCRIPT_DIR/.env"
    
    # Create updated configuration
    cat > "$CONFIG_FILE" << EOF
{
  "storage_account": {
    "name": "$STORAGE_ACCOUNT_NAME",
    "container": "$CONTAINER_NAME",
    "connection_string": "$STORAGE_CONNECTION_STRING",
    "sas_token": ""
  },
  "sql_server": {
    "server": "$SQL_SERVER_FQDN",
    "database": "$SQL_DATABASE_NAME",
    "username": "sqladmin",
    "password": "$SQL_ADMIN_PASSWORD",
    "table": "sales_data",
    "driver": "ODBC Driver 18 for SQL Server"
  },
  "azure_ad": {
    "tenant_id": "",
    "client_id": "",
    "client_secret": ""
  },
  "spark": {
    "app_name": "SparkToBlobToBCP",
    "master": "local[*]",
    "executor_memory": "2g",
    "driver_memory": "1g"
  },
  "pipeline": {
    "batch_size": 10000,
    "max_retries": 3,
    "timeout_seconds": 300,
    "log_level": "INFO"
  }
}
EOF
    
    print_message $GREEN "✅ Configuration file updated: $CONFIG_FILE"
}

# Function to create SQL table
create_sql_table() {
    print_message $BLUE "Creating SQL Server table..."
    
    # Check if sqlcmd is available
    if command -v sqlcmd &> /dev/null; then
        source "$SCRIPT_DIR/.env"
        
        sqlcmd -S "$SQL_SERVER_FQDN" -d "$SQL_DATABASE_NAME" \
               -U "sqladmin" -P "$SQL_ADMIN_PASSWORD" \
               -i "$SCRIPT_DIR/../sql/create_table.sql" \
               -C  # Trust server certificate
        
        print_message $GREEN "✅ SQL table created successfully"
    else
        print_message $YELLOW "⚠️  sqlcmd not found. Please run the SQL script manually:"
        print_message $YELLOW "    File: $SCRIPT_DIR/../sql/create_table.sql"
        print_message $YELLOW "    Server: $SQL_SERVER_FQDN"
        print_message $YELLOW "    Database: $SQL_DATABASE_NAME"
    fi
}

# Function to generate summary report
generate_summary() {
    print_message $BLUE "Generating deployment summary..."
    
    SUMMARY_FILE="$SCRIPT_DIR/deployment_summary_$(date +%Y%m%d_%H%M%S).txt"
    
    cat > "$SUMMARY_FILE" << EOF
=================================================================
Azure Infrastructure Deployment Summary
=================================================================
Deployment Date: $(date)
Deployment Name: $DEPLOYMENT_NAME
Resource Group: $RESOURCE_GROUP_NAME
Location: $LOCATION

Resources Created:
------------------------------------------------------------------
✅ Storage Account: $STORAGE_ACCOUNT_NAME
   - Container: $CONTAINER_NAME
   - Endpoint: https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/

✅ SQL Server: $SQL_SERVER_FQDN
   - Database: $SQL_DATABASE_NAME
   - Admin User: sqladmin
   - Admin Password: (stored in .env file)

Configuration:
------------------------------------------------------------------
✅ Updated: ../config/azure_config.json
✅ Created: .env (contains SQL password)

Next Steps:
------------------------------------------------------------------
1. Review the configuration file: ../config/azure_config.json
2. Test the pipeline: ../scripts/run_pipeline.sh
3. Keep the .env file secure (contains SQL password)
4. Consider setting up Azure AD authentication for production

Security Notes:
------------------------------------------------------------------
⚠️  SQL Server password is stored in .env file
⚠️  Storage account uses access keys (consider Azure AD for production)
⚠️  SQL Server allows Azure services access
⚠️  Configure firewall rules as needed for your IP address

Cleanup:
------------------------------------------------------------------
To remove all resources:
  az group delete --name $RESOURCE_GROUP_NAME --yes --no-wait

=================================================================
EOF
    
    print_message $GREEN "✅ Summary report created: $SUMMARY_FILE"
    cat "$SUMMARY_FILE"
}

# Main execution function
main() {
    print_message $BLUE "=============================================="
    print_message $BLUE "Azure Infrastructure Deployment for Spark to BCP Pipeline"
    print_message $BLUE "=============================================="
    
    check_prerequisites
    get_user_input
    
    # Confirm deployment
    print_message $YELLOW "Ready to deploy infrastructure. Continue? (y/N)"
    read -r confirm
    if [[ ! $confirm =~ ^[Yy]$ ]]; then
        print_message $YELLOW "Deployment cancelled"
        exit 0
    fi
    
    create_resource_group
    validate_template
    deploy_infrastructure
    get_deployment_outputs
    update_config_file
    create_sql_table
    generate_summary
    
    print_message $GREEN "=============================================="
    print_message $GREEN "🎉 Deployment completed successfully!"
    print_message $GREEN "=============================================="
}

# Handle script arguments
case "${1:-}" in
    "help"|"-h"|"--help")
        echo "Usage: $0 [help|validate-only|cleanup]"
        echo ""
        echo "Options:"
        echo "  help           Show this help message"
        echo "  validate-only  Only validate the Bicep template"
        echo "  cleanup        Delete the resource group and all resources"
        echo "  (no args)      Full deployment"
        exit 0
        ;;
    "validate-only")
        check_prerequisites
        get_user_input
        create_resource_group
        validate_template
        print_message $GREEN "✅ Validation completed successfully"
        ;;
    "cleanup")
        read -p "Enter Resource Group name to delete: " rg_name
        if [ -n "$rg_name" ]; then
            print_message $YELLOW "⚠️  This will delete ALL resources in '$rg_name'. Continue? (y/N)"
            read -r confirm
            if [[ $confirm =~ ^[Yy]$ ]]; then
                az group delete --name "$rg_name" --yes --no-wait
                print_message $GREEN "✅ Resource group deletion initiated"
            fi
        fi
        ;;
    *)
        main
        ;;
esac
