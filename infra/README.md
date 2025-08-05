# Azure Infrastructure for Spark to BCP Pipeline

This folder contains Infrastructure as Code (IaC) templates using Azure Bicep to provision the Azure resources needed for the Spark to BCP data pipeline.

## 📋 Resources Provisioned

### Storage Account
- **Purpose**: Store processed data from Spark before BCP import
- **Configuration**: 
  - Hot access tier for frequently accessed data
  - Secure transfer enabled (HTTPS only)
  - Minimum TLS 1.2
  - Blob soft delete enabled (7 days retention)

### Azure SQL Database
- **Purpose**: Target database for BCP bulk imports
- **Configuration**:
  - Basic tier (configurable)
  - 2GB storage
  - SQL authentication enabled
  - Firewall rules for Azure services

### Networking & Security
- **Storage**: Private access only (no public blob access)
- **SQL Server**: Configurable firewall rules
- **Encryption**: All data encrypted at rest and in transit

## 🚀 Quick Deployment

### Prerequisites
```bash
# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Login to Azure
az login

# Set your subscription (if you have multiple)
az account set --subscription "your-subscription-id"
```

### Deploy Infrastructure
```bash
# Make script executable
chmod +x deploy.sh

# Run deployment
./deploy.sh
```

The deployment script will:
1. ✅ Check prerequisites
2. ✅ Prompt for configuration (resource group, location)
3. ✅ Create resource group
4. ✅ Validate Bicep template
5. ✅ Deploy infrastructure
6. ✅ Update configuration files
7. ✅ Create SQL table
8. ✅ Generate deployment summary

## 📁 Files

| File | Purpose |
|------|---------|
| `main.bicep` | Main Bicep template defining all resources |
| `main.parameters.json` | Parameter values for the template |
| `deploy.sh` | Automated deployment script |
| `README.md` | This documentation |

## ⚙️ Configuration Parameters

### Required Parameters
- `sqlAdminPassword`: Secure password for SQL Server admin
- `location`: Azure region for deployment

### Optional Parameters
- `baseName`: Base name for resources (default: 'sparkbcp')
- `environment`: Environment tag (default: 'dev')
- `storageAccountSku`: Storage redundancy (default: 'Standard_LRS')
- `sqlDatabaseSkuName`: Database performance tier (default: 'Basic')
- `clientIPAddress`: Your IP for SQL firewall access

## 🔧 Manual Deployment

If you prefer to deploy manually using Azure CLI:

```bash
# Create resource group
az group create --name sparkbcp-rg --location eastus

# Deploy template
az deployment group create \
  --resource-group sparkbcp-rg \
  --template-file main.bicep \
  --parameters main.parameters.json \
  --parameters sqlAdminPassword='YourSecurePassword123!'
```

## 📊 Deployment Outputs

The template provides these outputs for integration:

```json
{
  "storageAccountName": "sparkbcpstorageXXXXXX",
  "storageConnectionString": "DefaultEndpointsProtocol=https;...",
  "containerName": "data-pipeline",
  "sqlServerFqdn": "sparkbcp-sql-XXXXXX.database.windows.net",
  "sqlDatabaseName": "sparkbcp-db"
}
```

## 🛡️ Security Considerations

### Production Recommendations
1. **Use Azure AD Authentication** instead of SQL authentication
2. **Store secrets in Azure Key Vault**
3. **Configure private endpoints** for storage and SQL
4. **Implement network security groups** and VNet integration
5. **Enable Azure Defender** for SQL and Storage
6. **Configure diagnostic logging** and monitoring

### Current Security Features
- ✅ HTTPS/TLS 1.2+ enforced
- ✅ Storage account keys protected
- ✅ SQL firewall configured
- ✅ Blob soft delete enabled
- ✅ Infrastructure encryption available

## 💰 Cost Optimization

### Default Configuration Costs (USD/month estimate)
- **Storage Account (Standard_LRS)**: ~$0.02/GB
- **SQL Database (Basic tier)**: ~$5.00
- **Data Transfer**: Variable based on usage

### Cost Optimization Tips
1. Use **Standard_LRS** for storage redundancy in dev/test
2. Start with **Basic** SQL tier and scale as needed
3. Enable **auto-pause** for SQL databases in dev environments
4. Monitor usage with **Azure Cost Management**

## 🔄 Environment Management

### Multiple Environments
Deploy to different environments by changing parameters:

```bash
# Development
./deploy.sh --environment dev --location eastus

# Production  
./deploy.sh --environment prod --location westus2 --sql-tier Standard
```

### Environment-Specific Parameters
```json
{
  "dev": {
    "sqlDatabaseSkuName": "Basic",
    "storageAccountSku": "Standard_LRS"
  },
  "prod": {
    "sqlDatabaseSkuName": "S2", 
    "storageAccountSku": "Standard_ZRS"
  }
}
```

## 🧹 Cleanup

### Remove All Resources
```bash
# Using the deployment script
./deploy.sh cleanup

# Or manually
az group delete --name sparkbcp-rg --yes --no-wait
```

### Remove Specific Resources
```bash
# Remove SQL Database only
az sql db delete --server sparkbcp-sql-XXXXXX --name sparkbcp-db --resource-group sparkbcp-rg

# Remove Storage Account only
az storage account delete --name sparkbcpstorageXXXXXX --resource-group sparkbcp-rg
```

## 🐛 Troubleshooting

### Common Issues

**1. Deployment Fails with "Name not available"**
- Solution: The generated resource names might conflict. Change the `baseName` parameter.

**2. SQL Connection Fails**
- Solution: Check firewall rules and ensure your IP is allowed.
- Add your IP: `az sql server firewall-rule create --server <server-name> --name ClientIP --start-ip-address <your-ip> --end-ip-address <your-ip>`

**3. BCP Cannot Access Storage**
- Solution: Verify storage account keys and connection string are correct in config.

**4. "Insufficient quota" Error**
- Solution: Check your subscription limits: `az vm list-usage --location eastus`

### Validation Commands
```bash
# Check resource group
az group show --name sparkbcp-rg

# Check storage account
az storage account show --name sparkbcpstorageXXXXXX --resource-group sparkbcp-rg

# Check SQL server
az sql server show --name sparkbcp-sql-XXXXXX --resource-group sparkbcp-rg

# Test SQL connection
sqlcmd -S sparkbcp-sql-XXXXXX.database.windows.net -d sparkbcp-db -U sqladmin
```

## 📚 Additional Resources

- [Azure Bicep Documentation](https://docs.microsoft.com/en-us/azure/azure-resource-manager/bicep/)
- [Azure Storage Account Documentation](https://docs.microsoft.com/en-us/azure/storage/)
- [Azure SQL Database Documentation](https://docs.microsoft.com/en-us/azure/azure-sql/)
- [Azure CLI Reference](https://docs.microsoft.com/en-us/cli/azure/)

---

## 🎯 Next Steps

After successful deployment:

1. ✅ **Verify Resources**: Check Azure portal to confirm all resources are created
2. ✅ **Test Connectivity**: Test connections to storage and SQL from your development environment  
3. ✅ **Run Pipeline**: Execute the Spark to BCP pipeline using the generated configuration
4. ✅ **Monitor Usage**: Set up Azure Monitor alerts for cost and performance
5. ✅ **Plan Production**: Review security recommendations for production deployment
