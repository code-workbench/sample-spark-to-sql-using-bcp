# Quick Start Guide

## 🚀 Getting Started with the Spark to BCP Pipeline

This guide will help you get the pipeline running in 5 minutes.

### Step 1: Deploy Azure Infrastructure

First, deploy the required Azure resources (Storage Account and SQL Database):

```bash
cd /home/kemack/github-projects/bcp-investigation
./infra/deploy.sh
```

This will:
- Create Azure Storage Account with container
- Create Azure SQL Database with table
- Generate secure passwords
- Update configuration files automatically

### Step 2: Run Environment Setup
```bash
./scripts/setup_environment.sh
```

### Step 3: Configure Your Azure Settings (Optional)

If you deployed using the infrastructure script, your `config/azure_config.json` is already configured! 

For manual configuration, edit the file with your values:

```json
{
  "storage_account": {
    "name": "YOUR_STORAGE_ACCOUNT_NAME",
    "container": "data-pipeline",
    "connection_string": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
  },
  "sql_server": {
    "server": "your-server.database.windows.net",
    "database": "your_database",
    "username": "your_username",
    "password": "your_password",
    "table": "sales_data"
  }
}
```

### Step 4: Run the Pipeline

```bash
# Run complete pipeline
./scripts/run_pipeline.sh

# Or run individual stages
./scripts/run_pipeline.sh spark-only
./scripts/run_pipeline.sh bcp-only
```

## 📊 What the Pipeline Does

1. **Spark Processing** (`spark/data_processor.py`):
   - Loads sample sales data
   - Generates additional test data (30 days worth)
   - Aggregates data by category, region, and date
   - Prepares data in BCP-compatible format

2. **Azure Blob Export** (`spark/spark_to_blob.py`):
   - Exports processed data to Azure Blob Storage
   - Uses pipe-delimited format optimal for BCP
   - Creates BCP format files
   - Includes comprehensive logging

3. **BCP Import** (`bcp/bcp_import.sh`):
   - Downloads data from Azure Blob Storage
   - Uses BCP utility to bulk import into SQL Server
   - Includes error handling and validation
   - Supports both direct URL and local file approaches

## 🔧 Configuration Options

### Spark Configuration
- Executor memory: 2GB (configurable)
- Driver memory: 1GB (configurable)
- Adaptive query execution enabled
- Coalesces output to single file for BCP

### BCP Configuration
- Batch size: 10,000 records (configurable)
- Field terminator: `|` (pipe)
- Character encoding: UTF-8
- Error logging enabled

### Azure Configuration
- Supports both storage account key and Azure AD authentication
- Configurable container and blob paths
- SAS token support for secure access

## 📁 Key Files

- `infra/main.bicep` - Azure infrastructure template
- `infra/deploy.sh` - Infrastructure deployment script
- `config/azure_config.json` - Main configuration (auto-generated)
- `data/sample_data.csv` - Sample input data
- `spark/data_processor.py` - Data processing logic
- `spark/spark_to_blob.py` - Spark to Blob export
- `sql/create_table.sql` - SQL Server table schema (auto-executed)
- `bcp/bcp_import.sh` - BCP import script
- `scripts/run_pipeline.sh` - Complete pipeline runner

## 🐛 Troubleshooting

### Common Issues

1. **PySpark Import Errors**: 
   - Ensure Java 11 is installed
   - Activate virtual environment: `source venv/bin/activate`

2. **BCP Not Found**:
   - Install SQL Server tools: `sudo apt-get install mssql-tools18`
   - Add to PATH: `export PATH="$PATH:/opt/mssql-tools18/bin"`

3. **Azure Blob Access Issues**:
   - Verify storage account credentials
   - Check SAS token permissions and expiration
   - Ensure container exists

4. **SQL Server Connection Issues**:
   - Verify server name and credentials
   - Check firewall settings
   - Ensure target database and table exist

### Logs and Debugging

- Pipeline logs: `pipeline_YYYYMMDD_HHMMSS.log`
- Spark logs: `logs/spark_YYYYMMDD_HHMMSS.log`
- BCP logs: `logs/bcp_YYYYMMDD_HHMMSS.log`
- Error logs: `bcp_import_errors_YYYYMMDD_HHMMSS.log`

## 📈 Scaling Considerations

### For Large Datasets

1. **Spark Scaling**:
   - Increase executor memory and cores
   - Use cluster mode instead of local mode
   - Partition data appropriately

2. **BCP Optimization**:
   - Increase batch size for larger datasets
   - Use multiple parallel BCP processes
   - Consider table partitioning in SQL Server

3. **Azure Blob Storage**:
   - Use multiple containers for partitioning
   - Consider Azure Data Factory for orchestration
   - Implement retry logic for transient failures

## 🔒 Security Best Practices

1. **Use Azure AD Authentication** instead of storage account keys
2. **Store credentials in Azure Key Vault**
3. **Use managed identities** when running on Azure
4. **Implement least privilege access** for all resources
5. **Enable logging and monitoring** for all components

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Azure Blob Storage SDK](https://docs.microsoft.com/en-us/azure/storage/blobs/)
- [SQL Server BCP Utility](https://docs.microsoft.com/en-us/sql/tools/bcp-utility)
- [Azure Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/)

---

## 🎯 Next Steps

After running the pipeline successfully:

1. **Monitor the imported data** in SQL Server
2. **Validate infrastructure** using `./infra/validate.sh`
3. **Schedule regular pipeline runs** using cron or Azure Data Factory
4. **Implement data quality checks** and validation rules
5. **Set up alerting** for pipeline failures
6. **Scale the solution** based on your data volume requirements
7. **Review security settings** and consider Azure AD authentication for production
