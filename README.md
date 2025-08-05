# Spark to Azure Blob to SQL Server BCP Pipeline

This project demonstrates a complete data pipeline that:
1. Uses Apache Spark to process sample data
2. Exports processed data to Azure Blob Storage in BCP-compatible format
3. Uses BCP utility to bulk import data into SQL Server

## Project Structure

```
bcp-investigation/
├── README.md
├── requirements.txt
├── config/
│   ├── spark_config.py
│   └── azure_config.json
├── data/
│   └── sample_data.csv
├── spark/
│   ├── data_processor.py
│   └── spark_to_blob.py
├── sql/
│   ├── create_table.sql
│   └── format_file.fmt
├── bcp/
│   ├── bcp_import.sh
│   └── bcp_import.ps1
├── infra/
│   ├── main.bicep
│   ├── main.parameters.json
│   ├── deploy.sh
│   └── README.md
└── scripts/
    ├── run_pipeline.sh
    └── setup_environment.sh
```

## Prerequisites

1. **Azure Resources:**
   - Azure Storage Account
   - Azure SQL Database or SQL Server instance
   - Service Principal (for authentication)

2. **Software Requirements:**
   - Python 3.8+
   - Apache Spark 3.x
   - SQL Server BCP utility
   - Azure CLI

3. **Create Configuration / Parameters files:**
    '''
    cp ./infra/main.parameters.json.copy ./infra/main.parameters.json
    cp ./config/azure_config.json.copy ./config/azure_config.json
    '''

4. **Install Azure CLI:**
   ```bash
   curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
   ```

5. **Install BCP:**
   ```
   chmod +x ./scripts/install_bcp.sh
   bash ./scripts/install_bcp.sh
   ```

## Setup Instructions

**For [Quick Start Information](./QUICK_START.md), and information on local [configuration](./CONFIGURATION_EXAMPLES.md).

1. **Deploy Azure Infrastructure:**
    For more information see [infra README.md](./infra/README.md).

   ```bash
   cd /home/kemack/github-projects/bcp-investigation
   chmod +x infra/deploy.sh
   ./infra/deploy.sh
   ```

2. **Setup Local Environment:**
   ```bash
   chmod +x scripts/setup_environment.sh
   ./scripts/setup_environment.sh
   ```

**NOTE: The user account being used to run the scripts will need azure permissions to the blob storage account.  Specifically "Storage Blob Data Contributor"**

3. **Run the Pipeline:**
   ```bash
   chmod +x scripts/run_pipeline.sh
   ./scripts/run_pipeline.sh
   ```

**NOTE: To clean up all log files generated in execution, you can run the script '''bash ./scripts/clear_log_files.sh'''.**

## Pipeline Flow

1. **Data Generation**: Creates sample sales data
2. **Spark Processing**: Transforms and aggregates data
3. **Blob Export**: Writes data to Azure Blob Storage in pipe-delimited format
4. **BCP Import**: Bulk imports data into SQL Server using BCP utility

## Configuration

Edit `config/azure_config.json` to match your Azure environment:
- Storage account details
- SQL Server connection information
- Authentication credentials

## Monitoring and Logging

All operations include comprehensive logging:
- Spark job logs
- BCP operation logs
- Error handling and retry logic

## Performance Considerations

- File sizes optimized for BCP performance
- Partitioning strategy for large datasets
- Connection pooling and timeout settings
