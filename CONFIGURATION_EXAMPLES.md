# Sample Configuration Examples

## Complete azure_config.json Example

```json
{
  "storage_account": {
    "name": "mydatalakestorage",
    "container": "data-pipeline",
    "connection_string": "DefaultEndpointsProtocol=https;AccountName=mydatalakestorage;AccountKey=YOUR_ACCOUNT_KEY_HERE;EndpointSuffix=core.windows.net",
    "sas_token": "?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupx&se=2024-12-31T23:59:59Z&st=2024-01-01T00:00:00Z&spr=https&sig=YOUR_SAS_SIGNATURE"
  },
  "sql_server": {
    "server": "mycompany-sql.database.windows.net",
    "database": "analytics_db",
    "username": "analytics_user",
    "password": "YourSecurePassword123!",
    "table": "sales_data",
    "driver": "ODBC Driver 18 for SQL Server"
  },
  "azure_ad": {
    "tenant_id": "12345678-1234-1234-1234-123456789abc",
    "client_id": "87654321-4321-4321-4321-cba987654321",
    "client_secret": "YourClientSecretHere"
  },
  "spark": {
    "app_name": "SalesDataPipeline",
    "master": "local[4]",
    "executor_memory": "4g",
    "driver_memory": "2g"
  },
  "pipeline": {
    "batch_size": 50000,
    "max_retries": 3,
    "timeout_seconds": 600,
    "log_level": "INFO"
  }
}
```

## Environment Variables Alternative

Instead of storing credentials in the config file, you can use environment variables:

```bash
# Set environment variables
export AZURE_STORAGE_ACCOUNT="mydatalakestorage"
export AZURE_STORAGE_KEY="your_storage_key"
export SQL_SERVER="mycompany-sql.database.windows.net"
export SQL_DATABASE="analytics_db"
export SQL_USERNAME="analytics_user"
export SQL_PASSWORD="YourSecurePassword123!"
```

Then use a modified config file:

```json
{
  "storage_account": {
    "name": "${AZURE_STORAGE_ACCOUNT}",
    "container": "data-pipeline",
    "connection_string": "DefaultEndpointsProtocol=https;AccountName=${AZURE_STORAGE_ACCOUNT};AccountKey=${AZURE_STORAGE_KEY};EndpointSuffix=core.windows.net"
  },
  "sql_server": {
    "server": "${SQL_SERVER}",
    "database": "${SQL_DATABASE}",
    "username": "${SQL_USERNAME}",
    "password": "${SQL_PASSWORD}",
    "table": "sales_data"
  }
}
```

## Azure Key Vault Integration

For production environments, integrate with Azure Key Vault:

```python
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def load_config_from_keyvault():
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url="https://your-keyvault.vault.azure.net/", credential=credential)
    
    config = {
        "storage_account": {
            "name": client.get_secret("storage-account-name").value,
            "connection_string": client.get_secret("storage-connection-string").value,
            "container": "data-pipeline"
        },
        "sql_server": {
            "server": client.get_secret("sql-server").value,
            "database": client.get_secret("sql-database").value,
            "username": client.get_secret("sql-username").value,
            "password": client.get_secret("sql-password").value,
            "table": "sales_data"
        }
    }
    
    return config
```
