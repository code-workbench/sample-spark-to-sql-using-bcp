"""
Spark Configuration Module
Handles Spark session creation and Azure Blob Storage configuration
"""

import json
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class SparkConfig:
    def __init__(self, config_path="../config/azure_config.json"):
        """Initialize Spark configuration with Azure settings"""
        with open(config_path, 'r') as f:
            self.config = json.load(f)
    
    def create_spark_session(self):
        """Create and configure Spark session with Azure Blob Storage support"""
        spark_config = self.config['spark']
        storage_config = self.config['storage_account']
        
        conf = SparkConf()
        conf.setAppName(spark_config['app_name'])
        conf.setMaster(spark_config['master'])
        conf.set("spark.executor.memory", spark_config['executor_memory'])
        conf.set("spark.driver.memory", spark_config['driver_memory'])
        
        # Enable adaptive query execution
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Azure Blob Storage configuration
        conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        
        # Configure Azure Blob Storage access
        storage_key = storage_config.get('connection_string', '').split('AccountKey=')[1].split(';')[0]
        if storage_key:
            spark.conf.set(
                f"fs.azure.account.key.{storage_config['name']}.blob.core.windows.net",
                storage_key
            )
        
        # Alternative: Azure AD authentication (recommended for production)
        azure_ad = self.config.get('azure_ad', {})
        if azure_ad.get('client_id'):
            spark.conf.set("fs.azure.account.auth.type", "OAuth")
            spark.conf.set("fs.azure.account.oauth.provider.type", 
                          "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            spark.conf.set("fs.azure.account.oauth2.client.id", azure_ad['client_id'])
            spark.conf.set("fs.azure.account.oauth2.client.secret", azure_ad['client_secret'])
            spark.conf.set("fs.azure.account.oauth2.client.endpoint", 
                          f"https://login.microsoftonline.com/{azure_ad['tenant_id']}/oauth2/token")
        
        return spark
    
    def get_blob_path(self, file_path=""):
        """Generate Azure Blob Storage path"""
        storage_config = self.config['storage_account']
        base_path = f"wasbs://{storage_config['container']}@{storage_config['name']}.blob.core.windows.net"
        return f"{base_path}/{file_path}" if file_path else base_path
    
    def get_config(self):
        """Return the full configuration"""
        return self.config
