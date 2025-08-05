"""
Data Processor Module
Handles data transformation and aggregation using Spark
"""

import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col as spark_col, when, lit, sum, count, avg, max, current_timestamp, to_date, date_format, round as spark_round
from pyspark.sql.types import *
from random import randint, uniform  # Add explicit import for random functions

# Add config path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from spark_config import SparkConfig

# Import Python's built-in max function explicitly to avoid conflict
import builtins


class DataProcessor:
    def __init__(self, config_path="../config/azure_config.json"):
        """Initialize Data Processor with Spark configuration"""
        self.spark_config = SparkConfig(config_path)
        self.spark = self.spark_config.create_spark_session()
        self.config = self.spark_config.get_config()
        
    def load_sample_data(self, data_path="../data/sample_data.csv"):
        """Load sample data from CSV file"""
        print(f"Loading data from: {data_path}")
        
        # Define schema for better performance
        schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity_sold", IntegerType(), True),
            StructField("sale_date", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("region", StringType(), True)
        ])
        
        df = self.spark.read.csv(data_path, header=True, schema=schema)
        
        # Convert sale_date to proper date format
        df = df.withColumn("sale_date", to_date(spark_col("sale_date"), "yyyy-MM-dd"))
        
        print(f"Loaded {df.count()} records")
        df.show(5)
        return df
    
    def transform_data(self, df):
        """Transform and aggregate data for BCP import"""
        print("Transforming data...")
        
        # Calculate revenue
        df_with_revenue = df.withColumn("revenue", spark_col("price") * spark_col("quantity_sold"))
        
        # Add processing timestamp
        df_processed = df_with_revenue.withColumn(
            "processed_timestamp", 
            current_timestamp()
        )
        
        # Create aggregated data by category and region
        aggregated_df = df_processed.groupBy("category", "region", "sale_date") \
            .agg(
                sum("quantity_sold").alias("total_quantity"),
                sum("revenue").alias("total_revenue"),
                count("product_id").alias("product_count"),
                avg("price").alias("avg_price"),
                max("processed_timestamp").alias("last_processed")
            )
        
        print("Data transformation completed")
        aggregated_df.show(10)
        return aggregated_df
    
    def prepare_for_bcp(self, df):
        """Prepare DataFrame for BCP ingestion - format for SQL Server compatibility"""
        print("Preparing data for BCP...")
        
        # Format data types for SQL Server
        bcp_df = df.select(
            spark_col("category").cast(StringType()),
            spark_col("region").cast(StringType()),
            date_format(spark_col("sale_date"), "yyyy-MM-dd").alias("sale_date"),
            spark_col("total_quantity").cast(IntegerType()),
            spark_round(spark_col("total_revenue"), 2).cast(DecimalType(18, 2)).alias("total_revenue"),
            spark_col("product_count").cast(IntegerType()),
            spark_round(spark_col("avg_price"), 2).cast(DecimalType(18, 2)).alias("avg_price"),
            date_format(spark_col("last_processed"), "yyyy-MM-dd HH:mm:ss").alias("last_processed")
        )
        
        # Handle nulls for BCP
        bcp_ready_df = bcp_df.select(*[
            when(spark_col(column_name).isNull(), lit("NULL")).otherwise(spark_col(column_name)).alias(column_name)
            for column_name in bcp_df.columns
        ])
        
        print("Data prepared for BCP")
        bcp_ready_df.show(5)
        return bcp_ready_df
    
    def generate_additional_sample_data(self, base_df, num_days=30):
        """Generate additional sample data for testing"""
        print(f"Generating additional {num_days} days of sample data...")
        
        # Create date range
        start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
        date_list = [(start_date + timedelta(days=x)).strftime("%Y-%m-%d") for x in range(num_days)]
        
        # Create expanded dataset
        expanded_data = []
        base_rows = base_df.collect()  # Collect once to avoid multiple collections
        
        for date_str in date_list:
            # Multiply base records with variations
            for row in base_rows:
                # Safely extract values with null handling
                product_id = row.product_id if row.product_id is not None else 1
                product_name = row.product_name if row.product_name is not None else "Unknown Product"
                category = row.category if row.category is not None else "General"
                price = row.price if row.price is not None else 0.0
                quantity_sold = row.quantity_sold if row.quantity_sold is not None else 1
                customer_id = row.customer_id if row.customer_id is not None else 1000
                region = row.region if row.region is not None else "Unknown"
                
                # Add some randomness to quantities and prices
                new_quantity = builtins.max(1, int(quantity_sold) + randint(-10, 20))
                price_variation = uniform(0.9, 1.1)
                new_price = round(float(price) * price_variation, 2)
                
                expanded_data.append((
                    int(product_id),
                    str(product_name),
                    str(category),
                    float(new_price),
                    int(new_quantity),
                    str(date_str),
                    int(customer_id) + randint(0, 1000),
                    str(region)
                ))
        
        # Create DataFrame from expanded data with explicit schema
        schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity_sold", IntegerType(), True),
            StructField("sale_date", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("region", StringType(), True)
        ])
        
        expanded_df = self.spark.createDataFrame(expanded_data, schema)
        expanded_df = expanded_df.withColumn("sale_date", to_date(spark_col("sale_date"), "yyyy-MM-dd"))
        
        print(f"Generated {expanded_df.count()} records")
        return expanded_df
    
    def save_to_azure_storage(self, df, container_name="data", blob_name="bcp_data.csv"):
        """Save DataFrame to Azure Blob Storage"""
        print(f"Saving data to Azure Storage: {container_name}/{blob_name}")
        
        # Get Azure Storage configuration
        storage_config = self.config.get('storage_account', {})
        storage_account = storage_config.get('name')
        connection_string = storage_config.get('connection_string')
        
        if not storage_account or not connection_string:
            print("Warning: Azure Storage credentials not found. Saving locally only.")
            return self.save_to_local_csv(df, f"../output/{blob_name}")
        
        # Always use Azure Python SDK approach since Spark Azure connector is not configured
        print("Using Azure Python SDK for upload (Spark Azure connector not configured)")
        return self._save_local_then_upload(df, container_name, blob_name)
    
    def _save_local_then_upload(self, df, container_name, blob_name):
        """Fallback method: save locally then upload to Azure"""
        print("Falling back to local save then Azure upload...")
        
        # Save locally first
        local_path = f"../output/{blob_name}"
        self.save_to_local_csv(df, local_path)
        
        # Upload to Azure using Azure SDK
        try:
            from azure.storage.blob import BlobServiceClient
            
            storage_config = self.config.get('storage_account', {})
            connection_string = storage_config.get('connection_string')
            
            if not connection_string:
                print("No connection string found in config")
                return local_path
            
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            
            # Upload the file
            with open(local_path, 'rb') as data:
                blob_service_client.get_blob_client(
                    container=container_name, 
                    blob=blob_name
                ).upload_blob(data, overwrite=True)
            
            storage_account = storage_config.get('name')
            azure_path = f"https://{storage_account}.blob.core.windows.net/{container_name}/{blob_name}"
            print(f"File uploaded to Azure Storage: {azure_path}")
            
            # Clean up local file
            os.remove(local_path)
            return azure_path
            
        except ImportError:
            print("Azure SDK not available. Please install: pip install azure-storage-blob")
            print(f"File saved locally at: {local_path}")
            return local_path
        except Exception as e:
            print(f"Failed to upload to Azure: {str(e)}")
            print(f"File remains locally at: {local_path}")
            return local_path
    
    def save_to_local_csv(self, df, output_path="../output/bcp_data.csv"):
        """Save DataFrame to local CSV file"""
        print(f"Saving data to local file: {output_path}")
        
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save as single CSV file locally
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir + "/temp")
        
        # Move the single CSV file to the desired location
        import glob
        csv_files = glob.glob(f"{output_dir}/temp/*.csv")
        if csv_files:
            import shutil
            shutil.move(csv_files[0], output_path)
            shutil.rmtree(f"{output_dir}/temp")
        
        print(f"Data saved to: {output_path}")
        return output_path
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            print("Spark session closed")


if __name__ == "__main__":
    # Example usage
    processor = DataProcessor()
    
    try:
        # Load sample data
        df = processor.load_sample_data()
        
        # Generate more data for testing
        expanded_df = processor.generate_additional_sample_data(df, num_days=7)
        
        # Transform data
        transformed_df = processor.transform_data(expanded_df)
        
        # Prepare for BCP
        bcp_ready_df = processor.prepare_for_bcp(transformed_df)
        
        # Save to Azure Storage (uses local save then upload approach)
        azure_path = processor.save_to_azure_storage(bcp_ready_df, container_name="data", blob_name="bcp_data.csv")
        
        print(f"Final dataset ready for BCP: {bcp_ready_df.count()} records")
        print(f"Data saved to Azure Storage: {azure_path}")
        
    finally:
        processor.close()
