"""
Data Processor Module
Handles data transformation and aggregation using Spark
"""

import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Add config path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from spark_config import SparkConfig


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
        df = df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
        
        print(f"Loaded {df.count()} records")
        df.show(5)
        return df
    
    def transform_data(self, df):
        """Transform and aggregate data for BCP import"""
        print("Transforming data...")
        
        # Calculate revenue
        df_with_revenue = df.withColumn("revenue", col("price") * col("quantity_sold"))
        
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
            col("category").cast(StringType()),
            col("region").cast(StringType()),
            date_format(col("sale_date"), "yyyy-MM-dd").alias("sale_date"),
            col("total_quantity").cast(IntegerType()),
            round(col("total_revenue"), 2).cast(DecimalType(18, 2)).alias("total_revenue"),
            col("product_count").cast(IntegerType()),
            round(col("avg_price"), 2).cast(DecimalType(18, 2)).alias("avg_price"),
            date_format(col("last_processed"), "yyyy-MM-dd HH:mm:ss").alias("last_processed")
        )
        
        # Handle nulls for BCP
        bcp_ready_df = bcp_df.select(*[
            when(col(column_name).isNull(), lit("NULL")).otherwise(col(column_name)).alias(column_name)
            for column_name in bcp_df.columns
        ])
        
        print("Data prepared for BCP")
        bcp_ready_df.show(5)
        return bcp_ready_df
    
    def generate_additional_sample_data(self, base_df, num_days=30):
        """Generate additional sample data for testing"""
        print(f"Generating additional {num_days} days of sample data...")
        
        from random import randint, uniform
        
        # Create date range
        start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
        date_list = [(start_date + timedelta(days=x)).strftime("%Y-%m-%d") for x in range(num_days)]
        
        # Create expanded dataset
        expanded_data = []
        for date_str in date_list:
            # Multiply base records with variations
            for row in base_df.collect():
                # Add some randomness to quantities and prices
                new_quantity = max(1, row['quantity_sold'] + randint(-10, 20))
                price_variation = uniform(0.9, 1.1)
                new_price = round(row['price'] * price_variation, 2)
                
                expanded_data.append((
                    row['product_id'],
                    row['product_name'],
                    row['category'],
                    new_price,
                    new_quantity,
                    date_str,
                    row['customer_id'] + randint(0, 1000),
                    row['region']
                ))
        
        # Create DataFrame from expanded data
        schema = base_df.schema
        expanded_df = self.spark.createDataFrame(expanded_data, schema)
        expanded_df = expanded_df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
        
        print(f"Generated {expanded_df.count()} records")
        return expanded_df
    
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
        
        print(f"Final dataset ready for BCP: {bcp_ready_df.count()} records")
        
    finally:
        processor.close()
