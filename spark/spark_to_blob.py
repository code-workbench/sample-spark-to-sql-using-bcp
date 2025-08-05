"""
Spark to Azure Blob Export Module
Handles exporting processed data to Azure Blob Storage in BCP-compatible format
"""

import sys
import os
import logging
from datetime import datetime

# Add config path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from spark_config import SparkConfig
from data_processor import DataProcessor


class SparkToBlobExporter:
    def __init__(self, config_path="../config/azure_config.json"):
        """Initialize exporter with configuration"""
        self.spark_config = SparkConfig(config_path)
        self.config = self.spark_config.get_config()
        self.processor = DataProcessor(config_path)
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, self.config['pipeline']['log_level']),
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def export_to_blob(self, df, output_path="processed_data"):
        """Export DataFrame to Azure Blob Storage in BCP format"""
        try:
            self.logger.info(f"Starting export to Azure Blob Storage: {output_path}")
            
            # Get full blob path
            blob_path = self.spark_config.get_blob_path(output_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            full_path = f"{blob_path}_{timestamp}"
            
            self.logger.info(f"Export path: {full_path}")
            
            # Configure write options for BCP compatibility
            write_options = {
                "header": "false",           # No header for BCP
                "delimiter": "|",            # Pipe delimiter (common for BCP)
                "quote": '"',                # Quote character
                "escape": "\\",              # Escape character
                "nullValue": "NULL",         # NULL representation
                "emptyValue": "",            # Empty string representation
                "dateFormat": "yyyy-MM-dd",  # Date format
                "timestampFormat": "yyyy-MM-dd HH:mm:ss"  # Timestamp format
            }
            
            # Write to blob storage
            # Use coalesce(1) to create single file for BCP
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .options(**write_options) \
              .csv(full_path)
            
            self.logger.info("Export completed successfully")
            
            # Return the path for BCP consumption
            return full_path
            
        except Exception as e:
            self.logger.error(f"Export failed: {str(e)}")
            raise
    
    def export_with_format_file(self, df, output_path="processed_data", create_format_file=True):
        """Export data and optionally create BCP format file"""
        # Export data
        data_path = self.export_to_blob(df, output_path)
        
        if create_format_file:
            format_file_content = self.generate_bcp_format_file(df)
            format_path = f"{output_path}_format.fmt"
            
            # Save format file locally (BCP format files are typically local)
            with open(f"../sql/{format_path}", 'w') as f:
                f.write(format_file_content)
            
            self.logger.info(f"BCP format file created: ../sql/{format_path}")
            
            return data_path, f"../sql/{format_path}"
        
        return data_path, None
    
    def generate_bcp_format_file(self, df):
        """Generate BCP format file content"""
        format_lines = ["14.0"]  # BCP version
        format_lines.append(str(len(df.columns)))  # Number of columns
        
        for i, (column_name, data_type) in enumerate(zip(df.columns, df.dtypes), 1):
            # Map Spark data types to SQL Server data types
            sql_type_mapping = {
                "string": "SQLCHAR",
                "int": "SQLINT",
                "bigint": "SQLBIGINT",
                "double": "SQLFLT8",
                "decimal": "SQLDECIMAL",
                "date": "SQLDATE",
                "timestamp": "SQLDATETIME2"
            }
            
            sql_type = sql_type_mapping.get(data_type.lower(), "SQLCHAR")
            delimiter = "|" if i < len(df.columns) else "\\n"
            
            # Format: field_num data_type prefix_length field_length delimiter column_order column_name
            format_lines.append(f"{i}\t{sql_type}\t0\t0\t\"{delimiter}\"\t{i}\t{column_name}\t\"\"")
        
        return "\n".join(format_lines)
    
    def run_full_pipeline(self):
        """Run the complete Spark to Blob export pipeline"""
        try:
            self.logger.info("Starting full export pipeline")
            
            # Load and process data
            df = self.processor.load_sample_data()
            expanded_df = self.processor.generate_additional_sample_data(df, num_days=30)
            transformed_df = self.processor.transform_data(expanded_df)
            bcp_ready_df = self.processor.prepare_for_bcp(transformed_df)
            
            # Export to blob storage
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"sales_data_aggregated_{timestamp}"
            
            data_path, format_file_path = self.export_with_format_file(
                bcp_ready_df, 
                output_path, 
                create_format_file=True
            )
            
            self.logger.info("Pipeline completed successfully")
            self.logger.info(f"Data exported to: {data_path}")
            self.logger.info(f"Format file created: {format_file_path}")
            
            return {
                "data_path": data_path,
                "format_file_path": format_file_path,
                "record_count": bcp_ready_df.count(),
                "status": "success"
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }
        
        finally:
            self.processor.close()
    
    def validate_export(self, blob_path):
        """Validate the exported data in blob storage"""
        try:
            self.logger.info(f"Validating export at: {blob_path}")
            
            # Read back the exported data to validate
            spark = self.spark_config.create_spark_session()
            
            validation_df = spark.read \
                .option("delimiter", "|") \
                .option("header", "false") \
                .csv(blob_path)
            
            record_count = validation_df.count()
            column_count = len(validation_df.columns)
            
            self.logger.info(f"Validation results: {record_count} records, {column_count} columns")
            
            # Show sample of exported data
            validation_df.show(5, truncate=False)
            
            spark.stop()
            
            return {
                "record_count": record_count,
                "column_count": column_count,
                "status": "valid"
            }
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return {
                "status": "invalid",
                "error": str(e)
            }


if __name__ == "__main__":
    # Example usage
    exporter = SparkToBlobExporter()
    
    # Run the full pipeline
    result = exporter.run_full_pipeline()
    
    if result["status"] == "success":
        print(f"✅ Export completed successfully!")
        print(f"📁 Data path: {result['data_path']}")
        print(f"📄 Format file: {result['format_file_path']}")
        print(f"📊 Records exported: {result['record_count']}")
        
        # Validate the export
        validation_result = exporter.validate_export(result['data_path'])
        print(f"✅ Validation: {validation_result['status']}")
    else:
        print(f"❌ Export failed: {result['error']}")
