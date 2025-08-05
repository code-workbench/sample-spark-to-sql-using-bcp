#!/usr/bin/env python3
"""
Spark to Azure Blob Export Script
Processes data using Spark and exports to Azure Blob Storage
"""

import sys
import os
import logging
from datetime import datetime

# Add project paths
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.append(os.path.dirname(__file__))

from data_processor import DataProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main execution function"""
    processor = None
    
    try:
        logger.info("Starting full export pipeline")
        
        # Initialize processor
        processor = DataProcessor()
        
        # Load sample data
        logger.info("Loading sample data...")
        df = processor.load_sample_data("../data/sample_data.csv")
        
        # Generate additional data for testing
        logger.info("Generating additional sample data...")
        expanded_df = processor.generate_additional_sample_data(df, num_days=30)
        
        # Transform data
        logger.info("Transforming data...")
        transformed_df = processor.transform_data(expanded_df)
        
        # Prepare for BCP
        logger.info("Preparing data for BCP...")
        bcp_ready_df = processor.prepare_for_bcp(transformed_df)
        
        # Generate timestamp for unique file names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"sales_data_aggregated_{timestamp}.csv"
        
        logger.info(f"Starting export to Azure Blob Storage: {blob_name}")
        
        # Save to Azure Storage using the fallback method
        azure_path = processor.save_to_azure_storage(
            bcp_ready_df, 
            container_name="data-pipeline", 
            blob_name=blob_name
        )
        
        logger.info(f"Export completed successfully")
        logger.info(f"Data location: {azure_path}")
        logger.info(f"Records exported: {bcp_ready_df.count()}")
        
        return True
        
    except Exception as e:
        logger.error(f"Export failed: {str(e)}")
        return False
        
    finally:
        if processor:
            processor.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
