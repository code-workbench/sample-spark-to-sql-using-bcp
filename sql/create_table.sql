-- Create target table for BCP import
-- Run this script on your SQL Server database before running BCP import

-- Copy the following into the preview window on the Azure Portal to create the sql table.  

-- Drop table if exists (for testing purposes)
IF OBJECT_ID('dbo.sales_data', 'U') IS NOT NULL
    DROP TABLE dbo.sales_data;
GO

-- Create the target table
CREATE TABLE dbo.sales_data (
    category NVARCHAR(50) NOT NULL,
    region NVARCHAR(50) NOT NULL,
    sale_date DATE NOT NULL,
    total_quantity INT NOT NULL,
    total_revenue DECIMAL(18,2) NOT NULL,
    product_count INT NOT NULL,
    avg_price DECIMAL(18,2) NOT NULL,
    last_processed DATETIME2 NOT NULL,
    
    -- Add constraints
    CONSTRAINT PK_sales_data PRIMARY KEY CLUSTERED (category, region, sale_date),
    CONSTRAINT CK_sales_data_quantity CHECK (total_quantity >= 0),
    CONSTRAINT CK_sales_data_revenue CHECK (total_revenue >= 0),
    CONSTRAINT CK_sales_data_count CHECK (product_count > 0),
    CONSTRAINT CK_sales_data_price CHECK (avg_price >= 0)
);
GO

-- Create indexes for better query performance
CREATE NONCLUSTERED INDEX IX_sales_data_category 
ON dbo.sales_data (category);
GO

CREATE NONCLUSTERED INDEX IX_sales_data_region 
ON dbo.sales_data (region);
GO

CREATE NONCLUSTERED INDEX IX_sales_data_date 
ON dbo.sales_data (sale_date DESC);
GO

-- Create a view for easy querying
CREATE VIEW dbo.vw_sales_summary AS
SELECT 
    category,
    region,
    COUNT(*) as days_count,
    SUM(total_quantity) as total_quantity_all_days,
    SUM(total_revenue) as total_revenue_all_days,
    AVG(avg_price) as overall_avg_price,
    MIN(sale_date) as first_sale_date,
    MAX(sale_date) as last_sale_date
FROM dbo.sales_data
GROUP BY category, region;
GO

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT, INSERT ON dbo.sales_data TO [your_user_or_role];
-- GRANT SELECT ON dbo.vw_sales_summary TO [your_user_or_role];

PRINT 'Table dbo.sales_data created successfully';
PRINT 'Ready for BCP import';
GO
