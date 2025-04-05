USE master;
GO

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
    PRINT 'DataWarehouse database created successfully.';
END
ELSE
BEGIN
    PRINT 'DataWarehouse database already exists.';
END
GO

USE DataWarehouse;
GO

-- Create bronze schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
    PRINT 'bronze schema created successfully.';
END
ELSE
BEGIN
    PRINT 'bronze schema already exists.';
END
GO

-- Verify/Create tables (must exist before the procedure can reference them)

-- crm_cust_info table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'crm_cust_info' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.crm_cust_info (
        cst_id INT,
        cst_key NVARCHAR(50),
        cst_firstname NVARCHAR(50),
        cst_lastname NVARCHAR(50),
        cst_marital_status NVARCHAR(50),
        cst_gndr NVARCHAR(50),
        cst_create_date DATE
    );
    PRINT 'bronze.crm_cust_info table created successfully.';
END
GO

-- crm_prd_info table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'crm_prd_info' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.crm_prd_info (
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost INT,
        prd_line NVARCHAR(50),
        prd_start_dt DATETIME,
        prd_end_dt DATETIME
    );
    PRINT 'bronze.crm_prd_info table created successfully.';
END
GO

-- crm_sales_details table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'crm_sales_details' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT,
        sls_ship_dt INT,
        sls_due_dt INT,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT
    );
    PRINT 'bronze.crm_sales_details table created successfully.';
END
GO

-- erp_loc_a101 table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'erp_loc_a101' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.erp_loc_a101 (
        cid NVARCHAR(50),
        cntry NVARCHAR(50)
    );
    PRINT 'bronze.erp_loc_a101 table created successfully.';
END
GO

-- erp_cust_az12 table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'erp_cust_az12' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.erp_cust_az12 (
        cid NVARCHAR(50),
        bdate DATE,
        gen NVARCHAR(50)
    );
    PRINT 'bronze.erp_cust_az12 table created successfully.';
END
GO

-- erp_px_cat_g1v2 table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'erp_px_cat_g1v2' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id NVARCHAR(50),
        cat NVARCHAR(50),
        subcat NVARCHAR(50),
        maintenance NVARCHAR(50)
    );
    PRINT 'bronze.erp_px_cat_g1v2 table created successfully.';
END
GO

-- Now create or alter the procedure
USE DataWarehouse;
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'load_bronze' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    PRINT 'Dropping existing bronze.load_bronze procedure...';
    DROP PROCEDURE bronze.load_bronze;
END
GO

CREATE PROCEDURE bronze.load_bronze 
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- crm_cust_info
        PRINT 'Truncating and loading bronze.crm_cust_info...';
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\jjkk6\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        
        -- crm_prd_info
        PRINT 'Truncating and loading bronze.crm_prd_info...';
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\jjkk6\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        
        -- crm_sales_details
        PRINT 'Truncating and loading bronze.crm_sales_details...';
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\jjkk6\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        
        -- erp_loc_a101
        PRINT 'Truncating and loading bronze.erp_loc_a101...';
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\jjkk6\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        
        -- erp_cust_az12
        PRINT 'Truncating and loading bronze.erp_cust_az12...';
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\jjkk6\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        
        -- erp_px_cat_g1v2
        PRINT 'Truncating and loading bronze.erp_px_cat_g1v2...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\jjkk6\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        
        COMMIT TRANSACTION;
        PRINT 'All bronze tables loaded successfully.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        PRINT 'Error in bronze.load_bronze:';
        PRINT ERROR_MESSAGE();
        
        THROW;
    END CATCH
END;
GO

-- Verify the procedure was created
SELECT * FROM sys.procedures WHERE name = 'load_bronze' AND schema_id = SCHEMA_ID('bronze');
GO

-- Execute the procedure
PRINT 'Executing bronze.load_bronze procedure...';
EXEC bronze.load_bronze;
GO

PRINT 'Procedure execution completed.';
