/*
===========================================================
LOAD VALUES INTO TABLES
===========================================================

Purpose: 
  Full Load:      The script executes the stored procedure to load data using BULK LOAD into bronze layer tables.
                  Each time script is executed, tables will be truncated before loading the data.
  Error Handling: If the code runs into an error it will print an error message as well as the line at which the error occured.
  Load Duration:  Load duration for the entire layer as well as individual tables are also calculated and printed.
Data Source: 
  CSV Files
Usage:
  EXEC bronze.load_bronze;
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME,@layer_start_time DATETIME,@layer_end_time DATETIME;
		PRINT '===========================================================';
		PRINT ' Loading Bronze Layer'
		PRINT '===========================================================';

		PRINT '-----------------------------------------------------------';
		PRINT ' Loading CRM Tables'
		PRINT '-----------------------------------------------------------';
-------------------------------------------------------------------------------------------------------------
		SET @layer_start_time =GETDATE();
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\DataAnalytics\SQL\DataWithBara\DataWarehouseProject\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
			FIRSTROW =2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '                                                                                       ';
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\DataAnalytics\SQL\DataWithBara\DataWarehouseProject\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW =2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '                                                                                       ';
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\DataAnalytics\SQL\DataWithBara\DataWarehouseProject\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW =2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '                                                                                       ';

		PRINT '-----------------------------------------------------------';
		PRINT ' Loading ERP Tables'
		PRINT '-----------------------------------------------------------';
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT 'Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\DataAnalytics\SQL\DataWithBara\DataWarehouseProject\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		)
		SET @end_time = GETDATE();
		PRINT 'Loading Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '                                                                                       ';
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT 'Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\DataAnalytics\SQL\DataWithBara\DataWarehouseProject\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW =2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '                                                                                       ';
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\DataAnalytics\SQL\DataWithBara\DataWarehouseProject\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			KEEPNULLS
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '                                                                                       ';
		
		SET @layer_end_time =GETDATE();
		PRINT 'Bronze layer took ' + CAST(DATEDIFF(second, @layer_start_time, @layer_end_time ) AS NVARCHAR) + ' seconds to load.';
	END TRY
	BEGIN CATCH
		PRINT 'ERROR LOADING BRONZE LAYER';
		PRINT 'Error MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'Error NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error LINE: ' + CAST(ERROR_LINE() AS NVARCHAR);
	END CATCH
END;
