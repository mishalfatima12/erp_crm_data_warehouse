/*
===========================================================
LOAD VALUES INTO TABLES
===========================================================

Purpose: 
  Full Load:      The script executes the stored procedure to load clean standardized data from bronze layer.
                  Each time script is executed, tables will be truncated before loading the data.
  Error Handling: If the code runs into an error it will print an error message as well as the line at which the error occured.
  Load Duration:  Load duration for the entire layer as well as individual tables are also calculated and printed.
Data Source: 
  Bronze Layer
Usage:
  EXEC silver.load_silver;
*/

/*
=============================================================================================================
								Customer Relationship Management
=============================================================================================================
*/
--USE DataWarehouse;
-------------------------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME , @end_time DATETIME, @layer_start_time DATETIME, @layer_end_time DATETIME;
		PRINT'======================================================================================='
		PRINT'Loading Silver Layer'
		PRINT'======================================================================================='

		PRINT'======================================================================================='
		PRINT'Loading CRM Tables'
		PRINT'======================================================================================='
-------------------------------------------------------------------------------------------------------------
		SET @layer_start_time = GETDATE();	

		SET @start_time = GETDATE();
		PRINT'TRUNCTATING TABLE: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT'ISERTING DATA INTO: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info 
		(
			cst_id,
			cst_key, 
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 ELSE 'n/a'
			END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
				 ELSE 'n/a'
			END AS cst_gender,
		cst_create_date
		FROM (
			SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL)t 
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT'Loading Time: '+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds.'
		PRINT'																						'
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT'TRUNCTATING TABLE: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT'ISERTING DATA INTO: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info 
		(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				WHEN 'R' THEN 'Road'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT'Loading Time: '+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds.'
		PRINT'																						'
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT'TRUNCTATING TABLE: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT'ISERTING DATA INTO: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details
		(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quatity,
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
		CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quatity*ABS(sls_price) THEN sls_quatity*ABS(sls_price)
			 ELSE sls_sales
			 END AS sls_sales,
			sls_quatity,
		CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quatity,0)
			 ELSE sls_price
			 END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT'Loading Time: '+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds.'
		PRINT'																						'
-------------------------------------------------------------------------------------------------------------
/*
=============================================================================================================
								Enterprise Resource Planning
=============================================================================================================
*/
		PRINT'======================================================================================='
		PRINT'Loading ERP Tables'
		PRINT'======================================================================================='
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT'TRUNCTATING TABLE: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT'ISERTING DATA INTO: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12
		(
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
				 ELSE cid
				 END AS cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
				 END AS bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('M' ,'Male') THEN 'Male'
				 WHEN UPPER(TRIM(gen)) IN ('F' ,'Female') THEN 'Female'
				 ELSE 'n/a'
				 END  AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT'Loading Time: '+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds.'
		PRINT'																						'
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT'TRUNCTATING TABLE: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT'ISERTING DATA INTO: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101
		(
			cid,
			cntry
		)
		SELECT 
			REPLACE(cid,'-','') AS cid,
			CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
				 WHEN UPPER(TRIM(cntry))= '' OR UPPER(TRIM(cntry)) IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT'Loading Time: '+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds.'
		PRINT'																						'
-------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT'TRUNCTATING TABLE: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT'ISERTING DATA INTO: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2
		(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT'Loading Time: '+CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds.'
		PRINT'																						'
-------------------------------------------------------------------------------------------------------------
		SET @layer_end_time = GETDATE();
		PRINT'Loading Time For Silver Layer: '+CAST(DATEDIFF(second, @layer_start_time, @layer_end_time) AS NVARCHAR)+ ' seconds.'
		PRINT'																						'
	END TRY
	BEGIN CATCH
		PRINT'<< ERROR LOADING SILVER LAYER >>';
		PRINT'Error Message: '+ERROR_MESSAGE();
		PRINT'Error Number: '+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Line: '+ CAST(ERROR_LINE() AS NVARCHAR);
	END CATCH
END
-------------------------------------------------------------------------------------------------------------
