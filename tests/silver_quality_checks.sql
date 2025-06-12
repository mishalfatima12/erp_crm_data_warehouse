/*
-------------------------------------------------------------------------------------------------------------
Purpose:
  The code checks the quality of data transformed and loaded into silver layer. The checks include
    - Null or duplicate primary keys
    - String functions
    - Data standardization
    - Invalid dates
Usage:
  Execute each time data is loaded into silver layer.
  Investigate and resolve any issues encountered during the checks.
    
-------------------------------------------------------------------------------------------------------------
*/
USE DataWarehouse;
/*
-------------------------------------------------------------------------------------------------------------
									QUALITY CHECKS: crm_cust_info
-------------------------------------------------------------------------------------------------------------
*/
SELECT * FROM silver.crm_cust_info;
SELECT COUNT(cst_id) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(CST_id) > 1 OR cst_id IS NULL;

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT cst_gender
FROM silver.crm_cust_info
WHERE cst_gender != TRIM(cst_gender);

SELECT DISTINCT(cst_gender)
FROM silver.crm_cust_info;

SELECT DISTINCT(cst_marital_status)
FROM silver.crm_cust_info;
/*
-------------------------------------------------------------------------------------------------------------
									QUALITY CHECKS: crm_prd_info
-------------------------------------------------------------------------------------------------------------
*/
SELECT * FROM silver.crm_prd_info;
--CHECK 1 Primary Key
SELECT COUNT(DISTINCT(prd_id)), COUNT (*)FROM silver.crm_prd_info;
SELECT prd_id FROM silver.crm_prd_info WHERE prd_id IS NULL;
--COLUMN 2: Nulls
SELECT COUNT(prd_key) FROM silver.crm_prd_info WHERE prd_key IS NOT NULL;

--COLUMN 3: Nulls, Distinct/Unique, Homogenizing
SELECT DISTINCT(prd_nm) FROM silver.crm_prd_info;

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--COLUMN 4
SELECT * FROM silver.crm_prd_info 
WHERE prd_key IN(
SELECT prd_key
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL) OR prd_nm IN(
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL);

--COLUMN 5
SELECT DISTINCT(prd_line) FROM silver.crm_prd_info;

--COLUMN 6&7

SELECT * FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;
/*
-------------------------------------------------------------------------------------------------------------
									QUALITY CHECKS: crm_sales_details
-------------------------------------------------------------------------------------------------------------
*/
-- ORDER NUM QUALITY
SELECT 
	*,
	COUNT(sls_ord_num) OVER (PARTITION BY sls_ord_num)
FROM silver.crm_sales_details
ORDER BY COUNT(sls_ord_num) OVER (PARTITION BY sls_ord_num) DESC;
-- PRODUCT KEY EXISTS IN PRODUCT INFO TABLE
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quatity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);
-- CUSTOMER ID EXISTS IN CUSTOMER INFO TABLE
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quatity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- CHECK QUALITY OF DATES

SELECT
	sls_ord_num,sls_prd_key,sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quatity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_ship_dt IS NULL 
	  OR sls_ship_dt < '1900-01-01'
	  OR sls_ship_dt > '2025-01-01';

SELECT
	sls_ord_num,sls_prd_key,sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quatity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_due_dt IS NULL 
	  OR sls_due_dt < '1900-01-01'
	  OR sls_due_dt > '2025-01-01';
-- CHECK ORDER OF DATES
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_ship_dt>sls_due_dt;
-- CHECK SALES DATA
SELECT * FROM silver.crm_sales_details
WHERE sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quatity*sls_price;
SELECT * FROM silver.crm_sales_details
WHERE sls_quatity <=0 OR sls_quatity IS NULL;
SELECT * FROM silver.crm_sales_details
WHERE sls_price <=0 OR sls_price IS NULL;
/*
-------------------------------------------------------------------------------------------------------------
									QUALITY CHECKS: erp_cust_az12
-------------------------------------------------------------------------------------------------------------
*/
SELECT 
	*
FROM silver.erp_cust_az12
WHERE cid != TRIM(cid);

SELECT 
	*
FROM silver.erp_cust_az12
WHERE cid IS NULL;

SELECT 
	*
FROM silver.crm_cust_info
WHERE  cst_key NOT IN (SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
							  ELSE cid
							  END
					FROM silver.erp_cust_az12);
SELECT bdate
FROM silver.erp_cust_az12
WHERE LEN (bdate) != 10;

SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate IS NULL;

SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

SELECT DISTINCT(gen)
FROM silver.erp_cust_az12;


SELECT DISTINCT(gen)
FROM (
SELECT CASE WHEN UPPER(TRIM(gen)) IN ('M' ,'Male') THEN 'Male'
			WHEN UPPER(TRIM(gen)) IN ('F' ,'Female') THEN 'Female'
			ELSE 'n/a'
			END  AS gen
FROM silver.erp_cust_az12)t;
/*
-------------------------------------------------------------------------------------------------------------
									QUALITY CHECKS: erp_loc_a101
-------------------------------------------------------------------------------------------------------------
*/
SELECT 
	* 
FROM silver.erp_loc_a101;

SELECT 
	* 
FROM silver.erp_loc_a101
WHERE cid IS NULL;

SELECT
	REPLACE(cid,'-','') AS cid
FROM silver.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN (SELECT cid FROM silver.erp_cust_az12);

SELECT
	REPLACE(cid,'-','') AS cid
FROM silver.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info);

SELECT 
	DISTINCT(cntry) 
FROM silver.erp_loc_a101;



/*
-------------------------------------------------------------------------------------------------------------
									QUALITY CHECKS: erp_px_cat_g1v2
-------------------------------------------------------------------------------------------------------------
*/

--No checks required
