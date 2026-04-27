/*
Script Purpose:
    this stored procedure performs the ETL
    process to pouplate the 'silver' schema tables from the 'bronze' schema.
  Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleaned data from bronze into silver tables
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info
		(
		cst_id,
		cst_key,
		cst_first_name,
		cst_last_name,
		cst_material_status,
		cst_gndr,
		cst_create_date
		)
	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_first_name) AS cst_first_name,
	TRIM(cst_last_name) AS cst_last_name,
	CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_material_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM(
		SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL 
		)t WHERE flag_last = 1 ;

	TRUNCATE TABLE silver.crm_prd_info; 
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
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS date) AS prd_start_dt,
	DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
	FROM bronze.crm_prd_info;

	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details (	
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE  CAST(CAST(sls_order_dt AS nvarchar) AS date)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE  CAST(CAST(sls_ship_dt AS nvarchar) AS date)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE  CAST(CAST(sls_due_dt AS nvarchar) AS date)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) 
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_quantity IS NULL OR sls_quantity <= 0
		 THEN sls_sales / NULLIF(ABS(sls_price),0)
		 ELSE sls_quantity
	END AS sls_quantity,
	sls_price
	FROM bronze.crm_sales_details;

	TRUNCATE TABLE silver.erp_CUST_AZ12;
	INSERT INTO silver.erp_CUST_AZ12 (CID, BDATE, GEN)
	SELECT 
	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		 ELSE CID
	END AS CID,
	CASE WHEN BDATE > GETDATE() THEN NULL
		 ELSE BDATE
	END AS BDATE,
	CASE WHEN UPPER(TRIM(GEN)) IN ('M','Male') THEN 'Male'
		 WHEN UPPER(TRIM(GEN)) IN ('F','Female') THEN  'Female'
		 ELSE 'n/a'
	END AS GEN
	FROM bronze.erp_CUST_AZ12;

	TRUNCATE TABLE silver.erp_LOC_A101;
	INSERT INTO silver.erp_LOC_A101 (CID,CNTRY)
	SELECT 
	REPLACE(CID,'-','') AS CID,
	CASE WHEN TRIM(CNTRY) ='DE' THEN 'Germany'
		 WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
		 WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY) = '' THEN 'n/a'
		 ELSE CNTRY
	END AS CNTRY
	FROM bronze.erp_LOC_A101;

	TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
	INSERT INTO silver.erp_PX_CAT_G1V2 (ID, CAT, SUBCAT, MAINTENANCE)
	SELECT 
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
	FROM bronze.erp_PX_CAT_G1V2;

END
