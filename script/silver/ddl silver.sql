/*
========================================
DDL SCRIPTS: Create Silver Tables
========================================
Script purpose:
    This script creates tables in the 'silver' schema, Dropping existing tables
    if the already exist.
========================================
*/
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info
(
	cst_id int,
	cst_key nvarchar(50),
	cst_first_name nvarchar(50),
	cst_last_name nvarchar(50),
	cst_material_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
	prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details
(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE silver.erp_CUST_AZ12;
CREATE TABLE silver.erp_CUST_AZ12
(
	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50),
	dwh_create_date datetime2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_LOC_A101','U') IS NOT NULL
	DROP TABLE silver.erp_LOC_A101;
CREATE TABLE silver.erp_LOC_A101
(
	CID nvarchar(50),
	CNTRY nvarchar(50),
	dwh_create_date datetime2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2
(
	ID nvarchar(50),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50),
	dwh_create_date datetime2 DEFAULT GETDATE()
); 
