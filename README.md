# Data_warehouse_Project
create schema bronze
Go;
create schema silver;
create schema gold;

create table bronze.crm_cust_info (
cst_id	int Primary key,
cst_key	nvarchar(50),
cst_firstname nvarchar(50),	
cst_lastname nvarchar(50),	
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);

create table bronze.crm_prd_info (
prd_id	int Primary key,
prd_key	nvarchar(50),
prd_nm	nvarchar(50),
prd_cost int,
prd_line varchar(1),
prd_start_dt date,
prd_end_dt date
);


create table bronze.crm_sales_details(
sls_ord_num	nvarchar(50),
sls_prd_key	nvarchar(50),
sls_cust_id	int,
sls_order_dt int, 
sls_ship_dt	int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);

create table bronze.erp_cust_az12(
CID	nvarchar(50),
BDATE Date,
GEN nvarchar(15)
);

create table bronze.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50)
);

create table bronze.erp_px_cat_g1v2(
ID nvarchar(50),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(50)
);
