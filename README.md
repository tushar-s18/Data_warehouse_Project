# Data_warehouse_Project
create schema bronze
Go;
create schema silver;
create schema gold;

create table bronze.crm_cust_info (
cst_id	int,
cst_key	nvarchar(50),
cst_firstname nvarchar(50),	
cst_lastname nvarchar(50),	
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);

create table bronze.crm_prd_info (
prd_id	int,
prd_key	nvarchar(50),
prd_nm	nvarchar(50),
prd_cost int,
prd_line varchar(10),
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

create or alter procedure bronze.load_bronze AS
BEGIN
	Declare @startime datetime, @endtime datetime
	print 'Inserting data in CRM Tables';
	
	Set @startime = GETDATE();
	print 'Inserting data:crm_cust_info';
	Truncate table [bronze].[crm_cust_info]
	Bulk insert [bronze].[crm_cust_info]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	Set @endtime = GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @startime, @endtime) as varchar) + 'seconds';

	Set @startime = GETDATE();
	print 'Inserting data:crm_prd_info';
	Truncate table [bronze].[crm_prd_info]
	Bulk insert [bronze].[crm_prd_info]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	print 'Load Duration ' + cast(Datediff(second, @startime, @endtime) as varchar) + 'seconds';

	Set @startime = GETDATE();
	print 'Inserting data:crm_sales_details';
	Truncate table [bronze].[crm_sales_details]
	Bulk insert [bronze].[crm_sales_details]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	print 'Load Duration ' + cast(Datediff(second, @startime, @endtime) as varchar) + 'seconds';

	print'-------------------------------';
	print 'Inserting data in ERP tables';

	Set @startime = GETDATE();
	print 'Inserting data:erp_cust_az12';
	Truncate table [bronze].[erp_cust_az12]
	Bulk insert [bronze].[erp_cust_az12]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	print 'Load Duration ' + cast(Datediff(second, @startime, @endtime) as varchar) + 'seconds';

	Set @startime = GETDATE();
	print 'Inserting data:erp_cust_az12';
	Truncate table [bronze].[erp_loc_a101]
	Bulk insert [bronze].[erp_loc_a101]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	print 'Load Duration ' + cast(Datediff(second, @startime, @endtime) as varchar) + 'seconds';

	Set @startime = GETDATE();
	print 'Inserting data:erp_px_cat_g1v2';
	Truncate table [bronze].[erp_px_cat_g1v2]
	Bulk insert [bronze].[erp_px_cat_g1v2]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	print 'Load Duration ' + cast(Datediff(second, @startime, @endtime) as varchar) + 'seconds';
END
