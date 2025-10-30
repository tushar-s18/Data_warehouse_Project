# Data_warehouse_Project

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	Declare @starttime datetime, @endtime datetime
	print 'Inserting data in CRM Tables';

--- 1. CRM Customer Info ---
	Set @starttime = GETDATE();
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
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds'; -- Added space before 'seconds'

--- 2. CRM Product Info ---
	Set @starttime = GETDATE();
	print 'Inserting data:crm_prd_info';
	Truncate table [bronze].[crm_prd_info]
	Bulk insert [bronze].[crm_prd_info]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	Set @endtime = GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds'; -- Added space before 'seconds'

--- 3. CRM Sales Details ---
	Set @starttime = GETDATE();
	print 'Inserting data:crm_sales_details';
	Truncate table [bronze].[crm_sales_details]
	Bulk insert [bronze].[crm_sales_details]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	Set @endtime = GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds'; -- Added space before 'seconds'

print'-------------------------------';
	print 'Inserting data in ERP tables';

--- 4. ERP Customer AZ12 ---
	Set @starttime = GETDATE();
	print 'Inserting data:erp_cust_az12';
	Truncate table [bronze].[erp_cust_az12]
	Bulk insert [bronze].[erp_cust_az12]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	Set @endtime = GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds'; -- Added space before 'seconds'

--- 5. ERP Location A101 ---
	Set @starttime = GETDATE();
	print 'Inserting data:erp_loc_a101'; -- CORRECTED print statement here
	Truncate table [bronze].[erp_loc_a101]
	Bulk insert [bronze].[erp_loc_a101]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
    Set @endtime = GETDATE(); -- Added missing SET @endtime
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds'; -- Added space before 'seconds'

--- 6. ERP Product Category G1V2 ---
	Set @starttime = GETDATE();
	print 'Inserting data:erp_px_cat_g1v2';
	Truncate table [bronze].[erp_px_cat_g1v2]
	Bulk insert [bronze].[erp_px_cat_g1v2]
	from 'C:\Users\lenovo\Desktop\Tushar\SQL Datasets\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
		firstrow= 2,
		Fieldterminator= ',',
		Tablock
	);
	Set @endtime = GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds'; -- Added space before 'seconds'
END
