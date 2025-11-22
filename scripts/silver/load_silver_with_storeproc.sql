Create or Alter procedure  silver.load_procedure AS
Begin
	Declare @starttime datetime, @endtime datetime, @batch_starttime datetime, @batch_endtime datetime;

	set @batch_starttime = GETDATE();
	PRINT 'Truncating table: silver.crm_cust_info';
	truncate table silver.crm_cust_info;
	set @starttime = GETDATE();
	PRINT 'Inserting Data into: silver.crm_cust_info';
	insert into silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
	select 
	cst_id,
	cst_key,
	Trim(cst_firstname) as cst_firstname,
	Trim(cst_lastname) as cst_lastname,
	Case
		When upper(trim(cst_marital_status)) = 'M' Then 'Married'
		When upper(trim(cst_marital_status)) = 'S' Then 'Single'
		else 'n/a'
	End as cst_marital_status,
	Case 
		when upper(trim(cst_marital_status)) = 'M' Then 'Male'
		When upper(trim(cst_marital_status)) = 'S' Then 'Female'
		else 'n/a'
	end as cst_gndr,
	cst_create_date
	from
	(
		select *,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag
		from bronze.crm_cust_info
		where cst_id is not null
	) t
	where flag=1
	Set @endtime= GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds';

	PRINT 'Truncating table: silver.crm_prd_info';
	truncate table silver.crm_prd_info;
	Set @starttime = GETDATE();
	PRINT 'Inserting Data into: silver.crm_prd_info';
	insert into silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
	select
	prd_id,
	replace(substring(prd_key,1,5),'-','_') as cat_id,
	substring(prd_key,7,len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	case upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a'
	end as prd_line,
	cast (prd_start_dt as date) as prd_start_dt,
	cast(dateadd(day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as date) as prd_end_dt
	from bronze.crm_prd_info
	Set @endtime= GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds';

	PRINT 'Truncating table: silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	Set @starttime = GETDATE();
	PRINT 'Inserting Data into: silver.crm_sales_details';
	insert into silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,
	sls_quantity,sls_price)
	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case 
		when sls_order_dt= 0 or len(sls_order_dt)!= 8 then null
		else cast(cast(sls_order_dt as varchar)as date)
	end as sls_order_dt,
	case 
		when sls_ship_dt= 0 or len(sls_ship_dt)!= 8 then null
		else cast(cast(sls_ship_dt as varchar)as date)
	end as sls_ship_dt,
	case 
		when sls_due_dt= 0 or len(sls_due_dt)!= 8 then null
		else cast(cast(sls_due_dt as varchar)as date)
	end as sls_due_dt,
	case 
		when sls_sales is null or sls_sales <=0 or sls_sales!= sls_quantity * abs(sls_price)
		then sls_quantity * sls_price
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case 
		when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity,0)
		else sls_price
	end as sls_price
	from [bronze].[crm_sales_details]
	Set @endtime= GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds';

	PRINT 'Truncating table: silver.erp_cust_az12';
	truncate table silver.erp_cust_az12;
	Set @starttime = GETDATE();
	PRINT 'Inserting Data into: silver.erp_cust_az12';
	insert into silver.erp_cust_az12(CID, BDATE, GEN)
	select
	Case
		when CID like 'NAS%' then substring(CID, 4,len(CID))
		else CID
	end as CID,
	Case
		when BDATE > Getdate() then null
		else BDATE
	end as BDATE,
	Case
		when Upper(trim(GEN)) in ('F','FEMALE') then 'Female'
		when Upper(trim(GEN)) in ('M','MALE') then 'Male'
		else 'n/a'
	end as GEN
	from bronze.erp_cust_az12
	Set @endtime= GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds';

	PRINT 'Truncating table: silver.erp_loc_a101';
	truncate table silver.erp_loc_a101;
	Set @starttime = GETDATE();
	PRINT 'Inserting Data into: silver.erp_loc_a101';
	insert into silver.erp_loc_a101(CID, CNTRY)
	select 
	Replace(CID, '-', '') as CID,
	case 
		when trim(CNTRY) = 'DE' then 'Germany'
		when trim(CNTRY) in ('US','USA') then 'United States'
		when trim(CNTRY) = '' or CNTRY is null then 'n/a'
		else trim(CNTRY)
	end as CNTRY
	from bronze.erp_loc_a101
	Set @endtime= GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds';

	PRINT 'Truncating table: silver.erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2;
	Set @starttime = GETDATE();
	PRINT 'Inserting Data into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2(ID, CAT, SUBCAT, MAINTENANCE)
	select 
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
	from bronze.erp_px_cat_g1v2
	Set @endtime= GETDATE();
	print 'Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds';
	print 'Loading Silver layer is Completed';
	print '-------------------------------------------'
	set @batch_endtime = GETDATE()
	print 'Total Load Duration ' + cast(Datediff(second, @starttime, @endtime) as varchar) + ' seconds';

END