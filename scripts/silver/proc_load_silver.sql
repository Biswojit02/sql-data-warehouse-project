/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


Create or Alter Procedure silver.load_silver AS
Begin
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

		Begin try

			Set @batch_start_time = GETDATE();
			print '=====================================';
			print 'Loading data into silver layer...';
			print '=====================================';

			print'--------------------------------------';
			print'Loading CRM tables...';
			print'--------------------------------------';


			-- Loading silver.crm_cust_info
			Set @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;

			PRINT '>> Inserting Data Into: silver.crm_cust_info';
			insert into silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			select 
				cst_id,
				cst_key,
				trim(cst_firstname) as cst_firstname,
				trim(cst_lastname) as cst_lastname,
				case 
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				when upper(trim(cst_marital_status)) ='S' then 'Single'
				else 'n/a'
				end as cst_marital_status, -- Normalize marital status values to readable format
				case 
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				when upper(trim(cst_gndr)) ='F' then 'Female'
				else 'n/a'
				end as cst_gndr, -- Normalize gender status values to readable format
				cst_create_date
			from 
				(
					select 
							*,
							ROW_NUMBER() OVER
							(PARTITION BY cst_id ORDER BY cst_create_date desc ) AS flag
					from bronze.crm_cust_info
					where cst_id is not null -- Select the most recent record per customer
			)t
			where flag = 1 ;
			Set @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			print '>> --------------------------------'


			-- Loading silver.crm_prd_info
			Set @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info;

			PRINT '>> Inserting Data Into: silver.crm_prd_info';
			insert into silver.crm_prd_info(
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
					Replace(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, -- Extract category from prd_key and replace '-' with '_'
					SUBSTRING(prd_key,7,len(prd_key)) as prd_key, -- Extract product key by removing the category part
					prd_nm,
					isnull(prd_cost,0) as prd_cost,
					case upper(trim(prd_line))
						when 'M' then 'Mountain'
						when 'R' then 'Road'
						when 'T' then 'Touring'
						when 'S' then 'Other Sales'
						else 'n/a'
					end as prd_line,-- Map prd_line codes to descriptive names, default to 'n/a' for unknown codes
					cast(prd_start_dt as date) prd_start_dt,
					cast(LEAD(prd_start_dt) 
					OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as date)
					AS prd_end_dt -- Calculate end date as one day before the next start date
			FROM bronze.crm_prd_info
				Set @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				print '>> --------------------------------'


			-- Loading silver.crm_sales_details
			Set @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;

			PRINT '>> Inserting Data Into: silver.crm_sales_details';
			insert into silver.crm_sales_details
			(
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
			select
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				case 
					when sls_order_dt <= 0 or LEN(sls_order_dt) != 8 then null
					else CAST(CAST(sls_order_dt AS varchar) AS DATE)
				end as sls_order_dt,
				case 
					when sls_ship_dt <= 0 or LEN(sls_ship_dt) != 8 then null
					else cast(cast(sls_ship_dt as varchar) as date) 
				end as sls_ship_dt,
				case 
					when sls_due_dt <= 0 or LEN(sls_due_dt) != 8 then null
					else cast(cast(sls_due_dt as varchar) as date)
					end as sls_due_dt,
				case 
						when sls_sales is null or sls_sales <= 0 
						or sls_sales != sls_quantity * abs(sls_price)
						then sls_quantity * abs(sls_price) 
						else sls_sales
				end sls_sales, -- Recalcute sales if original value is missing or incorrect

				sls_quantity,

				case
						when sls_price is null or sls_price <= 0
						then sls_sales / nullif(sls_quantity,0)
						else sls_price
				end sls_price -- Derive price if original value is invalid
			from bronze.crm_sales_details
				Set @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				print '>> --------------------------------'


			-- Loading silver.erp_cust_az12
			Set @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12;

			PRINT '>> Inserting Data Into: silver.erp_cust_az12';
			insert into silver.erp_cust_az12
			(
			cid,
			bdate,
			gen
			)

			select
				case 
					when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) -- Remove 'NAS' prefix if present
					else cid
				end as cid,
				case
					when bdate > getdate() then null
					else bdate
				end as bdate, -- set future birthdates to null
				case when upper(trim(gen)) in ('F','Female') then 'Female'
					 when upper(trim(gen)) in ('M','Male') then 'Male'
					 else 'n/a'
				end as gen -- Normalize gender values and handles unknown cases
			from bronze.erp_cust_az12
				Set @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				print '>> --------------------------------'


			-- Loading silver.erp_loc_a101
			Set @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;

			PRINT '>> Inserting Data Into: silver.erp_loc_a101';
			insert into silver.erp_loc_a101
			(
				cid,
				cntry
			)

			select
				replace(cid,'-','')cid,
				case 
					when trim(cntry) in ('USA','US') then 'United States'
					when trim(cntry) = 'DE' then 'Germany'
					when trim(cntry) = '' or trim(cntry) is null then 'n/a'
					else cntry
				end cntry -- normalize and Handel missing Or blank country codes

			from bronze.erp_loc_a101
				Set @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				print '>> --------------------------------'


			-- Loading silver.erp_px_cat_g1v2
			Set @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;

			PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
			insert into silver.erp_px_cat_g1v2 
			(
				id, cat, subcat, maintenance
			)

			select 
				id,
				cat,
				subcat,
				maintenance
			from bronze.erp_px_cat_g1v2;
				Set @end_time = GETDATE();
				PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				print '>> --------------------------------'

        SET @batch_end_time = GETDATE();
	    	PRINT '=========================================='
		    PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		    PRINT '=========================================='     
		End try
      
		Begin Catch
				PRINT '=========================================='
				PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
				PRINT 'Error Message:' + ERROR_MESSAGE();
				PRINT 'Error No:' + CAST (ERROR_NUMBER() AS NVARCHAR);
				PRINT('Error Line: '+ CAST(ERROR_LINE() AS NVARCHAR));
				PRINT 'Error State:' + CAST (ERROR_STATE() AS NVARCHAR);
				PRINT('Error Procedure: ' + Error_Procedure());
				PRINT '=========================================='
		End Catch
          
End;
go


