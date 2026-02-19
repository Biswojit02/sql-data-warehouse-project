/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


Create or alter procedure bronze.load_bronze as
Begin
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	Begin try
		Set @batch_start_time = GETDATE();
		print '=====================================';
		print 'Loading data into bronze layer...';
		print '=====================================';



		print'-----------------------';
		print'Loading CRM tables...';
		print'-----------------------';
	
		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_info';
		Truncate table bronze.crm_cust_info;

		print '>> Inserting Data Into: bronze.crm_cust_info';
		Bulk Insert bronze.crm_cust_info
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with
		(
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> --------------------------------'



		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_info';
		Truncate table bronze.crm_prd_info;

		print '>> Inserting Data Into: bronze.crm_prd_info';
		Bulk Insert bronze.crm_prd_info
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with
		(
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> --------------------------------'



		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details';
		Truncate table bronze.crm_sales_details;

		print '>> Inserting Data Into: bronze.crm_sales_details';
		Bulk Insert bronze.crm_sales_details
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with
		(
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> --------------------------------'



		print'-----------------------';
		print'Loading ERP tables...';
		print'-----------------------';


		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_az12';
		Truncate table bronze.erp_cust_az12;

		print '>> Inserting Data Into: bronze.erp_cust_az12';
		Bulk Insert bronze.erp_cust_az12
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with
		(
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> --------------------------------'
	


		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_loc_a101';
		Truncate table bronze.erp_loc_a101;

		print '>> Inserting Data Into: bronze.erp_loc_a101';
		Bulk Insert bronze.erp_loc_a101
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with
		(
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> --------------------------------'



		Set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		Truncate table bronze.erp_px_cat_g1v2;

		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		Bulk Insert bronze.erp_px_cat_g1v2
		from 'D:\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with
		(
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> --------------------------------';


		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds ';
		PRINT '==========================================';

	End Try

	Begin Catch
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error No:' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT('Error Line: '+ CAST(ERROR_LINE() AS NVARCHAR));
		PRINT 'Error State:' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT('Error Procedure: ' + Error_Procedure());
		PRINT '=========================================='
	End Catch

End
