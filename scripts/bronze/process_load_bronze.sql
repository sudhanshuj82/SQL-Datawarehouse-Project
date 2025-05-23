/*
=====================================================================
 Script: load_all_bronze_files.sql
 Purpose: Automates loading of structured CSV data from an internal 
          Snowflake stage into corresponding Bronze layer tables.

 Description:
 This stored procedure loads data from six predefined CSV files 
 located in a named internal stage into their corresponding 
 target tables in the BRONZE schema.

 Dependencies:
   - Stage: "SQL_WAREHOUSE"."BRONZE"."MY_CSV_STAGE"
   - File Format: bronze_csv_format (assumed TYPE=CSV, SKIP_HEADER=1)
   - Tables: CRM_CUST_INFO, CRM_PRD_INFO, CRM_SALES_DETAILS,
             ERP_LOC_A101, ERP_CUST_AZ12, ERP_PX_CAT_G1V2
=====================================================================
*/

/* Step 1: Create the stored procedure */
CREATE OR REPLACE PROCEDURE bronze.load_all_bronze_files()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_log STRING DEFAULT '';  -- Accumulates status messages for return
BEGIN

    -- Load CRM domain files
    COPY INTO SQL_WAREHOUSE.BRONZE.CRM_CUST_INFO
    FROM '@"SQL_WAREHOUSE"."BRONZE"."MY_CSV_STAGE"/cust_info.csv'
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
    v_log := v_log || 'CRM_CUST_INFO loaded. ';

    COPY INTO SQL_WAREHOUSE.BRONZE.CRM_PRD_INFO
    FROM '@"SQL_WAREHOUSE"."BRONZE"."MY_CSV_STAGE"/prd_info.csv'
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
    v_log := v_log || 'CRM_PRD_INFO loaded. ';

    COPY INTO SQL_WAREHOUSE.BRONZE.CRM_SALES_DETAILS
    FROM '@"SQL_WAREHOUSE"."BRONZE"."MY_CSV_STAGE"/sales_details.csv'
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
    v_log := v_log || 'CRM_SALES_DETAILS loaded. ';

    -- Load ERP domain files
    COPY INTO SQL_WAREHOUSE.BRONZE.ERP_LOC_A101
    FROM '@"SQL_WAREHOUSE"."BRONZE"."MY_CSV_STAGE"/LOC_A101.csv'
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
    v_log := v_log || 'ERP_LOC_A101 loaded. ';

    COPY INTO SQL_WAREHOUSE.BRONZE.ERP_CUST_AZ12
    FROM '@"SQL_WAREHOUSE"."BRONZE"."MY_CSV_STAGE"/CUST_AZ12.csv'
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
    v_log := v_log || 'ERP_CUST_AZ12 loaded. ';

    COPY INTO SQL_WAREHOUSE.BRONZE.ERP_PX_CAT_G1V2
    FROM '@"SQL_WAREHOUSE"."BRONZE"."MY_CSV_STAGE"/PX_CAT_G1V2.csv'
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
    v_log := v_log || 'ERP_PX_CAT_G1V2 loaded. ';

    -- Return the accumulated log of load operations
    RETURN v_log;

END;
$$;

/* Step 2: Call the stored procedure to trigger the load */
CALL bronze.load_all_bronze_files();

/* Step 3: Optional post-load verification - check row counts */
-- This helps in verifying successful data ingestion
SELECT 'bronze.crm_cust_info' AS table_name, COUNT(*) AS row_count FROM bronze.crm_cust_info
UNION ALL
SELECT 'bronze.crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
UNION ALL
SELECT 'bronze.crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
UNION ALL
SELECT 'bronze.erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
UNION ALL
SELECT 'bronze.erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
UNION ALL
SELECT 'bronze.erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2
ORDER BY table_name;

-- DROP PROCEDURE IF EXISTS SQL_WAREHOUSE.BRONZE.LOAD_ALL_FILES();
