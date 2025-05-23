/*
=============================================================
Create Database and Schemas - Snowflake SQL Script
=============================================================

Script Purpose:
    This script creates a new database named 'SQL_WAREHOUSE'.
    If a database with the same name already exists, it will be replaced.
    After creating the database, the script initializes three schemas:
    - bronze: for raw, unprocessed data
    - silver: for cleansed and transformed data
    - gold: for curated, business-ready data

WARNING:
    Executing this script will replace the 'SQL_WAREHOUSE' database if it already exists.
    All existing data within this database will be permanently deleted.
    Ensure backups are taken and validated before proceeding.

Author: Sudhanshu Joshi
Date: 2025-05-21
Environment: Snowflake
*/

-- Step 1: Create or replace the database
-- This statement creates a new database named 'SQL_WAREHOUSE'.
-- If the database already exists, it will be dropped and recreated.
CREATE OR REPLACE DATABASE SQL_WAREHOUSE;

-- Step 2: Set the context to use the newly created database
-- This ensures all subsequent operations are executed within 'SQL_WAREHOUSE'.
USE DATABASE SQL_WAREHOUSE;

-- Step 3: Create the 'bronze' schema
-- This schema is typically used for raw data ingestion from various sources.
CREATE OR REPLACE SCHEMA bronze;

-- Step 4: Create the 'silver' schema
-- This schema is used for storing cleansed, validated, and transformed datasets.
CREATE OR REPLACE SCHEMA silver;

-- Step 5: Create the 'gold' schema
-- This schema holds refined data that is ready for analytics and business consumption.
CREATE OR REPLACE SCHEMA gold;
