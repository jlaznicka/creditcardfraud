-- Setting the role to SYSADMIN for necessary permissions
use role SYSADMIN;

-- Creating a new database named CREDIT_CARD_FRAUD
create or replace DATABASE CREDIT_CARD_FRAUD;

-- Creating a new schema named HANDS_ON within the CREDIT_CARD_FRAUD database
create or replace SCHEMA CREDIT_CARD_FRAUD.HANDS_ON;

-- Creating a table for storing customer transaction data related to fraud
create or replace TABLE CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD (
    TRANSACTION_ID NUMBER(38,0),        -- Unique identifier for each transaction
    TX_DATETIME TIMESTAMP_NTZ(9),       -- Date and time of the transaction
    CUSTOMER_ID NUMBER(38,0),           -- Identifier for the customer
    TERMINAL_ID NUMBER(38,0),           -- Identifier for the terminal where transaction occurred
    TX_AMOUNT FLOAT,                    -- Transaction amount
    TX_TIME_SECONDS NUMBER(38,0),       -- Seconds from the first transaction
    TX_TIME_DAYS NUMBER(38,0),          -- Days from the first transaction
    TX_FRAUD NUMBER(38,0),              -- Indicator if the transaction was fraudulent (1 for fraud, 0 otherwise)
    TX_FRAUD_SCENARIO NUMBER(38,0)      -- Scenario number for the type of fraud
);

-- Creating a Snowflake warehouse optimized for machine learning operations
create or replace WAREHOUSE ML_OPTIMIZED_WH with 
WAREHOUSE_TYPE='SNOWPARK-OPTIMIZED'
WAREHOUSE_SIZE='MEDIUM'
;

-- Switching the role to ACCOUNTADMIN for administrative operations
use role ACCOUNTADMIN;

-- Setting default namespace, warehouse, and role for the user SNOWFLAKEUSER
ALTER USER SNOWFLAKEUSER SET DEFAULT_NAMESPACE = 'CREDIT_CARD_FRAUD.HANDS_ON';
ALTER USER SNOWFLAKEUSER SET DEFAULT_WAREHOUSE = 'COMPUTE_WH';
ALTER USER SNOWFLAKEUSER SET DEFAULT_ROLE  = 'SYSADMIN';

-- Transferring ownership of the COMPUTE_WH warehouse to the SYSADMIN role
GRANT OWNERSHIP ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;

-- Adjusting the size of the COMPUTE_WH warehouse to 'SMALL'
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE='SMALL';


---------------------------LOADING DATA------------------------------------
-- Setting the role to SYSADMIN for data loading
use role SYSADMIN;

-- Setting the warehouse to COMPUTE_WH for the data loading process
use warehouse COMPUTE_WH;

-- Step 1: Defining a CSV file format for data loading
CREATE OR REPLACE FILE FORMAT CREDIT_CARD_FRAUD.HANDS_ON.MY_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 0
    COMPRESSION = 'GZIP';

-- Step 2: Creating a stage for loading data from an S3 bucket
CREATE OR REPLACE STAGE CREDIT_CARD_FRAUD.HANDS_ON.MY_STAGE
    URL = 's3://billigencedemopublic/'
    FILE_FORMAT = CREDIT_CARD_FRAUD.HANDS_ON.MY_CSV_FORMAT
;

-- Step 3: Copying data into the CUSTOMER_TRANSACTIONS_FRAUD table from the specified stage
COPY INTO CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD
FROM @CREDIT_CARD_FRAUD.HANDS_ON.MY_STAGE/02_Fraud_Transactions.csv.gz
FILE_FORMAT = (FORMAT_NAME = CREDIT_CARD_FRAUD.HANDS_ON.MY_CSV_FORMAT);


-- Creating a new table for distinct customers
CREATE OR REPLACE TABLE CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMERS AS
    SELECT DISTINCT CUSTOMER_ID
    FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD
    ORDER BY CUSTOMER_ID
;

-- Creating a new table for distinct terminals
CREATE OR REPLACE TABLE CREDIT_CARD_FRAUD.HANDS_ON.TERMINALS AS
    SELECT DISTINCT TERMINAL_ID
    FROM CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD
    ORDER BY TERMINAL_ID
;
