-- =============================================================================
-- HeroCoders Data Platform — Snowflake Setup DDL
-- Run once in the Snowflake worksheet as ACCOUNTADMIN
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Warehouse
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- -----------------------------------------------------------------------------
-- Database + Schemas
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS HEROCODERS_DEV;

CREATE SCHEMA IF NOT EXISTS HEROCODERS_DEV.RAW;
CREATE SCHEMA IF NOT EXISTS HEROCODERS_DEV.STAGING;
CREATE SCHEMA IF NOT EXISTS HEROCODERS_DEV.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS HEROCODERS_DEV.MARTS;

-- -----------------------------------------------------------------------------
-- File Format — JSONL.gz (one JSON object per line, gzip compressed)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT HEROCODERS_DEV.RAW.JSONL_GZ
    TYPE                = 'JSON'
    COMPRESSION         = 'GZIP'
    STRIP_OUTER_ARRAY   = FALSE     -- each line is a self-contained JSON object
    STRIP_NULL_VALUES   = FALSE
    IGNORE_UTF8_ERRORS  = FALSE;

-- -----------------------------------------------------------------------------
-- External Stage — points to the S3 raw bucket
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE HEROCODERS_DEV.RAW.STG_S3_RAW
    URL         = 's3://<your_s3_bucket>/'
    CREDENTIALS = (
        AWS_KEY_ID     = '<your_aws_key_id>'
        AWS_SECRET_KEY = '<your_aws_secret_key>'
    )
    FILE_FORMAT = HEROCODERS_DEV.RAW.JSONL_GZ;

-- -----------------------------------------------------------------------------
-- Verify stage — should list your uploaded .jsonl.gz files
-- -----------------------------------------------------------------------------
LIST @HEROCODERS_DEV.RAW.STG_S3_RAW;

-- -----------------------------------------------------------------------------
-- Role + User for dbt (principle of least privilege)
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS TRANSFORMER_DEV;

GRANT USAGE  ON WAREHOUSE COMPUTE_WH                        TO ROLE TRANSFORMER_DEV;
GRANT USAGE  ON DATABASE  HEROCODERS_DEV                    TO ROLE TRANSFORMER_DEV;
GRANT USAGE  ON ALL SCHEMAS IN DATABASE HEROCODERS_DEV      TO ROLE TRANSFORMER_DEV;
GRANT CREATE TABLE ON SCHEMA HEROCODERS_DEV.RAW             TO ROLE TRANSFORMER_DEV;
GRANT CREATE TABLE ON SCHEMA HEROCODERS_DEV.STAGING         TO ROLE TRANSFORMER_DEV;
GRANT CREATE TABLE ON SCHEMA HEROCODERS_DEV.INTERMEDIATE    TO ROLE TRANSFORMER_DEV;
GRANT CREATE TABLE ON SCHEMA HEROCODERS_DEV.MARTS           TO ROLE TRANSFORMER_DEV;
GRANT READ   ON STAGE HEROCODERS_DEV.RAW.STG_S3_RAW         TO ROLE TRANSFORMER_DEV;

-- Assign role to your user
GRANT ROLE TRANSFORMER_DEV TO USER <your_user>;
