DROP EXTERNAL TABLE IF EXISTS "d-zhigalo-18".suppliers;
CREATE EXTERNAL TABLE "d-zhigalo-18".suppliers (
    R_NAME TEXT,
    N_NAME TEXT,
    unique_supplers_count BIGINT,
    avg_acctbal FLOAT8,
    mean_acctbal FLOAT8,
    min_acctbal FLOAT8,
    max_acctbal FLOAT8
    )
    LOCATION ('pxf://de-project/d-zhigalo-18/suppliers_report?PROFILE=s3:parquet&SERVER=default')
    ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
