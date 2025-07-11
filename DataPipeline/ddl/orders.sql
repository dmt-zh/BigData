DROP EXTERNAL TABLE IF EXISTS "d-zhigalo-18".orders;
CREATE EXTERNAL TABLE "d-zhigalo-18".orders (
    O_MONTH TEXT,
    N_NAME TEXT,
    O_ORDERPRIORITY TEXT,
    orders_count  BIGINT,
    avg_order_price FLOAT8,
    sum_order_price FLOAT8,
    min_order_price FLOAT8,
    max_order_price FLOAT8,
    f_order_status BIGINT,
    o_order_status BIGINT,
    p_order_status BIGINT
    )
    LOCATION ('pxf://de-project/d-zhigalo-18/orders_report?PROFILE=s3:parquet&SERVER=default')
    ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';