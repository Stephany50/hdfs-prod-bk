CREATE TABLE CDR.SPARK_IT_DAILY_EQUIMENT_SOLD
(
    material_code VARCHAR(32),
    inventory_name VARCHAR(64),
    sn VARCHAR(32),
    imsi VARCHAR(200),
    esn VARCHAR(64),
    sale_date DATE,
    sold_by VARCHAR(64),
    invoice_number VARCHAR(32),
    store_number VARCHAR(32),
    store_description VARCHAR(200),
    payment_type VARCHAR(32),
    receipt_amount DECIMAL(32,8),
    ORIGINAL_FILE_NAME VARCHAR(100),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (stat_date DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");




CREATE EXTERNAL TABLE CDR.SPARK_TT_DAILY_EQUIMENT_SOLD
(   
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    stat_date DATE,
    material_code VARCHAR(32),
    inventory_name VARCHAR(64),
    sn VARCHAR(32),
    imsi VARCHAR(200),
    esn VARCHAR(64),
    sale_date DATE,
    sold_by VARCHAR(64),
    invoice_number VARCHAR(32),
    store_number VARCHAR(32),
    store_description VARCHAR(200),
    payment_type VARCHAR(32),
    receipt_amount DECIMAL
)COMMENT 'CDR SPARK_TT_DAILY_EQUIMENT_SOLD'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "|"
)
-- LOCATION '/PROD/TT/ZSMART/DAILY_EQUIPMENT_SOLD'
LOCATION '/PROD/TT/STAT_TOOLS/DAILY_EQUIPMENT_SOLD'
TBLPROPERTIES ('serialization.null.format'='');