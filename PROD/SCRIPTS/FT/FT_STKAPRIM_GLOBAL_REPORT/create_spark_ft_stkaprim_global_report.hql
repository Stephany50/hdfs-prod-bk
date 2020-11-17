CREATE TABLE MON.SPARK_FT_STKAPRIM_GLOBAL_REPORT
(
REGION_COMMERCIALE VARCHAR(50),
REGION_ADMINISTRATIVE VARCHAR(50),
PARTNER_NAME VARCHAR(50),
CANAUX VARCHAR(50),
CANAL_TYPE VARCHAR(50),
PARENT VARCHAR(50),
CATEGORY_PARENT VARCHAR(50),
CATEGORY_ENFANT VARCHAR(50),
AMOUNT_PARENT DECIMAL(17, 2),
AMOUNT_CUMUL_PARENT DECIMAL(17, 2),
CATEGORY_POS VARCHAR(50),
MSISDN_COUNT DECIMAL(17, 2),
AMOUNT DECIMAL(17, 2),
MSISDN_CUMUL DECIMAL(17, 2),
AMOUNT_CUMUL DECIMAL(17, 2),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (REFILL_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')