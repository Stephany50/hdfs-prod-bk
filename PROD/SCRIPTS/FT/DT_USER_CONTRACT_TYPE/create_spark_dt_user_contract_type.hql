CREATE TABLE MON.SPARK_DT_USER_CONTRACT_TYPE (
    MSISDN VARCHAR(50),
    CONTRACT_TYPE VARCHAR(50),
    COMPTE VARCHAR(50),
    UPDATE_DATE TIMESTAMP
)
PARTITIONED BY (PROCESSING_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')