CREATE TABLE TMP.SPARK_TT_USER_CONTRACT_TYPE (
    MSISDN VARCHAR(50),
    CONTRACT_TYPE VARCHAR(50),
    COMPTE VARCHAR(50),
    UPDATE_DATE TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')