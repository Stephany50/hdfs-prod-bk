CREATE TABLE AGG.SPARK_FT_A_CREDIT_TRANSFER_SENDER

(
SENDER_MSISDN VARCHAR(10),
COMMERCIAL_OFFER VARCHAR(40),
TRANSFER_COUNT INT,
TRANSFER_AMOUNT INT,
TRANSFER_FEES INT,
LAST_UPDATE DATE,
OPERATOR_CODE VARCHAR(24),
IS_VIP INT

)
PARTITIONED BY (REFILL_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')




