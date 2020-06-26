CREATE TABLE MON.SPARK_FT_LOYALTY_PROGRAM_ATL_MARKS
(
    MSISDN VARCHAR(16),
    TYPE VARCHAR(25),
    DEBIT_AMOUNT DECIMAL(17, 2),
    MARKS BIGINT,
    IS_COMPLETED INT,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")