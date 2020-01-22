CREATE TABLE AGG.SPARK_FT_A_EDR_PRPD_EQT(
    ACCT_ID_MSISDN VARCHAR(30),
    MAIN_DEBIT     DECIMAL(13,2),
    LOAN_DEBIT     DECIMAL(13,2),
    SASSAYE_DEBIT  DECIMAL(13,2),
    MAIN_CREDIT    DECIMAL(13,2),
    LOAN_CREDIT    DECIMAL(13,2),
    SASSAYE_CREDIT DECIMAL(13,2),
    INSERT_DATE    TIMESTAMP
) COMMENT 'SPARK_FT_A_EDR_PRPD_EQT'
PARTITIONED BY (EVENT_DAY DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')