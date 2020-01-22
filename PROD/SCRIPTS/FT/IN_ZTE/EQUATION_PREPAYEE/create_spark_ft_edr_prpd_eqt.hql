------********************************************************************
------*********************Create FT_EDR_PRPD_EQT **********************  
------ ARNOLD CHUENFFO 13-03-2019 ; Modify by SNR 20-03-2019
------*****************************------**********************************

CREATE TABLE MON.SPARK_FT_EDR_PRPD_EQT(
    EVENT_TIME     TIMESTAMP,
    ACCT_ID_MSISDN  VARCHAR(30),
    MAIN_DEBIT     DOUBLE,
    LOAN_DEBIT     DOUBLE,
    SASSAYE_DEBIT  DOUBLE,
    MAIN_CREDIT    DOUBLE,
    LOAN_CREDIT    DOUBLE,
    SASSAYE_CREDIT DOUBLE,
    TYPE            VARCHAR(30),
    INSERT_DATE    TIMESTAMP
) COMMENT 'SPARK_FT_EDR_PRPD_EQT'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')