CREATE TABLE AGG.SPARK_FT_A_OVERDRAFT_MONTHLY
(
    MSISDN_COUNT DECIMAL(20,2),
    PROFILE VARCHAR(250),
    REIMBURSMENT_COUNT DECIMAL(20,2),
    REIMBURSMENT_AMOUNT DECIMAL(20,2),
    LOAN_COUNT DECIMAL(20,2),
    LOAN_AMOUNT DECIMAL(20,2),
    FEE_AMOUNT DECIMAL(20,2),
    OPERATOR_CODE VARCHAR(50),
    INSERT_DATE TIMESTAMP
 )
PARTITIONED BY (EVENT_MONTH DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')