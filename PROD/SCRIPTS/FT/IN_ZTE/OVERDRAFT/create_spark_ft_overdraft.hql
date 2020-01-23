CREATE TABLE MON.SPARK_FT_OVERDRAFT (
    TRANSACTION_ID  VARCHAR(250) ,
    TRANSACTION_TIME  VARCHAR(25),
    SERVICE_CODE  VARCHAR(25)  ,
    OPERATION_TYPE  VARCHAR(25)  ,
    REIMBURSMENT_CHANNEL  VARCHAR(50)  ,
    SERVED_PARTY_MSISDN  VARCHAR(25)  ,
    CONTRACT_TYPE  VARCHAR(100)  ,
    OFFER_PROFILE_CODE  VARCHAR(100)  ,
    REFILL_AMOUNT  INT  ,
    LOAN_AMOUNT  INT  ,
    FEE  INT  ,
    FEE_FLAG  VARCHAR(10)  ,
    SOURCE_TABLE  VARCHAR(50)  ,
    OPERATOR_CODE  VARCHAR(50)  ,
    INSERT_DATE TIMESTAMP
) COMMENT 'SPARK FT OVERDRAFT '
    PARTITIONED BY (TRANSACTION_DATE DATE)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY")
;