create table MON.FT_LAST_UPDATE_EC_EXTRACT
(
    MSISDN           VARCHAR(50),
    LOAN_AMOUNT       BIGINT,
    LOAN_TO_PAY       BIGINT,
    STATUS            VARCHAR(250),
    EC_DATE           TIMESTAMP ,
    LAST_PAYMENT_DATE TIMESTAMP,
    FEE_AMOUNT        BIGINT,
    SOURCE_FILE       VARCHAR(250),
    INSERT_DATE       TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")

LOAN_AMOUNT,LOAN_TO_PAY,STATUS,EC_DATE,LAST_PAYMENT_DATE,FEE_AMOUNT,SOURCE_FILE,INSERT_DATE,EVENT_DATE