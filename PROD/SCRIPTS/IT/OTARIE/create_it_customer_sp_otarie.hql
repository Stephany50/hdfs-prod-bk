CREATE TABLE CDR.IT_CUSTOMER_SP_OTARIE
(
    IDCUST	BIGINT
    ,SERVPROVIDER	VARCHAR(300)
    ,ORIGINAL_FILE_NAME	VARCHAR(300)
    ,ORIGINAL_FILE_DATE DATE
    ,ORIGINAL_FILE_SIZE INT
    ,ORIGINAL_FILE_LINE_COUNT INT
    ,INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
CLUSTERED BY(IDCUST) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")