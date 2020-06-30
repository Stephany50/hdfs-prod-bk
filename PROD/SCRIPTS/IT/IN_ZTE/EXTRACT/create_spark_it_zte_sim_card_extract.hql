CREATE TABLE CDR.SPARK_IT_ZTE_SIM_CART_EXTRACT (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    SIM_CARD_ID DECIMAL(17,2),
    SIM_TYPE_ID DECIMAL(17,2),
    IS_VIRTUAL VARCHAR(1),
    SP_ID DECIMAL(17,2),
    NAI_PASSWORD VARCHAR(120),
    NAI_USERNAME VARCHAR(120),
    ESN VARCHAR(60),
    ICCID VARCHAR(60),
    HLR_ID DECIMAL(17,2),
    IMSI VARCHAR(60),
    PIN1 VARCHAR(120),
    PUK1 VARCHAR(120),
    PIN2 VARCHAR(120),
    PUK2 VARCHAR(120),
    KI VARCHAR(120),
    STAFF_ID DECIMAL(17,2),
    ORG_ID DECIMAL(17,2),
    SIM_STATE VARCHAR(1),
    AREA_ID DECIMAL(17,2),
    STATE_DATE DATE,
    COMMENTS VARCHAR(4000),
    IMSI2 VARCHAR(60),
    KI2 VARCHAR(120),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(SIM_CARD_ID) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


