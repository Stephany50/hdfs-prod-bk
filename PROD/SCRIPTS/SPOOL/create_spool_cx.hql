CREATE TABLE SPOOL.SPOOL_CX
(
    TRANSFER_DATETIME TIMESTAMP,
    SENDER_MSISDN VARCHAR(100),
    SEGMENTATION VARCHAR(50),
    RECEIVER_MSISDN VARCHAR(100),
    TRANSACTION_TAG VARCHAR(100),
    TRANSFER_STATUS VARCHAR(100),
    ERROR_DESC VARCHAR(1000),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')