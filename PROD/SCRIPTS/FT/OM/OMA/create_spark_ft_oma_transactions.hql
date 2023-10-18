CREATE TABLE MON.SPARK_FT_OMA_TRANSACTION
(
    sender_msisdn VARCHAR(200),
    receiver_msisdn VARCHAR(200),
    transfer_id VARCHAR(200),
    transaction_amount VARCHAR(200),
    TRANSFER_DATETIME TIMESTAMP,
    txnmode VARCHAR(500),
    transfer_status VARCHAR(200),
    service_type VARCHAR(200)
   )
  PARTITIONED BY (TRANSACTION_DATE DATE)
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");