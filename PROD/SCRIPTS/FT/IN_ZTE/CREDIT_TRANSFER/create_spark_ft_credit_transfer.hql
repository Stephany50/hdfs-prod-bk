CREATE TABLE MON.SPARK_FT_CREDIT_TRANSFER (
TRANSFER_ID   VARCHAR (100)                                    
,REFILL_TIME    VARCHAR (8)                  
,SENDER_MSISDN  VARCHAR (10)                  
,COMMERCIAL_OFFER  VARCHAR (40)                  
,RECEIVER_IMSI  VARCHAR (1)                  
,RECEIVER_MSISDN   VARCHAR (10)                  
,TRANSFER_VOLUME    BIGINT                  
,TRANSFER_VOLUME_UNIT  VARCHAR (3)                  
,SENDER_DEBIT_AMT  BIGINT                  
,TRANSFER_AMT  BIGINT                  
,TRANSFER_FEES  BIGINT                  
,TRANSFER_MEAN  VARCHAR (3)                  
,TERMINATION_IND   VARCHAR (4)                                
,SENDER_OPERATOR_CODE    VARCHAR (25)                  
,RECEIVER_OPERATOR_CODE    VARCHAR (25)
,ORIGINAL_FILE_NAME  VARCHAR (50)                  
,INSERT_DATE   TIMESTAMP  
) COMMENT 'MON.FT_CREDIT_TRANSFERT Table'
PARTITIONED BY (REFILL_DATE   DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')




