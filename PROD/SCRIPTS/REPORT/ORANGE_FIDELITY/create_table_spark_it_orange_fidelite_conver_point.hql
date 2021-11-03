CREATE TABLE CDR.SPARK_IT_ORANGE_FIDELITE_CONVER_POINT (
    ID VARCHAR(200),            
    MSISDN VARCHAR(200),       
    LOYALTY_OFFER VARCHAR(200), 
    SEGMENT VARCHAR(200),      
    SEUIL VARCHAR(200),        
    BONUS VARCHAR(200),        
    UNIT_OFFER VARCHAR(200),    
    TXNID VARCHAR(200),        
    TXNSTATUS VARCHAR(200),     
    INSERT_AT VARCHAR(200),     
    UPDATE_AT VARCHAR(200),     
    OPTION1 VARCHAR(200),       
    OPTION2 VARCHAR(200), 
  ORIGINAL_FILE_NAME VARCHAR(100),
  INSERTED_DATE TIMESTAMP
)COMMENT 'CDR_ORANGE_FIDELITE_CONVER_POINT'
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')