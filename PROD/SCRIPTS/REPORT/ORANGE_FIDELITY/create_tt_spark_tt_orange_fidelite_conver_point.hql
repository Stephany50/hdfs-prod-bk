CREATE EXTERNAL TABLE TT.ORANGE_FIDELITE_CONVER_POINT (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
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
OPTION2 VARCHAR(200)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/OM_FIDELITE/CONVERSION_POINT'
TBLPROPERTIES ('serialization.null.format'='');