CREATE TABLE CDR.SPARK_IT_BILLING_DOC (
ORIGINAL_FILE_NAME VARCHAR(150),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
ACCOUNT_NUMBER  VARCHAR(100),
CUSTOMER_NAME   VARCHAR(255),
ACCOUNT_NAME    VARCHAR(255),
CURRENT_BALANCE VARCHAR(255),
DOCUMENT_NUMBER VARCHAR(255),
BILLING_CYCLE   VARCHAR(255),
DOCUMENT_TYPE   VARCHAR(255),
DOCUMENT_DATE   VARCHAR(255),
OPERATOR        VARCHAR(255),
TOTAL_AMOUNT    VARCHAR(255),
OPEN_AMOUNT     VARCHAR(255),
DOCUMENT_UPDATE_DATE VARCHAR(255),
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE    DATE
)
PARTITIONED BY (AS_OF_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY")



CREATE EXTERNAL TABLE CDR.TT_BILLING_DOC (
ORIGINAL_FILE_NAME VARCHAR(150),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
ACCOUNT_NUMBER  VARCHAR(100),
CUSTOMER_NAME   VARCHAR(255),
ACCOUNT_NAME    VARCHAR(255),
CURRENT_BALANCE VARCHAR(255),
DOCUMENT_NUMBER VARCHAR(255),
BILLING_CYCLE   VARCHAR(255),
DOCUMENT_TYPE   VARCHAR(255),
DOCUMENT_DATE   VARCHAR(255),
OPERATOR        VARCHAR(255),
TOTAL_AMOUNT    VARCHAR(255),
OPEN_AMOUNT     VARCHAR(255),
DOCUMENT_UPDATE_DATE VARCHAR(255),
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE    DATE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/STAT_TOOLS/RECOUVREMENT_CREANCES/BILLING_DOC'
TBLPROPERTIES ('serialization.null.format'='');




CREATE TABLE TMP.SQ_SPARK_IT_BILLING_DOC (
    original_file_name         varchar(150),            
    original_file_size         int,                     
    original_file_line_count   int,                    
    account_number             varchar(100),            
    customer_name              varchar(255),            
    account_name               varchar(255),            
    current_balance            varchar(255),            
    document_number            varchar(255),            
    billing_cycle              varchar(255),            
    document_type              varchar(255),            
    document_date              varchar(255),            
    operator                   varchar(255),            
    total_amount               varchar(255),            
    open_amount                varchar(255),            
    document_update_date       varchar(255),            
    insert_date                timestamp,               
    original_file_date         date,                    
    as_of_date                 DATE
);



create table CDR.SQ_IT_BILLING_DOC(
	 original_file_name         varchar(150),            
	 original_file_size         int,                     
	 original_file_line_count   int,                    
	 account_number             varchar(100),            
	 customer_name              varchar(255),            
	 account_name               varchar(255),            
	 current_balance            varchar(255),            
	 document_number            varchar(255),            
	 billing_cycle              varchar(255),            
	 document_type              varchar(255),            
	 document_date              varchar(255),            
	 operator                   varchar(255),            
	 total_amount               varchar(255),            
	 open_amount                varchar(255),            
	 document_update_date       varchar(255),            
	 insert_date                timestamp,               
	 original_file_date         date,                    
	 as_of_date                 DATE
);   




DECLARE 
SAMPLE_TABLE VARCHAR(200); 
MIN_DATE_PARTITION VARCHAR(200); 
MAX_DATE_PARTITION VARCHAR(200);  
KEY_COLUMN_PART_NAME VARCHAR(200);
KEY_COLUMN_PART_TYPE VARCHAR(200);   
PART_OWNER VARCHAR(200);  
PART_TABLE_NAME VARCHAR(200);  
PART_PARTITION_NAME VARCHAR(200);
PART_TYPE_PERIODE VARCHAR(200);  
PART_RETENTION NUMBER;  
PART_TBS_CIBLE VARCHAR(200);  
PART_GARDER_01_DU_MOIS VARCHAR(200);
PART_PCT_FREE NUMBER;   
PART_COMPRESSION VARCHAR(200);  
PART_ROTATION_ACTIVE VARCHAR(200);  
PART_FORMAT VARCHAR(200);
BEGIN 
SAMPLE_TABLE := 'CDR.SQ_IT_BILLING_DOC';
MIN_DATE_PARTITION := '20231211';
MAX_DATE_PARTITION := '20231211';
KEY_COLUMN_PART_NAME := 'AS_OF_DATE';
KEY_COLUMN_PART_TYPE := 'JOUR';
PART_OWNER := 'MON';
PART_TABLE_NAME := 'IT_BILLING_DOC';
PART_PARTITION_NAME := 'IT_BILLING_DOC_';
PART_TYPE_PERIODE := 'JOUR';
PART_RETENTION := 1000;
PART_TBS_CIBLE :=  'TAB_P_MON_J30_16M';
PART_GARDER_01_DU_MOIS := 'NON';
PART_PCT_FREE := 0;
PART_COMPRESSION := 'COMPRESS';
PART_ROTATION_ACTIVE := 'OUI';
PART_FORMAT := 'yyyymmdd';
MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
COMMIT; 
END;