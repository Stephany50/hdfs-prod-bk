CREATE TABLE CDR.SPARK_IT_RAPPORT_DAILY (
ORIGINAL_FILE_NAME VARCHAR(150),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
CODE_COMPTABLE     INT,
MARCHE             VARCHAR(10),
NUM_RECU           VARCHAR(50),
TYPE_PAIEMENT      VARCHAR(50),
TYPE_PAIEMENT_DETAIL     VARCHAR(50),
MONTANT             FLOAT,
CODE_CLIENT         INT,
ACCOUNT_NUMBER      VARCHAR(100),
CUSTOMER_NAME       VARCHAR(255),
INVOICE_NUMBER      INT,
INVOICE_DATE        DATE,
TYPE_CLIENT         VARCHAR(100),
CUSTOMER_SEGMENT    VARCHAR(60),
CHECK_NUMBER        VARCHAR(100),
CREDIT_CARD_NUMBER   VARCHAR(100),
BANK_DEPOSIT_NUMBER   VARCHAR(100),
BANK_TRANSFER_NUMBER   VARCHAR(100),
PAYMENT_METHON_NAME     VARCHAR(100),
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE  DATE
)
PARTITIONED BY (DATE_SAISIE DATE)
STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY")

CREATE TABLE TMP.SQ_SPARK_IT_DAILY_PAYMENT (
    original_file_name         varchar(150),            
    original_file_size         int,                     
    original_file_line_count   int,                     
    code_comptable             int,                     
    marche                     varchar(10),             
    num_recu                   varchar(50),             
    type_paiement              varchar(50),             
    type_paiement_detail       varchar(50),             
    montant                    float,                   
    code_client                int,                     
    account_number             varchar(100),            
    customer_name              varchar(255),            
    invoice_number             int,                     
    invoice_date               date,                    
    type_client                varchar(100),            
    customer_segment           varchar(60),             
    check_number               varchar(100),            
    credit_card_number         varchar(100),            
    bank_deposit_number        varchar(100),            
    bank_transfer_number       varchar(100),            
    payment_method_name        varchar(100),            
    insert_date                timestamp,               
    original_file_date         date,                    
    date_saisie                date  
);


CREATE TABLE CDR.SQ_IT_DAILY_PAYMENT (
    original_file_name         varchar(150),            
    original_file_size         int,                     
    original_file_line_count   int,                     
    code_comptable             int,                     
    marche                     varchar(10),             
    num_recu                   varchar(50),             
    type_paiement              varchar(50),             
    type_paiement_detail       varchar(50),             
    montant                    float,                   
    code_client                int,                     
    account_number             varchar(100),            
    customer_name              varchar(255),            
    invoice_number             int,                     
    invoice_date               date,                    
    type_client                varchar(100),            
    customer_segment           varchar(60),             
    check_number               varchar(100),            
    credit_card_number         varchar(100),            
    bank_deposit_number        varchar(100),            
    bank_transfer_number       varchar(100),            
    payment_method_name        varchar(100),            
    insert_date                timestamp,               
    original_file_date         date,                    
    date_saisie                date
);





DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'CDR.SQ_IT_DAILY_PAYMENT';
  MIN_DATE_PARTITION := '20231211';
  MAX_DATE_PARTITION := '20231211';
  KEY_COLUMN_PART_NAME := 'DATE_SAISIE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'CDR';
  PART_TABLE_NAME := 'IT_DAILY PAYMENT';
  PART_PARTITION_NAME := 'IT_DAILY PAYMENT_';
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
