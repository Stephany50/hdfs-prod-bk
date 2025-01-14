CREATE EXTERNAL TABLE CDR.SPARK_TT_REMITTANCE_IRT_WHA (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ID VARCHAR(500),
    CODE VARCHAR(500),
    MSISDN VARCHAR(500),
    TXNIDCI VARCHAR(500),
    TXNMODECI VARCHAR(500),
    STATUS VARCHAR(500),
    MOTIF VARCHAR(500),
    USERID VARCHAR(500),
    CREATEDAT VARCHAR(500),
    UPDATEDAT VARCHAR(500),
    PARTNERTRANSACTIONID VARCHAR(500),
    PARTNERSENDINGDATETIME VARCHAR(500),
    PARTNERCODE VARCHAR(500),
    PARTNERNAME VARCHAR(500),
    SENDERFIRSTNAME VARCHAR(500),
    SENDERLASTNAME VARCHAR(500),
    SENDERCOUNTRY VARCHAR(500),
    SENDERSOURCEFUNDS VARCHAR(500),
    SENDERRELATIONSHIPBENEFICIARY VARCHAR(500),
    SENDERTRANSFERREASON VARCHAR(500),
    BENEFICIARYFIRSTNAME VARCHAR(500),
    BENEFICIARYLASTNAME VARCHAR(500),
    BENEFICIARYADDRESS VARCHAR(500),
    BENEFICIARYCITY VARCHAR(500),
    BENEFICIARYCOUNTRY VARCHAR(500),
    BENEFICIARYPHONENUMBER VARCHAR(500),
    BENEFICIARYCURRENCY VARCHAR(500),
    BENEFICIARYAMOUNTTOBERECEIVED VARCHAR(500)

)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/IRT_WHA'
TBLPROPERTIES ('serialization.null.format'='')
;


CREATE TABLE CDR.SPARK_IT_REMITTANCE_IRT_WHA
(
    ID VARCHAR(500),
    CODE VARCHAR(500),
    MSISDN VARCHAR(500),
    TXNIDCI VARCHAR(500),
    TXNMODECI VARCHAR(500),
    STATUS VARCHAR(500),
    MOTIF VARCHAR(500),
    USERID VARCHAR(500),
    CREATEDAT TIMESTAMP,
    UPDATEDAT VARCHAR(500),
    PARTNERTRANSACTIONID VARCHAR(500),
    PARTNERSENDINGDATETIME VARCHAR(500),
    PARTNERCODE VARCHAR(500),
    PARTNERNAME VARCHAR(500),
    SENDERFIRSTNAME VARCHAR(500),
    SENDERLASTNAME VARCHAR(500),
    SENDERCOUNTRY VARCHAR(500),
    SENDERSOURCEFUNDS VARCHAR(500),
    SENDERRELATIONSHIPBENEFICIARY VARCHAR(500),
    SENDERTRANSFERREASON VARCHAR(500),
    BENEFICIARYFIRSTNAME VARCHAR(500),
    BENEFICIARYLASTNAME VARCHAR(500),
    BENEFICIARYADDRESS VARCHAR(500),
    BENEFICIARYCITY VARCHAR(500),
    BENEFICIARYCOUNTRY VARCHAR(500),
    BENEFICIARYPHONENUMBER VARCHAR(500),
    BENEFICIARYCURRENCY VARCHAR(500),
    BENEFICIARYAMOUNTTOBERECEIVED VARCHAR(500),
    ORIGINAL_FILE_NAME  VARCHAR(50),
    ORIGINAL_FILE_SIZE  INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
   )
  PARTITIONED BY (ORIGINAL_FILE_DATE DATE, CREATE_DATE DATE )
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");


  --Staging table in DWH
CREATE TABLE MON.SQ_IT_REMITTANCE_IRT_WHA (
    ID VARCHAR(500),
    CODE VARCHAR(500),
    MSISDN VARCHAR(500),
    TXNIDCI VARCHAR(500),
    TXNMODECI VARCHAR(500),
    STATUS VARCHAR(500),
    MOTIF VARCHAR(500),
    USERID VARCHAR(500),
    CREATEDAT TIMESTAMP,
    UPDATEDAT VARCHAR(500),
    PARTNERTRANSACTIONID VARCHAR(500),
    PARTNERSENDINGDATETIME VARCHAR(500),
    PARTNERCODE VARCHAR(500),
    PARTNERNAME VARCHAR(500),
    SENDERFIRSTNAME VARCHAR(500),
    SENDERLASTNAME VARCHAR(500),
    SENDERCOUNTRY VARCHAR(500),
    SENDERSOURCEFUNDS VARCHAR(500),
    SENDERRELATIONSHIPBENEFICIARY VARCHAR(500),
    SENDERTRANSFERREASON VARCHAR(500),
    BENEFICIARYFIRSTNAME VARCHAR(500),
    BENEFICIARYLASTNAME VARCHAR(500),
    BENEFICIARYADDRESS VARCHAR(500),
    BENEFICIARYCITY VARCHAR(500),
    BENEFICIARYCOUNTRY VARCHAR(500),
    BENEFICIARYPHONENUMBER VARCHAR(500),
    BENEFICIARYCURRENCY VARCHAR(500),
    BENEFICIARYAMOUNTTOBERECEIVED VARCHAR(500),
    CREATE_DATE DATE
);



---Staging table in data lake
CREATE TABLE TMP.SQ_IT_REMITTANCE_IRT_WHA (
    ID VARCHAR(500),
    CODE VARCHAR(500),
    MSISDN VARCHAR(500),
    TXNIDCI VARCHAR(500),
    TXNMODECI VARCHAR(500),
    STATUS VARCHAR(500),
    MOTIF VARCHAR(500),
    USERID VARCHAR(500),
    CREATEDAT TIMESTAMP,
    UPDATEDAT VARCHAR(500),
    PARTNERTRANSACTIONID VARCHAR(500),
    PARTNERSENDINGDATETIME VARCHAR(500),
    PARTNERCODE VARCHAR(500),
    PARTNERNAME VARCHAR(500),
    SENDERFIRSTNAME VARCHAR(500),
    SENDERLASTNAME VARCHAR(500),
    SENDERCOUNTRY VARCHAR(500),
    SENDERSOURCEFUNDS VARCHAR(500),
    SENDERRELATIONSHIPBENEFICIARY VARCHAR(500),
    SENDERTRANSFERREASON VARCHAR(500),
    BENEFICIARYFIRSTNAME VARCHAR(500),
    BENEFICIARYLASTNAME VARCHAR(500),
    BENEFICIARYADDRESS VARCHAR(500),
    BENEFICIARYCITY VARCHAR(500),
    BENEFICIARYCOUNTRY VARCHAR(500),
    BENEFICIARYPHONENUMBER VARCHAR(500),
    BENEFICIARYCURRENCY VARCHAR(500),
    BENEFICIARYAMOUNTTOBERECEIVED VARCHAR(500),
    CREATE_DATE DATE
);


DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_IT_REMITTANCE_IRT_WHA';
  MIN_DATE_PARTITION := '20230101';
  MAX_DATE_PARTITION := '20260101';
  KEY_COLUMN_PART_NAME := 'CREATE_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'IT_REMIT_IRT_WHA';
  PART_PARTITION_NAME := 'IT_REMIT_IRT_WHA_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_P_CDR_J01_16M';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;
