CREATE TABLE MON.SPARK_FT_CALL_NEW_CUSTOMER_SERVICE(
    SERVED_MSISDN VARCHAR(25),
    SERVED_IMSI VARCHAR(25),
    OTHER_PARTY VARCHAR(25)
)PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY') ;


--staging data lake
CREATE TABLE TMP.SQ_FT_CALL_NEW_CUSTOMER_SERVICE(
    SERVED_MSISDN VARCHAR(25),
    SERVED_IMSI VARCHAR(25),
    OTHER_PARTY VARCHAR(25),
    EVENT_DATE VARCHAR(25)
);



--staging dwh tmp
CREATE TABLE MON.SQ_FT_CALL_NEW_CUSTOMER_SERVICE(
    SERVED_MSISDN VARCHAR(25 BYTE),
    SERVED_IMSI VARCHAR(25 BYTE),
    OTHER_PARTY VARCHAR(25 BYTE),
    EVENT_DATE VARCHAR(25 BYTE)
);

DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_CALL_NEW_CUSTOMER_SERVICE';
  MIN_DATE_PARTITION := '20201101';
  MAX_DATE_PARTITION := '20300101';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_CALL_NEW_CUSTOMER_SERVICE';
  PART_PARTITION_NAME := 'NEW_CUST_SERVICE_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_MIG_64K';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;