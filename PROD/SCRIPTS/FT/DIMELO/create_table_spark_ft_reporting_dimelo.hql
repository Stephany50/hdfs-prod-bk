CREATE TABLE MON.SPARK_FT_REPORTING_DIMELO
(
    created_at DATE,
    periode varchar(10),
    user_id varchar(1000),
    user_name varchar(1000),
    expired_sum int,
    transferred_sum int,
    terminated_sum int,
    deferred_sum int,
    restablished_sum int,
    assignes_sum int ,
    acceptes_sum int,
    missed_sum int,
    team varchar(1000),
    channel varchar(1000)
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");


--Staging table in DWH
CREATE TABLE MON.SQ_REPORTING_DIMELO (
    created_at DATE,
    periode varchar(10),
    user_id varchar(1000),
    user_name varchar(1000),
    expired_sum integer,
    transferred_sum integer,
    terminated_sum integer,
    deferred_sum integer,
    restablished_sum integer,
    assignes_sum integer ,
    acceptes_sum integer,
    missed_sum integer,
    team varchar(1000),
    channel varchar(1000),
    EVENT_DATE DATE
);



---Staging table in data lake
CREATE TABLE TMP.SQ_FT_REPORTING_DIMELO (
    created_at DATE,
    periode varchar(10),
    user_id varchar(1000),
    user_name varchar(1000),
    expired_sum int,
    transferred_sum int,
    terminated_sum int,
    deferred_sum int,
    restablished_sum int,
    assignes_sum int ,
    acceptes_sum int,
    missed_sum int,
    team varchar(1000),
    channel varchar(1000),
    EVENT_DATE DATE
);


DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_REPORTING_DIMELO';
  MIN_DATE_PARTITION := '20240313';
  MAX_DATE_PARTITION := '20240328';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_REPORTING_DIMELO';
  PART_PARTITION_NAME := 'FT_REPORTING_DIMELO_';
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
