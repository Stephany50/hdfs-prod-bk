create TABLE mon.spark_ft_perco_q1_2021_incremental
(
    site_name varchar(200),
    region_name varchar(200),
    msisdn  varchar(20),

    ca_incremental_daily decimal(20,4),
    ca_voice_incremental_daily decimal(20,4),
    ca_data_incremental_daily decimal(20,4),
    ca_sms_incremental_daily decimal(20,4),

    ca_subs_incremental_daily decimal(20,4),
    ca_subs_voice_incremental_daily decimal(20,4),
    ca_subs_data_incremental_daily decimal(20,4),
    ca_subs_sms_incremental_daily decimal(20,4),

    ca_paygo_incremental_daily decimal(20,4),
    ca_paygo_voice_incremental_daily decimal(20,4),
    ca_paygo_data_incremental_daily decimal(20,4),
    ca_paygo_sms_incremental_daily decimal(20,4),

    usage_incremental_voice_daily decimal(20,4),
    usage_incremental_data_daily decimal(20,4),
    usage_incremental_sms_daily decimal(20,4),
    insert_date timestamp
)
COMMENT 'Table for PERCO Q1 2021 INCREMENTAL'
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');



---staging table in DATA LAKE
create TABLE TMP.SQ_FT_PERCO_Q1_2021_INC
(
    site_name varchar(200),
    region_name varchar(200),
    msisdn  varchar(20),

    ca_inc_dly decimal(20,4),
    ca_voice_inc_dly decimal(20,4),
    ca_data_inc_dly decimal(20,4),
    ca_sms_inc_dly decimal(20,4),

    ca_subs_inc_dly decimal(20,4),
    ca_subs_voice_inc_dly decimal(20,4),
    ca_subs_data_inc_dly decimal(20,4),
    ca_subs_sms_inc_dly decimal(20,4),

    ca_pyg_inc_dly decimal(20,4),
    ca_pyg_voice_inc_dly decimal(20,4),
    ca_pyg_data_inc_dly decimal(20,4),
    ca_pyg_sms_inc_dly decimal(20,4),

    usage_inc_voice_dly decimal(20,4),
    usage_inc_data_dly decimal(20,4),
    usage_inc_sms_dly decimal(20,4),
    insert_date timestamp,
    event_date DATE
);

---staging table in DATAWAREHOUSE
create TABLE MON.SQ_FT_PERCO_Q1_2021_INC
(
    site_name varchar(200),
    region_name varchar(200),
    msisdn  varchar(20),

    ca_inc_dly decimal(20,4),
    ca_voice_inc_dly decimal(20,4),
    ca_data_inc_dly decimal(20,4),
    ca_sms_inc_dly decimal(20,4),

    ca_subs_inc_dly decimal(20,4),
    ca_subs_voice_inc_dly decimal(20,4),
    ca_subs_data_inc_dly decimal(20,4),
    ca_subs_sms_inc_dly decimal(20,4),

    ca_pyg_inc_dly decimal(20,4),
    ca_pyg_voice_inc_dly decimal(20,4),
    ca_pyg_data_inc_dly decimal(20,4),
    ca_pyg_sms_inc_dly decimal(20,4),

    usage_inc_voice_dly decimal(20,4),
    usage_inc_data_dly decimal(20,4),
    usage_inc_sms_dly decimal(20,4),
    insert_date timestamp,
    event_date DATE
);



create TABLE mon.tmp_ft_perco_q1_2021_inc
(
    site_name varchar(200),
    region_name varchar(200),
    msisdn  varchar(20),

    ca_inc_dly decimal(20,4),
    ca_voice_inc_dly decimal(20,4),
    ca_data_inc_dly decimal(20,4),
    ca_sms_inc_dly decimal(20,4),

    ca_subs_inc_dly decimal(20,4),
    ca_subs_voice_inc_dly decimal(20,4),
    ca_subs_data_inc_dly decimal(20,4),
    ca_subs_sms_inc_dly decimal(20,4),

    ca_pyg_inc_dly decimal(20,4),
    ca_pyg_voice_inc_dly decimal(20,4),
    ca_pyg_data_inc_dly decimal(20,4),
    ca_pyg_sms_inc_dly decimal(20,4),

    usage_inc_voice_dly decimal(20,4),
    usage_inc_data_dly decimal(20,4),
    usage_inc_sms_dly decimal(20,4),
    insert_date timestamp,
    event_date DATE
);


DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'mon.tmp_ft_perco_q1_2021_inc';
  MIN_DATE_PARTITION := '20210101';
  MAX_DATE_PARTITION := '20210601';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_PERCO_Q1_2021_INC';
  PART_PARTITION_NAME := 'FT_PERCO_Q1_';
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
















SELECT IF(
    A.FT_EXIST = 0
    and B.IT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and F.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and G.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and H.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and I.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and J.IT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and K.IT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    , "OK"
    , "NOK"
) FROM
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_perco_q1_2021 WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(distinct event_date) IT_EXIST FROM CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') B,
(SELECT COUNT(distinct event_date) IT_EXIST FROM CDR.SPARK_IT_ZEMBLAREPORT WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') J,
(SELECT COUNT(distinct event_date) IT_EXIST FROM CDR.SPARK_IT_MYWAY_REPORT WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') K,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_MARKETING_DATAMART WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') F,
(SELECT COUNT(distinct transfer_datetime) FT_EXIST FROM cdr.spark_it_omny_transactions WHERE transfer_datetime between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') G,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') H,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') I
