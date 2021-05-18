CREATE TABLE CDR.SPARK_IT_MYWAY_REPORT (
  sb_id                  int,
  sb_msisdn              varchar(20),
  sb_type                varchar(20),
  sb_amount_data         double,
  sb_amount_onnet        double ,
  sb_amount_allnet       double  ,
  sb_created_at          timestamp,
  sb_treated_by          varchar(50)  ,
  sb_treated_at          timestamp,
  sb_contact_chanel      varchar(10)  ,
  sb_error               varchar(500) ,
  sb_payment_means       varchar(20)  ,
  sb_envoi_bonus_allnet  varchar(5),
  sb_envoi_bonus_onnet   varchar(5),
  sb_envoi_cout          varchar(5),
  sb_envoi_validity      varchar(5),
  sb_envoi_bonus_data    varchar(5),
  sb_txn_id              varchar(100),
  sb_identical           varchar(5),
  sb_status_in           varchar(50) ,
  sb_status_tango        varchar(50),
  sb_validity            int,
  sb_canal               varchar(20),
  sb_channel_msisdn      varchar(20),
  ORIGINAL_FILE_NAME VARCHAR(100),
  INSERT_DATE TIMESTAMP
)PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_MYWAY_REPORT (
  ORIGINAL_FILE_NAME VARCHAR(100),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  sb_id                  int,
  sb_msisdn              varchar(20),
  sb_type                varchar(20),
  sb_amount_data         double,
  sb_amount_onnet        double ,
  sb_amount_allnet       double  ,
  sb_created_at          timestamp,
  sb_treated_by          varchar(50)  ,
  sb_treated_at          timestamp,
  sb_contact_chanel      varchar(10)  ,
  sb_error               varchar(500) ,
  sb_payment_means       varchar(20)  ,
  sb_envoi_bonus_allnet  varchar(5),
  sb_envoi_bonus_onnet   varchar(5),
  sb_envoi_cout          varchar(5),
  sb_envoi_validity      varchar(5),
  sb_envoi_bonus_data    varchar(5),
  sb_txn_id              varchar(100),
  sb_identical           varchar(5),
  sb_status_in           varchar(50) ,
  sb_status_tango        varchar(50),
  sb_validity            int,
  sb_canal               varchar(20) ,
  sb_channel_msisdn      varchar(20)
)COMMENT 'CDR SPARK_TT_MYWAY_REPORT external table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = ";"
)
LOCATION '/PROD/TT/PERCO_Q1_2021/PERCO_Q1_MYWAY'
TBLPROPERTIES ('serialization.null.format'='');