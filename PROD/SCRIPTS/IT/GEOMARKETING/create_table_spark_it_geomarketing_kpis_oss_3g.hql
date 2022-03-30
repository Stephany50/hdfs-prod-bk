******** Tables TT *************************

CREATE EXTERNAL TABLE CDR.TT_GEOMARKETING_KPIS_3G (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
PERIOD_START_TIME varchar(20),
PLMN_name varchar(50),
RNC_name varchar(50),
WBTS_name varchar(50),
WBTS_ID varchar(50),
Cell_Availability_excluding_blocked_by_user_state decimal(19,2),
ORA_CSSR_CS_CellPCH_URAPCHnew decimal(19,2),
ORA_CSSR_PS_CellPCH_URAPCHnew decimal(19,2),
ORA_3G_Call_Dro_CS_new decimal(19,2),
ORA_3G_Call_Drop_HSDPAnew decimal(19,2),
ORA_3G_TRAFFIC_SPEECH_Erl decimal(19,2),
ORA_TOTAL_3G_DATA_VOLUME_DL_UL_GBYTES decimal(19,2),
ORA_AVG_HSDPA_THROUGHPUT_USER decimal(19,2),
ORA_HSUPA_User_Throughput_new decimal(19,2),
DL_PWR_Load_3G_Num decimal(19,2),
DL_PWR_Load_3G_Denum decimal(19,2)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/GEOMARKETING/OSS_3G/'
TBLPROPERTIES ('serialization.null.format'='');


******** Tables IT *************************

CREATE TABLE CDR.SPARK_IT_GEOMARKETING_KPIS_3G (
PERIOD_START_TIME timestamp,
PLMN_name varchar(50),
RNC_name varchar(50),
WBTS_name varchar(50),
WBTS_ID varchar(50),
Cell_Availability_excluding_blocked_by_user_state decimal(19,2),
ORA_CSSR_CS_CellPCH_URAPCHnew decimal(19,2),
ORA_CSSR_PS_CellPCH_URAPCHnew decimal(19,2),
ORA_3G_Call_Dro_CS_new decimal(19,2),
ORA_3G_Call_Drop_HSDPAnew decimal(19,2),
ORA_3G_TRAFFIC_SPEECH_Erl decimal(19,2),
ORA_TOTAL_3G_DATA_VOLUME_DL_UL_GBYTES decimal(19,2),
ORA_AVG_HSDPA_THROUGHPUT_USER decimal(19,2),
ORA_HSUPA_User_Throughput_new decimal(19,2),
DL_PWR_Load_3G_Num decimal(19,2),
DL_PWR_Load_3G_Denum decimal(19,2),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (PERIOD_START_DATE DATE, FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');