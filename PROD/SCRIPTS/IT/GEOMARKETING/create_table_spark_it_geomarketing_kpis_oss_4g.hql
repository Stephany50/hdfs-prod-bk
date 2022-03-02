;;;;;;;;;;;

******** Tables IT *************************

CREATE EXTERNAL TABLE CDR.TT_GEOMARKETING_4G (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
PERIOD_START_TIME varchar(20),
MRBTS_SBTS_name varchar(50),
LNBTS_name varchar(50),
Cell_Avail_excl_BLU decimal(19,2),
ORA_4G_LTE_CSSR_Without_VoLTE decimal(19,2),
ORA_ERAB_SETUP_SUCCESS_RATE decimal(19,2),
ORA_4G_Call_Drop_Wo_VoLTE_new decimal(19,2),
ORA_LTE_DL_User_Throughput_new decimal(19,2),
ORA_LTE_UL_User_Throughput_new decimal(19,2),
ORA_PRB_LOAD_DL decimal(19,2),
ORA_Total_TRAFFIC_DL_UL decimal(19,2),
Average_CQI decimal(19,2)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/GEOMARKETING/OSS_4G/'
TBLPROPERTIES ('serialization.null.format'='');


******** Tables TT *************************

CREATE TABLE CDR.SPARK_IT_GEOMARKETING_4G (
PERIOD_START_TIME timestamp,
MRBTS_SBTS_name varchar(50),
LNBTS_name varchar(50),
Cell_Avail_excl_BLU decimal(19,2),
ORA_4G_LTE_CSSR_Without_VoLTE decimal(19,2),
ORA_ERAB_SETUP_SUCCESS_RATE decimal(19,2),
ORA_4G_Call_Drop_Wo_VoLTE_new decimal(19,2),
ORA_LTE_DL_User_Throughput_new decimal(19,2),
ORA_LTE_UL_User_Throughput_new decimal(19,2),
ORA_PRB_LOAD_DL decimal(19,2),
ORA_Total_TRAFFIC_DL_UL decimal(19,2),
Average_CQI decimal(19,2),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,

INSERT_DATE TIMESTAMP
)
PARTITIONED BY (PERIOD_START_DATE DATE, FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');