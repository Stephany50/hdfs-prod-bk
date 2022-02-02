******** Tables IT *************************
CREATE TABLE CDR.SPARK_IT_SC_OSS_3G (
PERIOD_START_TIME timestamp,
PLMN_name varchar(50),
RNC_name varchar(50),
WBTS_name varchar(50),
WBTS_ID int,
WCEL_name varchar(50),
WCEL_ID int,
3G_CS_Traffic decimal(19,2),
3G_DL_Data_traffic decimal(19,2),
3G_UL_Data_traffic decimal(19,2),
HSDPA_THROUGHPUT_USER decimal(19,2),
HSUPA_USER_THROUGHPUT decimal(19,2),
AVG_3G_ACTIVE_USERS_IN_QUEUE int,
Avg_num_of_HSDPA_users int,
Avg_num_of_HSUPA_users int,
DL_PWR_Load_3G_Num decimal(19,2),
DL_PWR_Load_3G_Denum decimal(19,2),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (PERIOD_START_DATE DATE, FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


******** Tables TT *************************
CREATE EXTERNAL TABLE CDR.TT_SC_OSS_3G (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
PERIOD_START_TIME varchar(20),
PLMN_name varchar(50),
RNC_name varchar(50),
WBTS_name varchar(50),
WBTS_ID int,
WCEL_name varchar(50),
WCEL_ID int,
3G_CS_Traffic decimal(19,2),
3G_DL_Data_traffic decimal(19,2),
3G_UL_Data_traffic decimal(19,2),
HSDPA_THROUGHPUT_USER decimal(19,2),
HSUPA_USER_THROUGHPUT decimal(19,2),
AVG_3G_ACTIVE_USERS_IN_QUEUE int,
Avg_num_of_HSDPA_users int,
Avg_num_of_HSUPA_users int,
DL_PWR_Load_3G_Num decimal(19,2),
DL_PWR_Load_3G_Denum decimal(19,2)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/SMARTCAPEX/OSS_3G/'
TBLPROPERTIES ('serialization.null.format'='');