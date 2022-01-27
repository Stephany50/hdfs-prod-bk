******** Tables IT *************************

CREATE EXTERNAL TABLE CDR.TT_SC_OSS_2G (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
PERIOD_START_TIME varchar(20),
BSC_name varchar(50),
BCF_name varchar(50),
BTS_name varchar(50),
2G_CS_Traffic decimal(19,2),
2G_Traffic_Lost decimal(19,2),
2G_Traffic_Data decimal(19,2),
2G_DL_Throughput decimal(19,2),
2G_UL_Throughput decimal(19,2),
Average_number_DL_simultaneous_users decimal(19,2),
Average_number_UL_simultaneous_users decimal(19,2),
2G_TCH_Congestion decimal(19,2),
UL_TBF_Cong decimal(19,2),
DL_TBF_Cong decimal(19,2)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/SMARTCAPEX/OSS_2G/'
TBLPROPERTIES ('serialization.null.format'='');


******** Tables TT *************************

CREATE TABLE CDR.SPARK_IT_SC_OSS_2G (
PERIOD_START_TIME timestamp,
BSC_name varchar(50),
BCF_name varchar(50),
BTS_name varchar(50),
2G_CS_Traffic decimal(19,2),
2G_Traffic_Lost decimal(19,2),
2G_Traffic_Data decimal(19,2),
2G_DL_Throughput decimal(19,2),
2G_UL_Throughput decimal(19,2),
Average_number_DL_simultaneous_users decimal(19,2),
Average_number_UL_simultaneous_users decimal(19,2),
2G_TCH_Congestion decimal(19,2),
UL_TBF_Cong decimal(19,2),
DL_TBF_Cong decimal(19,2),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,

INSERT_DATE TIMESTAMP
)
PARTITIONED BY (PERIOD_START_DATE DATE, FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');