******** Tables TT *************************

CREATE EXTERNAL TABLE CDR.TT_GEOMARKETING_KPIS_2G (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
PERIOD_START_TIME varchar(20),
BSC_name varchar(50),
BCF_name varchar(50),
ORA_2G_AVAILABILITY_RATE decimal(19,2),
ORA_CSR_update decimal(19,2),
ORA_CSSR_2G_update decimal(19,2),
ORA_2G_CALL_BLOCKING decimal(19,2),
ORA_SD_Cong decimal(19,2),
ORA_SD_DROP decimal(19,2),
ORA_2G_CDR_update decimal(19,2),
ORA_TCH_Norm_Assign_SR decimal(19,2),
ORA_2G_CS_TRAFFIC decimal(19,2),
ORA_2G_Traffic_Data_Mbytes decimal(19,2),
ORA_2G_RxQUAL_DL decimal(19,2),
ORA_TCH_Assign_FR decimal(19,2)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/GEOMARKETING/OSS_2G/'
TBLPROPERTIES ('serialization.null.format'='');


******** Tables IT *************************
CREATE TABLE CDR.SPARK_IT_GEOMARKETING_KPIS_2G (
PERIOD_START_TIME timestamp,
BSC_name varchar(50),
BCF_name varchar(50),
ORA_2G_AVAILABILITY_RATE decimal(19,2),
ORA_CSR_update decimal(19,2),
ORA_CSSR_2G_update decimal(19,2),
ORA_2G_CALL_BLOCKING decimal(19,2),
ORA_SD_Cong decimal(19,2),
ORA_SD_DROP decimal(19,2),
ORA_2G_CDR_update decimal(19,2),
ORA_TCH_Norm_Assign_SR decimal(19,2),
ORA_2G_CS_TRAFFIC decimal(19,2),
ORA_2G_Traffic_Data_Mbytes decimal(19,2),
ORA_2G_RxQUAL_DL decimal(19,2),
ORA_TCH_Assign_FR decimal(19,2),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (PERIOD_START_DATE DATE, FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');