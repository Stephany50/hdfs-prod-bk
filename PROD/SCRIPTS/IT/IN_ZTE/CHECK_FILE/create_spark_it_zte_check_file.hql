CREATE TABLE CDR.SPARK_IT_ZTE_CHECK_FILE (
   CDR_NAME VARCHAR (100),
   CDR_TYPE VARCHAR(50),
   CDR_GEN_DATE TIMESTAMP,
   CDR_FILE_SIZE INT,
   CDR_LINE_COUNT INT,
   ORIGINAL_FILE_NAME VARCHAR (100),
   ORIGINAL_FILE_DATE DATE,
   INSERT_DATE TIMESTAMP
)
PARTITIONED BY (CDR_DATE DATE,FILE_DATE DATE)
CLUSTERED BY (CDR_TYPE) INTO 4 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')