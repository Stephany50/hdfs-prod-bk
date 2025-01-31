CREATE TABLE TMP.SPARK_IT_ZTE_PROFILE (
   PROFILE_ID STRING,
   PROFILE_NAME STRING,
   INSERT_DATE TIMESTAMP
)
COMMENT 'SPARK_IT_ZTE_PROFILE - TMP'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');