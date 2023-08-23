CREATE EXTERNAL TABLE CDR.SPARK_TT_OMNY_GRADE_DETAILS_HOURLY (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    GRADE_CODE VARCHAR(100),
    GRADE_NAME VARCHAR(100),
    CATEGORY_CODE VARCHAR(100),
    STATUS VARCHAR(100) 

)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM_HOURLY/GRADE'
TBLPROPERTIES ('serialization.null.format'='')
;


CREATE TABLE CDR.SPARK_IT_OMNY_GRADE_DETAILS_HOURLY
(
    GRADE_CODE VARCHAR(100),
    GRADE_NAME VARCHAR(100),
    CATEGORY_CODE VARCHAR(100),
    STATUS VARCHAR(100), 
    ORIGINAL_FILE_NAME  VARCHAR(50),
    ORIGINAL_FILE_SIZE  INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
   )
  PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");