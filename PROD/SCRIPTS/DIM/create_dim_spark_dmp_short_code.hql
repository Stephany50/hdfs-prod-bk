
CREATE TABLE DIM.SPARK_DMP_SHORT_CODES
(
  SHORT_CODE_ID INT,
  SUPPLIER_ID VARCHAR(8),
  SERVICE_NAME VARCHAR(30),
  CREATION_DATE DATE,
  USER_CREATION VARCHAR(5),
  UPDATE_DATE DATE,
  USER_UPDATE VARCHAR(5),
  SHORT_NUMBER VARCHAR(9),
  LONG_NUMBER VARCHAR(9),
  SVA_RATE INT,
  SVA_UNIT VARCHAR(5),
  SVA_CATEGORY VARCHAR(11),
  IS_CONTENT_EXTERNAL VARCHAR(1),
  SUPPLIER_REVENUE_RATE INT,
  SHORT_CODE_TYPE VARCHAR(10),
  VALID_FROM_DATECODE VARCHAR(8),
  VALID_TO_DATECODE VARCHAR(8)
) STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');