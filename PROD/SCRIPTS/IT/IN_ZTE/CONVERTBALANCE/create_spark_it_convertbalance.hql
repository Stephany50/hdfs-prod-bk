
---***********************************************************---
------------ CREATE IT Table- CONVERTBALANCE TT AND IT -------------------
---***********************************************************---

-- TT.CONVERTBALANCE
CREATE EXTERNAL TABLE TT.CONVERTBALANCE (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,   
MSISDN VARCHAR(200),   
CHANNEL_ID INT, 
BAL_ID_LIST_SOURCE VARCHAR(200),  
BAL_ID_DEST VARCHAR(200),
CONVERTION_TYPE INT,
INPUT_VALUE DECIMAL(19, 5),
OUTPUT_VALUE DECIMAL(19, 5),
INPUT_BALANCE INT,
OUTPUT_BALANCE INT,
CHARGE_FEE DECIMAL(19, 5),
DATETIME VARCHAR(200)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/CONVERTBALANCE'
TBLPROPERTIES ('serialization.null.format'='');

-- CDR.SPARK_IT_CONVERTBALANCE
CREATE TABLE CDR.SPARK_IT_CONVERTBALANCE (
	MSISDN VARCHAR(200),
	CHANNEL_ID INT,
	BAL_ID_LIST_SOURCE VARCHAR(200),
	BAL_ID_DEST VARCHAR(200),
	CONVERTION_TYPE INT,
	INPUT_VALUE DECIMAL(19, 5),
	OUTPUT_VALUE DECIMAL(19, 5),
	INPUT_BALANCE INT,
	OUTPUT_BALANCE INT,
	CHARGE_FEE DECIMAL(19, 5),
	DATETIME TIMESTAMP,
	ORIGINAL_FILE_NAME VARCHAR(200),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT,
	INSERTED_DATE TIMESTAMP,
	ORIGINAL_FILE_DATE DATE
)COMMENT 'CDR_SPARK_IT_CONVERTBALANCE'
PARTITIONED BY (CREATEDDATE DATE,FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

ALTER TABLE CDR.SPARK_IT_CONVERTBALANCE ADD COLUMNS (CHARGE_TEST DECIMAL(19, 5));
