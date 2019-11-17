CREATE EXTERNAL TABLE CDR.TT_SERVICE
(
	SERVICE_ID
	ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE VARCHAR(200),
ORIGINAL_FILE_LINE_COUNT VARCHAR(200)
)
COMMENT 'SERVICE external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZMART/SERVICE_'
TBLPROPERTIES ('serialization.null.format'='');