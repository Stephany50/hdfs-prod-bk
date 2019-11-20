CREATE EXTERNAL TABLE CDR.TT_CUSTGROUP
(
Distributeur VARCHAR(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE VARCHAR(200),
ORIGINAL_FILE_LINE_COUNT VARCHAR(200)
)
COMMENT 'CUSTGROUP external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZSMART/CUSTGROUP_'
TBLPROPERTIES ('serialization.null.format'='');
