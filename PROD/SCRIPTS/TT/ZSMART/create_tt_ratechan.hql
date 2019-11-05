CREATE EXTERNAL TABLE CDR.TT_RATECHAN (

Subs_id VARCHAR(200)
,OLD_DEFAULT_PRICE_PLAN_ID VARCHAR(200)
,NEW_DEFAULT_PRICE_PLAN_ID VARCHAR(200)
,Update_date VARCHAR(200)
,CUID VARCHAR(200)
,ORIGINAL_FILE_NAME VARCHAR(200)
,ORIGINAL_FILE_SIZE INT
,ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'CHANSIM external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZMART/RATECHAN_'
TBLPROPERTIES ('serialization.null.format'='');