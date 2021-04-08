CREATE EXTERNAL TABLE CDR.TT_MY_ORANGE_USERS_BACKEND
(
    date1 VARCHAR(100),
    date2 VARCHAR(100),
    country VARCHAR(100),
    store VARCHAR(100),
    msisdn VARCHAR(100),
    index VARCHAR(100)
)COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/PROD/TT/MY_ORANGE/USERS_BACKEND'
TBLPROPERTIES ('serialization.null.format'='')






COMMENT 'external tables-TT'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = ","
)
LOCATION '/PROD/TT/MY_ORANGE/USERS_BACKEND'
TBLPROPERTIES ('serialization.null.format'='');





