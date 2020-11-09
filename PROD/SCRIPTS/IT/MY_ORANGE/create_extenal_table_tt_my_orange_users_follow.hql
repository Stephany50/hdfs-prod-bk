CREATE EXTERNAL TABLE CDR.TT_MY_ORANGE_USERS_FOLLOW
(
    event_date VARCHAR(100),
    number_users_follow bigint
)
COMMENT 'external tables-TT for users follow'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/PROD/TT/MY_ORANGE/USERS_FOLLOW'
TBLPROPERTIES ('serialization.null.format'='')