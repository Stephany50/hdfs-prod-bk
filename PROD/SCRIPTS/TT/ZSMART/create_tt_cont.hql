CREATE EXTERNAL TABLE CDR.TT_CONT
(

Subs_id VARCHAR(200),
Customer_id VARCHAR(200),
Subscriber_type  VARCHAR(200),
Default_price_plan_ID VARCHAR(200),
ACCNBR VARCHAR(200),
ICCID VARCHAR(200),
IMSI VARCHAR(200),
PROD_STATE VARCHAR(200),
block_reason VARCHAR(200),
update_date VARCHAR(200),
CUID VARCHAR(200),
activation_date VARCHAR(200),
DATE_MAJ VARCHAR(200),
LOGIN_UTILISATEUR_MAJ VARCHAR(200),

ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE VARCHAR(200),
ORIGINAL_FILE_LINE_COUNT VARCHAR(200)

)
COMMENT 'CONT external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZSMART/CONT_'
TBLPROPERTIES ('serialization.null.format'='');

Subs_id  ,
Customer_id  ,
Subscriber_type   ,
Default_price_plan_ID  ,
ACCNBR  ,
ICCID  ,
IMSI  ,
PROD_STATE  ,
block_reason  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(update_date, 'dd/MM/yy hh:mm:ss')) update_date  ,
CUID  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(activation_date, 'dd/MM/yy hh:mm:ss')) activation_date  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_MAJ, 'dd/MM/yy hh:mm:ss')) DATE_MAJ,
LOGIN_UTILISATEUR_MAJ,