CREATE  TABLE CDR.IT_CONT
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
update_date timestamp,
CUID VARCHAR(200),
activation_date timestamp,
DATE_MAJ VARCHAR(200),
LOGIN_UTILISATEUR_MAJ VARCHAR(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE VARCHAR(200),
ORIGINAL_FILE_LINE_COUNT VARCHAR(200),
INSERT_DATE timestamp
)
PARTITIONED BY (original_file_date DATE)
CLUSTERED BY(Subs_id) INTO 64 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");