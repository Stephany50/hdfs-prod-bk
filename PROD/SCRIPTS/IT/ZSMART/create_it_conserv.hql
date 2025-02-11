CREATE  TABLE CDR.IT_CONSERV
(

Subs_id VARCHAR(200),
service_type VARCHAR(200),
Offer_name VARCHAR(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE VARCHAR(200),
ORIGINAL_FILE_LINE_COUNT VARCHAR(200),
INSERT_DATE timestamp
)
PARTITIONED BY (original_file_date DATE)
CLUSTERED BY(Subs_id) INTO 128 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");