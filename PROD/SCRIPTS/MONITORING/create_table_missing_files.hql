
CREATE TABLE IF NOT EXISTS MISSING_FILES2(
    ORIGINAL_FILE_DATE STRING,
    TABLE_SOURCE STRING,
    FLUX_TYPE STRING,
    FLUX_NAME STRING,
    FILE_NAME STRING,
    INSERT_DATE TIMESTAMP
)
CLUSTERED BY(FLUX_TYPE) INTO 20 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")

;
