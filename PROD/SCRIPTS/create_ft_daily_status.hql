
CREATE TABLE IF NOT EXISTS FT_DAILY_STATUS(
    TABLE_DATE STRING,
    TABLE_TYPE STRING,
    TABLE_NAME STRING,
    NB_ROWS INT,
    TABLE_INSERT_DATE TIMESTAMP,
    INSERT_DATE TIMESTAMP
)
STORED AS ORC
TBLPROPERTIES('transactional'='true')
;