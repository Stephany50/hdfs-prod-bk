
CREATE TABLE IF NOT EXISTS SMS_USAGE(
    TRANSACTION_DATE DATE,
    SMS STRING,
    INSERT_DATE TIMESTAMP
)
STORED AS ORC
TBLPROPERTIES('TRANSACTIONAL'='TRUE')
;
