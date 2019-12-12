CREATE TABLE CDR.IT_ZTE_BAL_EXTRACT (
	BAL_ID	INT,
	ACCT_ID	INT,
	ACCT_RES_ID	INT,
	GROSS_BAL	DOUBLE,
	RESERVE_BAL	DOUBLE,
	CONSUME_BAL	DOUBLE,
	RATING_BAL	DOUBLE,
	BILLING_BAL	DOUBLE,
	EFF_DATE	TIMESTAMP,
	EXP_DATE	TIMESTAMP,
	UPDATE_DATE	TIMESTAMP,
	CEIL_LIMIT	DOUBLE,
	FLOOR_LIMIT	DOUBLE,
	DAILY_CEIL_LIMIT	DOUBLE,
	DAILY_FLOOR_LIMIT	DOUBLE,
	PRIORITY	DOUBLE,
	LAST_BAL	DOUBLE,
	LAST_RECHARGE	DOUBLE,
	ORIGINAL_FILE_NAME VARCHAR(50),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT,
  	INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(BAL_ID,ACCT_ID) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
