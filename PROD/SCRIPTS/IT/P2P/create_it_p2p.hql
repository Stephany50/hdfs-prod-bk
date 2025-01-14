
CREATE TABLE CDR.IT_P2P_LOG (
	COMMAND_NAME VARCHAR(100),
	USSD_ORDER VARCHAR(100),
	RETURN_CODE VARCHAR(25),
	START_DATE_TIME TIMESTAMP,
	END_DATE_TIME TIMESTAMP,
	DURATION INT,
	MSISDN_SRC VARCHAR(25),
	CREDIT_SRC DECIMAL(20, 2),
	VALIDITY_DATE_SRC TIMESTAMP,
	INVALIDITY_DATE_SRC TIMESTAMP,
	PROFILE_SRC VARCHAR(40),
	MSISDN_DEST VARCHAR(25),
	CREDIT_DEST DECIMAL(20, 2),
	VALIDITY_DATE_DEST TIMESTAMP,
	INVALIDITY_DATE_DEST TIMESTAMP,
	TRANSACTION_COST DECIMAL(20, 2),
	TOTAL_AMOUNT_DEBIT DECIMAL(20, 2),
	ROLLBACK_FLAG VARCHAR(25),
	ROLLBACK_RESULT VARCHAR(25),
	COMMAND_END_DATE TIMESTAMP,
	AMOUNT DECIMAL(20, 2),
	ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_DATE  TIMESTAMP,
    INSERT_DATE TIMESTAMP,
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT
 )
PARTITIONED BY (START_DATE DATE)
CLUSTERED BY(MSISDN_SRC) INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")

