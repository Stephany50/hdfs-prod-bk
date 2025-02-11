CREATE TABLE CDR.IT_OM_COMMISSIONS (
	TRANSACTION_ID	VARCHAR(20),
	NQ_TRANSACTION_DATE	TIMESTAMP,
	COMMISSION_ID	VARCHAR(10),
	TRANSACTION_AMOUNT	DECIMAL(17,2),
	PAYER_USER_ID	VARCHAR(20),
	PAYER_CATEGORY_CODE	VARCHAR(20),
	PAYEE_USER_ID	VARCHAR(20),
	PAYEE_CATEGORY_CODE	VARCHAR(20),
	COMMISSION_AMOUNT	DECIMAL(17,2),
	SERVICE_TYPE	VARCHAR(10),
	TRANSFER_STATUS	VARCHAR(3),
	TRANSFER_SUBTYPE	VARCHAR(10),
	PAYER_DOMAIN_CODE	VARCHAR(10),
	PAYER_GRADE_NAME	VARCHAR(40),
	PAYER_MOBILE_GROUP_ROLE	VARCHAR(38),
	PAYER_GROUP_ROLE	VARCHAR(35),
	PAYER_MSISDN_ACC	VARCHAR(15),
	PARENT_PAYER_USER_ID	VARCHAR(20),
	PARENT_PAYER_USER_MSISDN	VARCHAR(15),
	OWNER_PAYER_USER_ID	VARCHAR(20),
	OWNER_PAYER_USER_MSISDN	VARCHAR(15),
	PAYER_WALLET_NUMBER	VARCHAR(25),
	PAYEE_DOMAIN_CODE	VARCHAR(10),
	PAYEE_GRADE_NAME	VARCHAR(40),
	PAYEE_MOBILE_GROUP_ROLE	VARCHAR(38),
	PAYEE_GROUP_ROLE	VARCHAR(35),
	PAYEE_MSISDN_ACC	VARCHAR(15),
	PARENT_PAYEE_USER_ID	VARCHAR(20),
	PARENT_PAYEE_USER_MSISDN	VARCHAR(15),
	OWNER_PAYEE_USER_ID	VARCHAR(20),
	OWNER_PAYEE_USER_MSISDN	VARCHAR(15),
	PAYEE_WALLET_NUMBER	VARCHAR(25),
	ORIGINAL_FILE_NAME VARCHAR(50),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT,
	ORIGINAL_FILE_DATE DATE,
	INSERT_DATE TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
CLUSTERED BY(TRANSACTION_ID) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
