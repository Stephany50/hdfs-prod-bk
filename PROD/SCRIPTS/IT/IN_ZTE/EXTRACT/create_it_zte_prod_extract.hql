CREATE TABLE CDR.IT_ZTE_PROD_EXTRACT (
	PROD_ID	INT,
	CREATED_DATE	TIMESTAMP,
	ROUTING_ID	INT,
	OFFER_ID	INT,
	SP_ID	INT,
	PROD_SPEC_ID	INT,
	INDEP_PROD_ID	INT,
	PROD_EXP_DATE	TIMESTAMP,
	BLOCK_REASON	VARCHAR(60),
	COMPLETED_DATE	TIMESTAMP,
	PROD_STATE	CHAR (1),
	PROD_STATE_DATE	TIMESTAMP,
	UPDATE_DATE	TIMESTAMP,
	STATE	CHAR(1),
	STATE_DATE	TIMESTAMP,
	ORIGINAL_FILE_NAME VARCHAR(50),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT,
  	INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(PROD_ID) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
