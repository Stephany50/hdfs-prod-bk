CREATE TABLE CDR.IT_ZTE_ACC_NBR_EXTRACT (
	SP_ID	INT,
	PEER_OPERATOR_CODE	VARCHAR (60),
	DAYS	INT,
	PRE_CHARGING	INT,
	PPS_PWD	VARCHAR (60),
	COMMENTS	VARCHAR (4000),
	STATE_DATE	TIMESTAMP,
	AREA_ID	INT,
	HLR_ID	INT,
	ACC_NBR_STATE	CHAR (1),
	ACC_NBR_TYPE_ID	INT,
	ACC_NBR_CLASS_ID	INT,
	ORG_ID	INT,
	STAFF_ID	INT,
	ACC_NBR	VARCHAR (60),
	PREFIX	VARCHAR (60),
	RESERVE_EXP_DATE	TIMESTAMP,
	ACC_NBR_ID	INT,
	ORIGINAL_FILE_NAME VARCHAR(50),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT,
  	INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(ACC_NBR) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
