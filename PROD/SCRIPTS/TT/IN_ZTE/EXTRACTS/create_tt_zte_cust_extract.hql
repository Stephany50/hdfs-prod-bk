CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_ZTE_CUST_EXTRACT (
	CUST_ID	INT,
	AGENT_ID	INT,
	CUST_NAME	VARCHAR(255),
	CUST_TYPE	CHAR (1),
	CERT_ID	INT,
	PARENT_ID	INT,
	DELIVER_METHOD	CHAR (1),
	ZIPCODE	VARCHAR(60),
	ADDRESS	VARCHAR(255),
	EMAIL	VARCHAR(255),
	CREATED_DATE	VARCHAR(20),
	UPDATE_DATE	VARCHAR(20),
	STATE	CHAR (1),
	STATE_DATE	VARCHAR(20),
	PWD	VARCHAR (60),
	PARTY_TYPE	CHAR (1),
	PARTY_CODE	VARCHAR(30),
	ROUTING_ID	INT,
	SP_ID	INT,
	CUST_CODE	VARCHAR(60),
	ORIGINAL_FILE_NAME VARCHAR(50),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'ZTE CUST EXTRACT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/EXTRACT_CUST'
TBLPROPERTIES ('serialization.null.format'='');
