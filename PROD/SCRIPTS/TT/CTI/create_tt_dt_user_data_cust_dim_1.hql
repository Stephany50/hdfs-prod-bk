CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_USER_DATA_CUST_DIM_1 (
	ID                       VARCHAR(255),
	TENANT_KEY               VARCHAR(255),
	CREATE_AUDIT_KEY         VARCHAR(255),
	DIM_ATTRIBUTE_1          VARCHAR(255),
	DIM_ATTRIBUTE_2          VARCHAR(255),
	DIM_ATTRIBUTE_3          VARCHAR(255),
	DIM_ATTRIBUTE_4          VARCHAR(255),
	DIM_ATTRIBUTE_5          VARCHAR(255),
	ORIGINAL_FILE_NAME VARCHAR(100),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u003B'
LOCATION '/PROD/TT/CTI'
TBLPROPERTIES ('serialization.null.format'='')