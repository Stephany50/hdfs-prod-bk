CREATE TABLE DIM.DT_VAS_OPERATOR (
    SHORT_LONG_NUMBER VARCHAR(10), 
	OPERATOR_NAME VARCHAR(50), 
	OPERATOR_DESC VARCHAR(50), 
	OPERATOR_ADRESS VARCHAR(50), 
	SOURCE_PLATFORM VARCHAR(40), 
	ORIGINAL_FILE_NAME VARCHAR(60), 
	INSERT_DATE TIMESTAMP,
	REFRESH_DATE TIMESTAMP
) STORED AS ORC