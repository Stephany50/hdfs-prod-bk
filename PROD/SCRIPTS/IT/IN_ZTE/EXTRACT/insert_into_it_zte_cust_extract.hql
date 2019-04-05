INSERT INTO CDR.IT_ZTE_CUST_EXTRACT PARTITION (ORIGINAL_FILE_DATE)
SELECT
	CUST_ID,
	AGENT_ID,
	CUST_NAME,
	CUST_TYPE,
	CERT_ID,
	PARENT_ID,
	DELIVER_METHOD,
	ZIPCODE,
	ADDRESS,
	EMAIL,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CREATED_DATE, 'yyyyMMdd'))) CREATED_DATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(UPDATE_DATE, 'yyyyMMdd'))) UPDATE_DATE,
	STATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(STATE_DATE, 'yyyyMMdd'))) STATE_DATE,
	PWD,
	PARTY_TYPE,
	PARTY_CODE,
	ROUTING_ID,
	SP_ID,
	CUST_CODE,
	ORIGINAL_FILE_NAME,
	ORIGINAL_FILE_SIZE,
	ORIGINAL_FILE_LINE_COUNT,
	CURRENT_TIMESTAMP() INSERT_DATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ZTE_CUST_EXTRACT C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM') 
    AND B.FILE_TYPE = 'ZTE_CUST_EXTRACT' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
)