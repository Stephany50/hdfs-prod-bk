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
--LEFT JOIN (
 --   SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_ZTE_CUST_EXTRACT
 --   WHERE ORIGINAL_FILE_DATE >= DATE_SUB(CURRENT_DATE,${hivevar:date_offset})
--) T ON (T.FILE_NAME = C.ORIGINAL_FILE_NAME)
--WHERE T.FILE_NAME IS NULL;
