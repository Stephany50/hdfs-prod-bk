INSERT INTO CDR.IT_ZTE_SUBS_EXTRACT PARTITION (ORIGINAL_FILE_DATE)
SELECT
	SUBS_ID,
	IMSI2,
	SP_ID,
	IMSI,
	PPS_PWD,
	DEF_LANG_ID,
	ROUTING_ID,
	BONUS_AMOUNT,
	CREDIT_LIMIT_PLAN_ID,
	CREDIT_LIMIT,
	FROM_UNIXTIME(UNIX_TIMESTAMP(UPDATE_DATE, 'yyyyMMdd HHmmss')) UPDATE_DATE,
	AREA_ID,
	PREFIX,
	ACC_NBR,
	CUST_ID,
	USER_ID,
	ACCT_ID,
	AGENT_ID,
	PRICE_PLAN_ID,
	ORIGINAL_FILE_NAME,
	ORIGINAL_FILE_SIZE,
	ORIGINAL_FILE_LINE_COUNT,
	CURRENT_TIMESTAMP() INSERT_DATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ZTE_SUBS_EXTRACT C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM') 
    AND B.FILE_TYPE = 'ZTE_SUBS_EXTRACT' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
);