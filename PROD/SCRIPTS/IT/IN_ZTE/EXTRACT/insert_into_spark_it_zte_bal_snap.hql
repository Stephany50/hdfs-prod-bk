INSERT INTO CDR.SPARK_IT_ZTE_BAL_SNAP PARTITION (ORIGINAL_FILE_DATE)
SELECT
	CREATE_DATE,
    BAL_ID,
	ACCT_ID,
	ACCT_RES_ID,
	GROSS_BAL,
	RESERVE_BAL,
	CONSUME_BAL,
	RATING_BAL,
	BILLING_BAL,
	EFF_DATE,
	EXP_DATE,
	UPDATE_DATE,
	CEIL_LIMIT,
	FLOOR_LIMIT,
	DAILY_CEIL_LIMIT,
	DAILY_FLOOR_LIMIT,
	PRIORITY,
	LAST_BAL,
	LAST_RECHARGE,
	ORIGINAL_FILE_NAME,
	ORIGINAL_FILE_SIZE,
	ORIGINAL_FILE_LINE_COUNT,
	CURRENT_TIMESTAMP() INSERT_DATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ZTE_BAL_SNAP C
LEFT JOIN (
   SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_ZTE_BAL_SNAP
   WHERE ORIGINAL_FILE_DATE >= DATE_SUB(CURRENT_DATE,3)
) T ON (T.FILE_NAME = C.ORIGINAL_FILE_NAME)
WHERE T.FILE_NAME IS NULL;