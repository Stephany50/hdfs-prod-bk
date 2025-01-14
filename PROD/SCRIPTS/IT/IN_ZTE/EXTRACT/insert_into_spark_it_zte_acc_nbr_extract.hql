INSERT INTO CDR.SPARK_IT_ZTE_ACC_NBR_EXTRACT PARTITION (ORIGINAL_FILE_DATE)
SELECT
	SP_ID,
	PEER_OPERATOR_CODE,
	DAYS,
	PRE_CHARGING,
	PPS_PWD,
	COMMENTS,
	FROM_UNIXTIME(UNIX_TIMESTAMP(STATE_DATE, 'yyyyMMdd HHmmss')) STATE_DATE,
	AREA_ID,
	HLR_ID,
	ACC_NBR_STATE,
	ACC_NBR_TYPE_ID,
	ACC_NBR_CLASS_ID,
	ORG_ID,
	STAFF_ID,
	ACC_NBR,
	PREFIX,
	FROM_UNIXTIME(UNIX_TIMESTAMP(RESERVE_EXP_DATE, 'yyyyMMdd HHmmss')) RESERVE_EXP_DATE,
	ACC_NBR_ID,
	ORIGINAL_FILE_NAME,
	ORIGINAL_FILE_SIZE,
	ORIGINAL_FILE_LINE_COUNT,
	CURRENT_TIMESTAMP() INSERT_DATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ZTE_ACC_NBR_EXTRACT C
LEFT JOIN (
    SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_ZTE_ACC_NBR_EXTRACT
    WHERE ORIGINAL_FILE_DATE >= DATE_SUB(CURRENT_DATE,3)
) T ON (T.FILE_NAME = C.ORIGINAL_FILE_NAME)
WHERE T.FILE_NAME IS NULL