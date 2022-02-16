INSERT INTO CDR.SPARK_IT_SC_OSS_3G_SUBUNIT_LOAD
SELECT 
FROM_UNIXTIME(UNIX_TIMESTAMP(PERIOD_START_TIME, 'MM.dd.yyyy HH:mm:ss')) PERIOD_START_TIME,
PLMN_name,
RNC_name,
WBTS_name,
WBTS_ID,
NUM_BB_SUBUNITS,
MAX_USED_BB_SUBUNITS,
AVG_USED_BB_SUBUNITS,
Max_BB_SUs_Util_ratio,
Avg_BB_SUs_Util_BTS,
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp)-425888) INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(PERIOD_START_TIME, 'MM.dd.yyyy HH:mm:ss'))) PERIOD_START_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -27, 10),'yyyy_MM_dd'))) FILE_DATE
FROM CDR.TT_SC_OSS_3G_SUBUNIT_LOAD C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_SC_OSS_3G_SUBUNIT_LOAD  WHERE FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,10) AND CURRENT_DATE)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL