INSERT INTO CDR.SPARK_IT_SC_OSS_4G 
SELECT 
FROM_UNIXTIME(UNIX_TIMESTAMP(PERIOD_START_TIME, 'MM.dd.yyyy HH:mm:ss')) PERIOD_START_TIME,
MRBTS_SBTS_name,
LNBTS_name,
LNCEL_name,
4G_DL_TRAFFIC,
4G_UL_TRAFFIC,
4G_LTE_DL_USER_THRPUT,
4G_LTE_UL_USER_THRPUT,
ORA_AVG_UE_QUEUED_UL,
ORA_AVG_UE_QUEUED_DL,
ORA_AVG_NBR_UE,
PRB_LOAD_UL,
PRB_LOAD_DL,
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp)-426888) INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(PERIOD_START_TIME, 'MM.dd.yyyy HH:mm:ss'))) PERIOD_START_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -28, 10),'yyyy_MM_dd'))) FILE_DATE
FROM CDR.TT_SC_OSS_4G C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_SC_OSS_4G WHERE FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,10) AND CURRENT_DATE)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL