INSERT INTO CDR.SPARK_IT_DMC_GMK_4G
SELECT
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
PERIOD_START_TIME,
MRBTS_SBTS_NAME,
LNBTS_NAME,
LNCEL_NAME,
DN,
CELL_AVAIL_EXCL_BLU,
CELL_AVAIL,
4G_NUR,
ORA_4G_CALL_DROP_WO_VOLTE_NEW,
ORA_4G_CSSR_WITHOUT_VOLTE_NEW,
ORA_LTE_DL_USER_THROUGHPUT_NEW,
ORA_LTE_UL_USER_THROUGHPUT_NEW,
TRAFFICNEWDLSRAN1,
LTE_DL_UL_TRAFFIC_VOLUME_NEW_MBYTE,
4G_LTE_DL_TRAFFIC_VOLUME_NEW_MBITES,
ORA_4G_ERAB_SETUP_SR_NEW,
SRAN_OSKC_AVERAGE_NUMBER_OF_UE_QUEUED_DL_NEW,
PERC_DL_PRB_UTIL,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) EVENT_DATE
FROM CDR.TT_DMC_GMK_4G C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM  CDR.SPARK_IT_DMC_GMK_4G)T ON T.FILE_NAME= C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL