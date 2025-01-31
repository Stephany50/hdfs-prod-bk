
INSERT INTO CDR.SPARK_IT_DMC_GMK_3G
SELECT
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
PERIOD_START_TIME,
PLMN_NAME,
RNC_NAME,
WBTS_NAME,
WBTS_ID,
WCEL_NAME,
WCEL_ID,
DN,
GRP_CELL_AVAILABILITY,
3G_NUR,
GRP_TRAFFIC_SPEECH_ERLANGS,
GRP_TOTAL_DATA_TRAFFIC_VOL_DLUL,
GRP_3G_DROP_CALL_CS,
ORA_CSSR_CS_CELLPCH_URAPCHNEW,
ORA_CSSR_PS_CELLPCH_URAPCHNEW,
ORA_HSUPA_USER_THROUGHPUT_NEW,
ORA_HSDPA_USER_THROUGHPUT_NEW,
GRP_NUMBER_OF_USERS_QUEUED,
CS_DROP_NB,
HS_DSCH_CREDIT_REDUCTIONS_DUE_TO_FRAME_LOSS,
ORA_3G_TRAFFIC_SPEECH,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) EVENT_DATE
FROM CDR.TT_DMC_GMK_3G C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM  CDR.SPARK_IT_DMC_GMK_3G)T ON T.FILE_NAME= C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL