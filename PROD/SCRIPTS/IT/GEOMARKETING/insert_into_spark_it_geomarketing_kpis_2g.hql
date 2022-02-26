INSERT INTO CDR.SPARK_IT_GEOMARKETING_KPIS_2G
SELECT 
FROM_UNIXTIME(UNIX_TIMESTAMP(PERIOD_START_TIME, 'MM.dd.yyyy HH:mm:ss')) PERIOD_START_TIME,
BSC_name,
BCF_name,
ORA_2G_AVAILABILITY_RATE,
ORA_CSR_update,
ORA_CSSR_2G_update,
ORA_2G_CALL_BLOCKING,
ORA_SD_Cong,
ORA_SD_DROP,
ORA_2G_CDR_update,
ORA_TCH_Norm_Assign_SR,
ORA_2G_CS_TRAFFIC,
ORA_2G_Traffic_Data_Mbytes,
ORA_2G_RxQUAL_DL,
ORA_TCH_Assign_FR,
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
FROM_UNIXTIME(UNIX_TIMESTAMP(current_timestamp)-428888) INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(PERIOD_START_TIME, 'MM.dd.yyyy HH:mm:ss'))) PERIOD_START_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -27, 10),'yyyy_MM_dd'))) FILE_DATE
FROM CDR.TT_GEOMARKETING_KPIS_2G C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_GEOMARKETING_KPIS_2G  WHERE FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,10) AND CURRENT_DATE)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL