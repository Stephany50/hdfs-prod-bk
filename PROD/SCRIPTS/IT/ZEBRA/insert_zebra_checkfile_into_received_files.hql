INSERT INTO RECEIVED_FILES PARTITION(ORIGINAL_FILE_MONTH)
SELECT
  ORIGINAL_FILE_NAME,
  'ZEBRA_CHECKFILE_CDR' FILE_TYPE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -10, 6),'ddMMyy')))  ORIGINAL_FILE_DATE,
  MAX(ORIGINAL_FILE_SIZE),
  MAX(ORIGINAL_FILE_LINE_COUNT),
  CURRENT_TIMESTAMP,
  DATE_FORMAT(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -10, 6),'ddMMyy'))), 'yyyy-MM') ORIGINAL_FILE_MONTH
FROM CDR.TT_ZEBRA_CHECKFILE C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM RECEIVED_FILES WHERE ORIGINAL_FILE_MONTH BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}),'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE,'yyyy-MM') )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
GROUP BY ORIGINAL_FILE_NAME;