INSERT INTO RECEIVED_FILES PARTITION(ORIGINAL_FILE_MONTH)
SELECT
  IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_NAME,FILE_TAP_ID) ORIGINAL_FILE_NAME,
  'ZTE_DATA_CDR' FILE_TYPE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_NAME,FILE_TAP_ID), -25, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  MAX(IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_SIZE,CAST(ISMP_PRODUCT_ID AS INT))),
  MAX(IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_LINE_COUNT,CAST(ORIGINAL_FILE_NAME AS INT))),
  CURRENT_TIMESTAMP,
  DATE_FORMAT(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_NAME,FILE_TAP_ID), -25, 8),'yyyyMMdd'))), 'yyyy-MM') ORIGINAL_FILE_MONTH
FROM CDR.TT_ZTE_DATA C
--WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM')      AND DATE_FORMAT(current_date, 'yyyy-MM') AND B.FILE_TYPE = 'ZTE_DATA_CDR' AND (B.ORIGINAL_FILE_NAME = IF(C.ORIGINAL_FILE_NAME LIKE '%in_pr%',C.ORIGINAL_FILE_NAME,C.FILE_TAP_ID)))
--GROUP BY IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_NAME,FILE_TAP_ID);