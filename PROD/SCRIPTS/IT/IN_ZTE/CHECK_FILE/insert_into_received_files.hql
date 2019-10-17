INSERT INTO RECEIVED_FILES PARTITION(ORIGINAL_FILE_MONTH)
SELECT
  ORIGINAL_FILE_NAME,
  IF(ORIGINAL_FILE_NAME like '%CHECK_FILELIST_POSTPAID%','ZTE_CHECKFILE_POSTPAID','ZTE_CHECKFILE_PREPAID') FILE_TYPE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -14, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  MAX(ORIGINAL_FILE_SIZE),
  MAX(ORIGINAL_FILE_LINE_COUNT),
  CURRENT_TIMESTAMP,
  DATE_FORMAT(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -14, 8),'yyyyMMdd'))), 'yyyy-MM') ORIGINAL_FILE_MONTH
FROM CDR.TT_ZTE_CHECK_FILE C
--WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(current_date, 'yyyy-MM') AND (B.FILE_TYPE = 'ZTE_CHECKFILE_PREPAID' OR B.FILE_TYPE = 'ZTE_CHECKFILE_POSTPAID') AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME)
--GROUP BY ORIGINAL_FILE_NAME;