INSERT INTO RECEIVED_FILES PARTITION(ORIGINAL_FILE_MONTH)
SELECT
  ORIGINAL_FILE_NAME,
  '${hivevar:cdr_type}' FILE_TYPE,
  SUBSTRING(ORIGINAL_FILE_NAME,-14,10) ORIGINAL_FILE_DATE,
  MAX(ORIGINAL_FILE_SIZE),
  MAX(ORIGINAL_FILE_LINE_COUNT),
  CURRENT_TIMESTAMP,
  SUBSTRING(ORIGINAL_FILE_NAME,-14,7)  ORIGINAL_FILE_MONTH
FROM ${hivevar:tt_table_name} C
WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM')
                   AND DATE_FORMAT(current_date, 'yyyy-MM') AND B.FILE_TYPE = '${hivevar:cdr_type}' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME)
GROUP BY ORIGINAL_FILE_NAME