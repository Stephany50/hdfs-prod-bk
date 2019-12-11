INSERT INTO CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
SELECT
  PROD_SPEC_ID,
  PROD_SPEC_TYPE,
  PROD_SPEC_NAME,
  COMMENTS	,
  STD_CODE	,
  EFF_DATE	,
  EXP_DATE	,
  STATE	,
  STATE_DATE,
  SP_ID	,
  ORIGINAL_FILE_NAME,
  CURRENT_TIMESTAMP() INSERT_DATE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME,-12,8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ZTE_PROD_SPEC_EXTRACT C
LEFT JOIN (
   SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
   WHERE ORIGINAL_FILE_DATE >= DATE_SUB(CURRENT_DATE,3)
) T ON (T.FILE_NAME = C.ORIGINAL_FILE_NAME)
WHERE T.FILE_NAME IS NULL