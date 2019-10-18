INSERT INTO CDR.STREAM_IT_ZTE_PROD_SPEC_EXTRACT
SELECT
   PROD_SPEC_ID,
  PROD_SPEC_TYPE,
  PROD_SPEC_NAME,
  COMMENTS,
  STD_CODE,
  EFF_DATE,
  EXP_DATE,
  STATE,
  STATE_DATE,
  SP_ID,
  ORIGINAL_FILE_NAME,
  ORIGINAL_FILE_DATE,
  CURRENT_TIMESTAMP() INSERT_DATE
FROM CDR.IT_ZTE_PROD_SPEC_EXTRACT C
--WHERE ORIGINAL_FILE_DATE IN (SELECT
--MAX(ORIGINAL_FILE_DATE) FROM CDR.IT_ZTE_PROD_SPEC_EXTRACT
-- WHERE ORIGINAL_FILE_DATE >= DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) )
