INSERT INTO CDR.STREAM_IT_ZTE_PRICE_PLAN_EXTRACT
SELECT
  PRICE_PLAN_ID,
  APPLY_LEVEL,
  PRICE_PLAN_NAME,
  COMMENTS,
  STATE,
  STATE_DATE,
  PRIORITY,
  PRICE_PLAN_CODE,
  SP_ID,
  WARN_LEVEL,
  ORIGINAL_FILE_NAME,
  CURRENT_TIMESTAMP() INSERT_DATE,
  ORIGINAL_FILE_DATE
FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT C
WHERE ORIGINAL_FILE_DATE IN (SELECT MAX(ORIGINAL_FILE_DATE) FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT
                            WHERE ORIGINAL_FILE_DATE >= DATE_SUB(CURRENT_DATE,3) )