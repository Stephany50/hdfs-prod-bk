INSERT INTO CDR.SPARK_IT_OMNY_PIN_MODIFIED
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
USER_ID,
USER_NAME,
CATEGORY_CODE,
MSISDN,
LAST_NAME,
USER_TYPE,
FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSACTION_ON,'dd/MM/yyyy HH:mm:ss')) TRANSACTION_ON,
STATUS,
ERROR_CODE,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_PIN_MODIFIED C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_OMNY_PIN_MODIFIED) T 
ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL