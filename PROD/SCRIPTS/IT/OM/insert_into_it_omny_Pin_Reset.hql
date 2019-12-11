INSERT INTO CDR.IT_OMNY_PIN_RESET PARTITION (ORIGINAL_FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
TARGET_USER_ID,
TARGET_USER_MSISDN,
TARGET_USER_CATEGORY_CODE,
TARGET_USER_NAME,
TARGET_USER_LAST_NAME,
CHANGED_BY_USER_ID,
CHANGED_BY_USER_MSISDN,
CHANGED_BY_USER_CATEGORY_CODE,
CHANGED_BY_USER_LOGIN_ID,
CHANGED_BY_USER_NAME,
CHANGED_BY_USER_LAST_NAME,
FROM_UNIXTIME(UNIX_TIMESTAMP(CREATED_ON,'dd/MM/yyyy HH:mm:ss')) CREATED_ON,
ACTION_TYPE,
REMARKS,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_PIN_RESET C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.IT_OMNY_PIN_RESET) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL