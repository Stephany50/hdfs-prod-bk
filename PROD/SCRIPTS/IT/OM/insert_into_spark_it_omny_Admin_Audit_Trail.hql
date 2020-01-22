INSERT INTO CDR.SPARK_IT_OMNY_ADMIN_AUDIT_TRAIL PARTITION (ORIGINAL_FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
USER_ID,
USER_MSISDN,
FROM_UNIXTIME(UNIX_TIMESTAMP(LOGGED_IN,'dd/MM/yyyy HH:mm:ss')) LOGGED_IN,
FROM_UNIXTIME(UNIX_TIMESTAMP(LOG_OUT,'dd/MM/yyyy HH:mm:ss')) LOG_OUT,
CATEGORY,
ACTION_TYPE,
ACTION_PERFORMED_ON,
BARRED_USER,
REMARKS,
CREATED_BY,
FROM_UNIXTIME(UNIX_TIMESTAMP(CREATED_ON,'dd/MM/yyyy HH:mm:ss')) CREATED_ON,
ATT_1_NAME,
ATT_1_VALUE,
ATT_2_NAME,
ATT_2_VALUE,
ATT_3_NAME,
ATT_3_VALUE,
SN,
TRANSACTION_ID,
CUST_ID,
STATUS_ID,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_ADMIN_AUDIT_TRAIL C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_OMNY_ADMIN_AUDIT_TRAIL) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL