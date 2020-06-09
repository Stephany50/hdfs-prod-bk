INSERT INTO CDR.SPARK_IT_OMNY_COMMISSION PARTITION (TRANSACTION_DATE, FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
COMMISSION_ID,
NETWORK_NAME,
PROFILE_NAME,
SHORT_CODE,
SERVICE_TYPE,
PAYER_GRADE_CODE,
PAYEE_GRADE_CODE,
SERVICE_CHARGE_VERSION,
FROM_UNIXTIME(UNIX_TIMESTAMP(START_DATETIME,'dd/MM/yyyy HH:mm:ss')) START_DATETIME,
MIN_TRANSFER_VALUE,
MAX_TRANSFER_VALUE,
MULTIPLE_OF,
MIN_FIXED_COMMISSION,
MAX_FIXED_COMMISSION,
MIN_PCT_COMMISSION,
MAX_PCT_COMMISSION,
START_RANGE,
END_RANGE,
FIXED_COMMISSION,
PCT_COMMISSION,
PAYER_CATEGORY_CODE,
PAYEE_CATEGORY_CODE,
PAYER_PAYMENT_TYPE_ID,
PAYEE_PAYMENT_TYPE_ID,
PAYER_TYPE,
PAYEE_TYPE,
SLAB_CODE,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) TRANSACTION_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) FILE_DATE
FROM CDR.TT_OMNY_COMMISSION C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_OMNY_COMMISSION WHERE TRANSACTION_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL