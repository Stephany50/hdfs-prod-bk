INSERT INTO CDR.IT_CONT
SELECT
SUBS_ID  ,
ACCOUNT_NUMBER,
RESP_PAYMENT,
CUSTOMER_ID  ,
SUBSCRIBER_TYPE   ,
DEFAULT_PRICE_PLAN_ID  ,
ACCNBR  ,
ICCID  ,
IMSI  ,
PROD_STATE  ,
BLOCK_REASON  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(UPDATE_DATE, 'MM/dd/yyyy HH:mm:ss')) UPDATE_DATE  ,
CUID  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(ACTIVATION_DATE, 'MM/dd/yyyy HH:mm:ss')) ACTIVATION_DATE  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_MAJ, 'MM/dd/yyyy HH:mm:ss')) DATE_MAJ,
LOGIN_UTILISATEUR_MAJ,
ORIGINAL_FILE_NAME  ,
ORIGINAL_FILE_SIZE  ,
ORIGINAL_FILE_LINE_COUNT ,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_CONT  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.IT_CONT) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;