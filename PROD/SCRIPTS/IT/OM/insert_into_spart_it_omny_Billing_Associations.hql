INSERT INTO CDR.SPARK_IT_OMNY_BILLING_ASSOCIATIONS PARTITION (ORIGINAL_FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
USER_ID,
MSISDN,
USER_FIRST_NAME,
USER_LAST_NAME,
ACCOUNT_STATUS,
BILL_COMPANY_CODE,
BILL_COMPANY_NAME,
COMPANY_TYPE,
SOUSCR_CREATED_BY,
SOUSCR_CREATED_BY_MSISDN,
FROM_UNIXTIME(UNIX_TIMESTAMP(CREATED_ON,'dd/MM/yyyy HH:mm:ss')) CREATED_ON,
SOUSCR_MODIFIED_BY,
SOUSCR_MODIFIED_BY_MSISDN,
FROM_UNIXTIME(UNIX_TIMESTAMP(MODIFIED_ON,'dd/MM/yyyy HH:mm:ss')) MODIFIED_ON,
BILL_ACCOUNT_NUMBER,
NOTIF_TYPE_OBU,
NOTIF_TYPE_OBD,
NOTIF_TYPE_BND,
REG_FORM_NUMBER,
NOTIF_FORM_NUMBER,
COMPANY_FORM_NUMBER,
USER_TYPE,
ACTION_TYPE	,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_BILLING_ASSOCIATIONS C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_OMNY_BILLING_ASSOCIATIONS) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL