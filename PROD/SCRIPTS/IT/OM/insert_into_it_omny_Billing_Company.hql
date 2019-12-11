INSERT INTO CDR.IT_OMNY_BILLING_COMPANY PARTITION (ORIGINAL_FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
COMPANY_CATEGORY,
COMPANY_NAME,
COMPANY_CODE,
ADDRESS,
CITY,
DISTRICT,
COUNTRY,
ACCOUNT_STATUS,
FROM_UNIXTIME(UNIX_TIMESTAMP(CREATED_ON,'dd/MM/yyyy HH:mm:ss')) CREATED_ON,
NOTIFICATION_NAME,
FROM_UNIXTIME(UNIX_TIMESTAMP(DELETION_APPROVED_ON,'dd/MM/yyyy HH:mm:ss')) DELETION_APPROVED_ON,
DELETION_APPROVED_BY,
LOGIN_ID,
EMAIL,
CONTACT_PERSON,
CONTACT_NUMBER,
PAID_BILL_NOTIFICATION,
AUTO_BILL_DELETE_FREQUENCY,
USER_PROFILE_ID,
PROFILE_NAME,
CREATION_INITIATED_ON,
CREATION_INITIATED_BY,
FROM_UNIXTIME(UNIX_TIMESTAMP(CREATION_APPROVED_ON,'dd/MM/yyyy HH:mm:ss')) CREATION_APPROVED_ON,
CREATION_APPROVED_BY,
FROM_UNIXTIME(UNIX_TIMESTAMP(LAST_MODIFIED_ON,'dd/MM/yyyy HH:mm:ss')) LAST_MODIFIED_ON,
LAST_MODIFIED_BY,
FROM_UNIXTIME(UNIX_TIMESTAMP(DELETION_INITIATED_ON,'dd/MM/yyyy HH:mm:ss')) DELETION_INITIATED_ON,
DELETION_INITIATED_BY,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_BILLING_COMPANY C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.IT_OMNY_BILLING_COMPANY) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;