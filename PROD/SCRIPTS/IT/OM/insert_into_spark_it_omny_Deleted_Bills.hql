INSERT INTO CDR.SPARK_IT_OMNY_DELETED_BILLS PARTITION (ORIGINAL_FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
BILLER_CODE ,
BILL_ACCOUNT_NUMBER,
BILL_NUMBER,
BILL_STATUS,
AMOUNT,
FROM_UNIXTIME(UNIX_TIMESTAMP(BILL_DUE_DATE,'dd/MM/yyyy HH:mm:ss')) BILL_DUE_DATE,
FROM_UNIXTIME(UNIX_TIMESTAMP(DELETION_DATE_TIME,'dd/MM/yyyy HH:mm:ss')) DELETION_DATE_TIME,
FROM_UNIXTIME(UNIX_TIMESTAMP(BILL_CREATED_ON,'dd/MM/yyyy HH:mm:ss')) BILL_CREATED_ON,
FROM_UNIXTIME(UNIX_TIMESTAMP(LAST_MODIFIED_ON,'dd/MM/yyyy HH:mm:ss')) LAST_MODIFIED_ON,
FILE_NAME_USED_FOR_UPLOAD,
USER_ID,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_DELETED_BILLS C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_OMNY_DELETED_BILLS) T ON T.file_name = C.original_file_name