INSERT INTO CDR.IT_OMNY_RECIEVER_KYC PARTITION (ORIGINAL_FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
MSISDN              ,
UNREG_USER_ID       ,
UNREG_FIRST_NAME    ,
UNREG_LAST_NAME     ,
UNREG_DOB           ,
UNREG_ID_NUMBER     ,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_RECIEVER_KYC C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.IT_OMNY_RECIEVER_KYC) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL