INSERT INTO CDR.IT_OMNY_CATEGORY_DETAILS PARTITION (ORIGINAL_FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
CATEGORY_CODE,
DOMAIN_CODE,
CATEGORY_NAME,
PARENT_CATEGORY_CODE,
STATUS,
CATEGORY_TYPE,
SEQUENCE_NO,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_CATEGORY_DETAILS C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.IT_OMNY_CATEGORY_DETAILS) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;