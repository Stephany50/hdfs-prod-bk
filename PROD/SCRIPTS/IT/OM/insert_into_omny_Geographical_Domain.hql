INSERT INTO CDR.IT_OMNY_GEOGRAPHICAL_DOMAIN PARTITION (ORIGINAL_FILE_DATE)
SELECT
ORIGINAL_FILE_NAME ,
ORIGINAL_FILE_SIZE ,
ORIGINAL_FILE_LINE_COUNT ,
GRPH_DOMAIN_CODE,
NETWORK_NAME,
PARENT_GRPH_DOMAIN_NAME,
PARENT_GRPH_DOMAIN_TYPE,
GRPH_DOMAIN_TYPE,
GRPH_DOMAIN_SHORT_NAME,
DESCRIPTION,
STATUS,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_GEOGRAPHICAL_DOMAIN C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.IT_OMNY_GEOGRAPHICAL_DOMAIN) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL