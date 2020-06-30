INSERT INTO CDR.SPARK_IT_ZTE_BALANCE_TYPE (ORIGINAL_FILE_DATE)
SELECT
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    BALANCE_TYPE_ID,
    ACCT_ITEM_TYPE_ID,
    BALANCE_TYPE_NAME,
    BALANCE_TYPE_STD_CODE,
    CURRENT_DATE INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, 14, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ZTE_BALANCE_TYPE C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_ZTE_BALANCE_TYPE WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL