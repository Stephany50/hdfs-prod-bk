INSERT INTO CDR.SPARK_IT_GOOGLE_RCS_PROVISION PARTITION (START_DATE,FILE_DATE)
SELECT
    MSISDN,
    TENANT,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(CREATION_DATE) START_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(REGEXP_EXTRACT(ORIGINAL_FILE_NAME, '[0-9]{14}', 0),'yyyyMMddHHmmss'))) FILE_DATE
FROM CDR.TT_GOOGLE_RCS_PROVISION  C
LEFT JOIN 
(
    SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME
    FROM CDR.SPARK_IT_GOOGLE_RCS_PROVISION 
    WHERE START_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE 
)T 
ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL

