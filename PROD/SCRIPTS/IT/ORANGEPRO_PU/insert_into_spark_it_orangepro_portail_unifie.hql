INSERT INTO CDR.SPARK_IT_ORANGEPRO_PORTAIL_UNIFIE
SELECT
    id,
    user_id,
    action,
    details,
    archived,
    created_at,
    ORIGINAL_FILE_NAME, 
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(Substring (ORIGINAL_FILE_NAME, -12,8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ORANGEPRO_PORTAIL_UNIFIE C
LEFT JOIN 
(
SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME
FROM CDR.SPARK_IT_ORANGEPRO_PORTAIL_UNIFIE
) T 
ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL