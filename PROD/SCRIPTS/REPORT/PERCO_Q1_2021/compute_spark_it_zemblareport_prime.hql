INSERT INTO CDR.SPARK_IT_ZEMBLAREPORT_PRIME
SELECT
subsdate,
msisdn,
segment,
subscription,
subs_date,
original_file_name,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) EVENT_DATE
FROM CDR.SPARK_TT_ZEMBLAREPORT_PRIME C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_ZEMBLAREPORT_PRIME WHERE EVENT_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL
