INSERT INTO TABLE CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND PARTITION(EVENT_DATE)
select
    substr(msisdn, 4, 9) msisdn,
    --ORIGINAL_FILE_SIZE,
    --ORIGINAL_FILE_LINE_COUNT,
    --ORIGINAL_FILE_NAME,
    --to_date(from_unixtime(unix_timestamp(substr(ORIGINAL_FILE_NAME, 4, 8), 'yyyyMMdd'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    to_date(substr(replace(index, '"', ''), -10, 10)) EVENT_DATE
from CDR.TT_MY_ORANGE_USERS_BACKEND C ;
--LEFT JOIN
--(
--    SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME
--    FROM CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND
--    WHERE EVENT_DATE BETWEEN DATE_SUB(CURRENT_DATE, 7) AND CURRENT_DATE
--) T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
--WHERE T.FILE_NAME IS NULL



INSERT INTO TABLE CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND PARTITION(EVENT_DATE)
select
    substr(msisdn, 4, 9) msisdn,
    CURRENT_TIMESTAMP() INSERT_DATE,
    to_date(substr(replace(index, '"', ''), -10, 10)) EVENT_DATE
from CDR.TT_MY_ORANGE_USERS_BACKEND C