INSERT INTO CDR.IT_OMNY_ACCOUNT_SNAPSHOT PARTITION (ORIGINAL_FILE_DATE)
SELECT
    USER_NAME,
    LAST_NAME,
    FROM_UNIXTIME(UNIX_TIMESTAMP(CREATED_ON,'dd/MM/yyyy HH:mm:ss')) CREATED_ON,
    FROM_UNIXTIME(UNIX_TIMESTAMP(MODIFIED_ON,'dd/MM/yyyy HH:mm:ss')) MODIFIED_ON,
    FROM_UNIXTIME(UNIX_TIMESTAMP(BIRTH_DATE,'dd.MM.yyyy')) LAST_TRANSFER_ON,
    MSISDN,
    USER_ID,
    BALANCE,
    EXTERNAL_CODE,
    IS_ACTIVE,
    FROM_UNIXTIME(UNIX_TIMESTAMP(BIRTH_DATE,'dd.MM.yyyy')) BIRTH_DATE,
    USER_TYPE,
    USER_DOMAIN,
    USER_CATEGORY,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_OMNY_ACCOUNT_SNAPSHOT_CHANNEL C
-- LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_OMNY_ACCOUNT_SNAPSHOT WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
-- WHERE T.FILE_NAME IS NULL;
