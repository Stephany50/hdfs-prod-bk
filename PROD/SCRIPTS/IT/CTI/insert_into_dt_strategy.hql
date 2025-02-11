INSERT INTO CTI.DT_STRATEGY
SELECT
    STRATEGY_KEY ,
    TENANT_KEY ,
    CREATE_AUDIT_KEY ,
    UPDATE_AUDIT_KEY ,
    STRATEGY_TYPE ,
    STRATEGY_TYPE_CODE ,
    STRATEGY_NAME ,
    PURGE_FLAG ,
    ORIGINAL_FILE_NAME ,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT ,
    CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_DT_STRATEGY C
LEFT JOIN (
    SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.DT_STRATEGY WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE
)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
