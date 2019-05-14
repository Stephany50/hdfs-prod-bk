INSERT INTO CTI.DT_RESOURCE_STATE
SELECT
    RESOURCE_STATE_KEY ,
    STATE_TYPE   ,
    STATE_TYPE_CODE ,
    STATE_NAME  ,
    STATE_NAME_CODE ,
    CREATE_AUDIT_KEY ,
    UPDATE_AUDIT_KEY  ,
    ORIGINAL_FILE_NAME ,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT ,
    CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_DT_RESOURCE_STATE C
LEFT JOIN (
    SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.DT_RESOURCE_STATE WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE
)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
