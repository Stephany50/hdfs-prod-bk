INSERT INTO CTI.FT_RESOURCE_GROUP_FACT
SELECT
    RESOURCE_GROUP_FACT_KEY,
    START_DATE_TIME_KEY,
    END_DATE_TIME_KEY,
    TENANT_KEY,
    RESOURCE_KEY,
    GROUP_KEY,
    RESOURCE_CFG_TYPE_ID,
    CREATE_AUDIT_KEY,
    UPDATE_AUDIT_KEY,
    START_TS,
    END_TS,
    IDB_ID,
    ACTIVE_FLAG,
    PURGE_FLAG,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_RESOURCE_GROUP_FACT C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.FT_RESOURCE_GROUP_FACT)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;