INSERT INTO CTI.DT_RESOURCE_GROUP_COMBINATION
SELECT
GROUP_COMBINATION_KEY,
GROUP_KEY,
TENANT_KEY,
CREATE_AUDIT_KEY,
UPDATE_AUDIT_KEY,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
   	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
   	CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_RESOURCE_GROUP_COMBINATION C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.DT_RESOURCE_GROUP_COMBINATION)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;