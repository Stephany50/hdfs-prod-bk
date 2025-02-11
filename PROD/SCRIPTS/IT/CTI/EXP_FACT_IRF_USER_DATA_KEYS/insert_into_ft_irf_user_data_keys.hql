INSERT INTO CTI.FT_IRF_USER_DATA_KEYS
SELECT
    INTERACTION_RESOURCE_ID,
    START_DATE_TIME_KEY,
    TENANT_KEY,
    INTERACTION_DESCRIPTOR_KEY,
    CUSTOM_KEY_1,
    CUSTOM_KEY_2,
    CUSTOM_KEY_3,
    CUSTOM_KEY_4,
    CUSTOM_KEY_5,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_IRF_USER_DATA_KEYS C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.FT_IRF_USER_DATA_KEYS)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;