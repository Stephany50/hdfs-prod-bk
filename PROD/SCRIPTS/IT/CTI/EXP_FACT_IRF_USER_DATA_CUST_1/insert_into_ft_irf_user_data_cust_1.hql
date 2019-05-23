INSERT INTO CTI.FT_IRF_USER_DATA_CUST_1
SELECT
   INTERACTION_RESOURCE_ID,
   START_DATE_TIME_KEY,
   TENANT_KEY,
   CUSTOM_DATA_1,
   CUSTOM_DATA_2,
   CUSTOM_DATA_3,
   CUSTOM_DATA_4,
   CUSTOM_DATA_5,
   CUSTOM_DATA_6,
   CUSTOM_DATA_7,
   CUSTOM_DATA_8,
   CUSTOM_DATA_9,
   CUSTOM_DATA_10,
   CUSTOM_DATA_11,
   CUSTOM_DATA_12,
   CUSTOM_DATA_13,
   CUSTOM_DATA_14,
   CUSTOM_DATA_15,
   CUSTOM_DATA_16,
   ORIGINAL_FILE_NAME,
   ORIGINAL_FILE_SIZE,
   ORIGINAL_FILE_LINE_COUNT,
   TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
   CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_IRF_USER_DATA_CUST_1 C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.FT_IRF_USER_DATA_CUST_1)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;