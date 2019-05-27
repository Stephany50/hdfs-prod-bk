INSERT INTO CTI.DT_USER_DATA_CUST_DIM_5
SELECT
    ID,
    TENANT_KEY,
    CREATE_AUDIT_KEY,
    DIM_ATTRIBUTE_1,
    DIM_ATTRIBUTE_2,
    DIM_ATTRIBUTE_3,
    DIM_ATTRIBUTE_4 ,
    DIM_ATTRIBUTE_5 ,
    ORIGINAL_FILE_NAME ,
    ORIGINAL_FILE_SIZE ,
    ORIGINAL_FILE_LINE_COUNT,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_DT_USER_DATA_CUST_DIM_5 C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.DT_USER_DATA_CUST_DIM_5)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;
