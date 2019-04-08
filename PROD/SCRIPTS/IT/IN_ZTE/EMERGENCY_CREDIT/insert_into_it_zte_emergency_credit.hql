INSERT INTO CDR.IT_ZTE_EMERGENCY_CREDIT PARTITION (TRANSACTION_DATE)
SELECT
   MSISDN,
   DATE_FORMAT(TRANSACTION_DATE,'HHmmss') TRANSACTION_TIME,
   AMOUNT	,
   TRANSACTION_TYPE,
   FEE,
   CONTACT_CHANNEL,
   ORIGINAL_FILE_NAME,
   ORIGINAL_FILE_SIZE,
   ORIGINAL_FILE_LINE_COUNT,
   TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
   CURRENT_TIMESTAMP() INSERT_DATE,
   TO_DATE(TRANSACTION_DATE) TRANSACTION_DATE
FROM CDR.TT_ZTE_EMERGENCY_CREDIT C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM') 
    AND B.FILE_TYPE = 'ZTE_EMERGENCY_CREDIT_CDR' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
);
