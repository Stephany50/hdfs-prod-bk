INSERT INTO CDR.SPARK_IT_ZTE_EMERGENCY_DATA PARTITION (TRANSACTION_DATE,FILE_DATE)
SELECT
   MSISDN,
   DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(transaction_date,'yyyy-MM-dd HH:mm:ss') ),'HHmmss') TRANSACTION_TIME,
   AMOUNT	,
   TRANSACTION_TYPE,
   FEE,
   CONTACT_CHANNEL,
   ORIGINAL_FILE_NAME,
   ORIGINAL_FILE_SIZE,
   ORIGINAL_FILE_LINE_COUNT,
   TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
   CURRENT_TIMESTAMP() INSERT_DATE,
   TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (transaction_date, 1, 10),'yyyy-MM-dd')) ) TRANSACTION_DATE,
   TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) FILE_DATE
FROM CDR.TT_ZTE_EMERGENCY_DATA C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_ZTE_EMERGENCY_DATA WHERE TRANSACTION_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
