INSERT INTO CDR.SPARK_IT_LL_CHARGE_REPORT PARTITION (EVENT_DATE)
SELECT
    Account_Code,
    Customer_Name ,
    Bill_Month ,
    Capacity ,
    Account_Status ,
    Username ,
    Link ,
    Bill_Amount ,
    ORIGINAL_FILE_NAME,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) EVENT_DATE
FROM CDR.SPARK_TT_LL_CHARGE_REPORT C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_RAPPORT_DAILY)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL