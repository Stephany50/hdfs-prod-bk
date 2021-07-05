INSERT INTO CDR.SPARK_IT_ZTE_LOAN_CDR PARTITION ( ORIGINAL_FILE_DATE )
SELECT  MSISDN,
    PRICE_PLAN_CODE ,
	AMOUNT,
	TRANSACTION_TYPE ,
	CONTACT_CHANNEL ,
	TRANSACTION_DATE,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd') ) ) ORIGINAL_FILE_DATE
FROM CDR.TT_ZTE_LOAN_CDR C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_ZTE_LOAN_CDR WHERE TRANSACTION_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL