INSERT INTO CDR.SPARK_IT_BILL
SELECT
    BILL_ID  ,
    INVOICE_NUMBER  ,
    CUST_ID  ,
    ACCOUNT_NUMBER,
    FROM_UNIXTIME(UNIX_TIMESTAMP(BILL_DATE, 'MM/dd/yyyy HH:mm:ss'))  BILL_DATE ,
    BILL_AMOUNT  ,
    REMAINING_AMOUNT  ,
    ORIGINAL_FILE_NAME  ,
    ORIGINAL_FILE_SIZE ,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_BILL  C
         LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.SPARK_IT_BILL) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL