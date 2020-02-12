INSERT INTO CDR.SPARK_IT_DEPOCREDI
SELECT

    Customer_id   ,
    Subs_id   ,
    ACCNBR   ,
    Adjust_balance_amount   ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Adjust_balance_date, 'dd/MM/yy hh:mm:ss')) Adjust_balance_date   ,
    ORIGINAL_FILE_NAME   ,
    ORIGINAL_FILE_SIZE   ,
    ORIGINAL_FILE_LINE_COUNT   ,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE

FROM CDR.TT_DEPOCREDI  C
         LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_DEPOCREDI) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL