INSERT INTO CDR.IT_BILL
SELECT

    BILL_ID  ,
    Invoice_number  ,
    CUST_ID  ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Bill_date, 'dd/MM/yy hh:mm:ss'))  Bill_date ,
    bill_amount  ,
    remaining_amount  ,
    ORIGINAL_FILE_NAME  ,
    ORIGINAL_FILE_SIZE ,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE

FROM CDR.TT_BILL  C
         LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.IT_BILL) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;