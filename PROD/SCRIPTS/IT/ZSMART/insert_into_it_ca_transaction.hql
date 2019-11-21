INSERT INTO CDR.IT_CA_TRANSACTION
SELECT

Payment_ID  ,
Payment_reference  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(Payment_date, 'dd/MM/yy hh:mm:ss')) Payment_date  ,
CUID  ,
ShopCode  ,
Bill_ID  ,
Invoice_Number  ,
Bill_Amount  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(BILL_Date, 'dd/MM/yy hh:mm:ss')) BILL_Date  ,
Guid  ,
Custname  ,
GL_Code  ,
PaymentMethod  ,
Amount  ,
CheckNumber  ,
CheckBank  ,
Transfer_number  ,
Credit_Note_Number  ,
Credit_card_number  ,
FROM_UNIXTIME(UNIX_TIMESTAMP(Credit_card_end_date, 'dd/MM/yy hh:mm:ss')) Credit_card_end_date ,
ORIGINAL_FILE_NAME  ,
ORIGINAL_FILE_SIZE  ,
ORIGINAL_FILE_LINE_COUNT  ,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE

FROM CDR.TT_CA_TRANSACTION  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.IT_CA_TRANSACTION) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;