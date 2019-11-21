CREATE EXTERNAL TABLE CDR.TT_CA_TRANSACTION
(
Payment_ID VARCHAR(200) ,
Payment_reference VARCHAR(200) ,
Payment_date VARCHAR(200) ,
CUID VARCHAR(200) ,
ShopCode VARCHAR(200) ,
Bill_ID VARCHAR(200) ,
Invoice_Number VARCHAR(200) ,
Bill_Amount VARCHAR(200) ,
BILL_Date VARCHAR(200) ,
Guid VARCHAR(200) ,
Custname VARCHAR(200) ,
GL_Code VARCHAR(200) ,
PaymentMethod VARCHAR(200) ,
Amount VARCHAR(200) ,
CheckNumber VARCHAR(200) ,
CheckBank VARCHAR(200) ,
Transfer_number VARCHAR(200) ,
Credit_Note_Number VARCHAR(200) ,
Credit_card_number VARCHAR(200) ,
Credit_card_end_date VARCHAR(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE VARCHAR(200),
ORIGINAL_FILE_LINE_COUNT VARCHAR(200)
)
COMMENT 'CA_TRANSACTION external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZSMART/CA_TRANSACTION_'
TBLPROPERTIES ('serialization.null.format'='');