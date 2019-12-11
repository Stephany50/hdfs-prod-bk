CREATE TABLE CDR.IT_CA_TRANSACTION
(
    Payment_ID VARCHAR(200),
    Payment_reference VARCHAR(200),
    Payment_date timestamp ,
    CUID VARCHAR(200),
    ShopCode VARCHAR(200),
    Bill_ID VARCHAR(200),
    Invoice_Number VARCHAR(200),
    Bill_Amount VARCHAR(200),
    BILL_Date timestamp ,
    Guid VARCHAR(200),
    Custname VARCHAR(200),
    GL_Code VARCHAR(200),
    PaymentMethod VARCHAR(200),
    Amount VARCHAR(200),
    CheckNumber VARCHAR(200),
    CheckBank VARCHAR(200),
    Transfer_number VARCHAR(200),
    Credit_Note_Number VARCHAR(200),
    Credit_card_number VARCHAR(200),
    Credit_card_end_date timestamp,
    ORIGINAL_FILE_NAME string,
    ORIGINAL_FILE_SIZE string,
    ORIGINAL_FILE_LINE_COUNT string,
    INSERT_DATE timestamp

)
    PARTITIONED BY (original_file_date DATE)
    CLUSTERED BY(Payment_ID) INTO 8 BUCKETS
    STORED AS ORC
    TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");