INSERT INTO  CDR.IT_CUST_FULL
SELECT

    Birthday    ,
    CustId    ,
    CUSTOMER_PARENT_ID    ,
    GuID    ,
    AcctGuid    ,
    LastName    ,
    FirstName    ,
    Title    ,
    CustName    ,
    Email    ,
    city    ,
    DEALER_ID    ,
    Bill_Cycle_ID    ,
    CustSeg    ,
    total_bill_amount    ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(CreatedDate, 'dd/MM/yy hh:mm:ss')) CreatedDate,
    FROM_UNIXTIME(UNIX_TIMESTAMP(ModifiedDate, 'dd/MM/yy hh:mm:ss')) ModifiedDate   ,
    CUID    ,
    RUE    ,
    NUMERO_RUE    ,
    QUARTIER    ,
    postCode    ,
    Address    ,
    Address2    ,
    Address3    ,
    ACCNBR    ,
    faxNumber    ,
    Mobilephone    ,
    CNPSNumber    ,
    CertNbr    ,
    BusinessRegister  ,
    ORIGINAL_FILE_NAME  ,
    ORIGINAL_FILE_SIZE  ,
    ORIGINAL_FILE_LINE_COUNT  ,
   CURRENT_TIMESTAMP() INSERT_DATE,
   TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE

FROM CDR.TT_CUST_FULL  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.IT_CUST_FULL) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;