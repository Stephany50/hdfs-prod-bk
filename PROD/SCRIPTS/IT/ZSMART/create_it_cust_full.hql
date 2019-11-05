CREATE  TABLE CDR.IT_CUST_FULL
(

    Birthday  VARCHAR(200) ,
    CustId  VARCHAR(200) ,
    CUSTOMER_PARENT_ID  VARCHAR(200) ,
    GuID  VARCHAR(200) ,
    AcctGuid  VARCHAR(200) ,
    LastName  VARCHAR(200) ,
    FirstName  VARCHAR(200) ,
    Title  VARCHAR(200) ,
    CustName  VARCHAR(200) ,
    Email  VARCHAR(200) ,
    city  VARCHAR(200) ,
    DEALER_ID  VARCHAR(200) ,
    Bill_Cycle_ID  VARCHAR(200) ,
    CustSeg  VARCHAR(200) ,
    total_bill_amount  VARCHAR(200) ,
    CreatedDate  timestamp ,
    ModifiedDate  timestamp ,
    CUID  VARCHAR(200) ,
    RUE  VARCHAR(200) ,
    NUMERO_RUE  VARCHAR(200) ,
    QUARTIER  VARCHAR(200) ,
    postCode  VARCHAR(200) ,
    Address  VARCHAR(200) ,
    Address2  VARCHAR(200) ,
    Address3  VARCHAR(200) ,
    ACCNBR  VARCHAR(200) ,
    faxNumber  VARCHAR(200) ,
    Mobilephone  VARCHAR(200) ,
    CNPSNumber  VARCHAR(200) ,
    CertNbr  VARCHAR(200) ,
    BusinessRegister VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE VARCHAR(200),
    ORIGINAL_FILE_LINE_COUNT VARCHAR(200),
    INSERT_DATE timestamp
)
PARTITIONED BY (original_file_date DATE)
CLUSTERED BY(CustId) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");