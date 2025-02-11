CREATE EXTERNAL TABLE CDR.tt_cust_full (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  BIRTHDAY  VARCHAR(200),
  CUSTID  VARCHAR(200),
  CUSTOMER_PARENT_ID  VARCHAR(200),
  GUID  VARCHAR(200),
  ACCTGUID  VARCHAR(200),
  LASTNAME  VARCHAR(200),
  FIRSTNAME  VARCHAR(200),
  TITLE  VARCHAR(200),
  CUSTNAME  VARCHAR(200),
  EMAIL  VARCHAR(200),
  CITY  VARCHAR(200),
  DEALER_ID  VARCHAR(200),
  BILL_CYCLE_ID  VARCHAR(200),
  CUSTSEG  VARCHAR(200),
  TOTAL_BILL_AMOUNT  VARCHAR(200),
  CREATEDDATE  VARCHAR(200),
  MODIFIEDDATE  VARCHAR(200),
  CUID  VARCHAR(200),
  RUE  VARCHAR(200),
  NUMERO_RUE  VARCHAR(200),
  QUARTIER  VARCHAR(200),
  POSTCODE  VARCHAR(200),
  ADDRESS  VARCHAR(200),
  ADDRESS2  VARCHAR(200),
  ADDRESS3  VARCHAR(200),
  ACCNBR  VARCHAR(200),
  FAXNUMBER  VARCHAR(200),
  MOBILEPHONE  VARCHAR(200),
  CNPSNUMBER  VARCHAR(200),
  CERTNBR  VARCHAR(200),
  BUSINESSREGISTER  VARCHAR(200))
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZSMART/CUST_FULL_'
TBLPROPERTIES ('serialization.null.format'='')
;
