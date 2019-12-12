
CREATE EXTERNAL TABLE CDR.tt_ca_transaction (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  PAYMENT_ID  VARCHAR(200),
  PAYMENT_REFERENCE  VARCHAR(200),
  PAYMENT_DATE  VARCHAR(200),
  CUID  VARCHAR(200),
  SHOPCODE  VARCHAR(200),
  BILL_ID  VARCHAR(200),
  INVOICE_NUMBER  VARCHAR(200),
  BILL_AMOUNT  VARCHAR(200),
  BILL_DATE  VARCHAR(200),
  GUID  VARCHAR(200),
  CUSTNAME  VARCHAR(200),
  GL_CODE  VARCHAR(200),
  PAYMENTMETHOD  VARCHAR(200),
  AMOUNT  VARCHAR(200),
  CHECKNUMBER  VARCHAR(200),
  CHECKBANK  VARCHAR(200),
  TRANSFER_NUMBER  VARCHAR(200),
  CREDIT_NOTE_NUMBER  VARCHAR(200),
  CREDIT_CARD_NUMBER  VARCHAR(200),
  CREDIT_CARD_END_DATE  VARCHAR(200)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZSMART/CA_TRANSACTION_'
TBLPROPERTIES ('serialization.null.format'='')
;