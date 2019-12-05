CREATE EXTERNAL TABLE CDR.tt_omny_services_charges_details (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  TRANSACTION_ID  VARCHAR(20),
  TRANSACTION_DATE  VARCHAR(20),
  SERVICE_CHARGE_ID  VARCHAR(10),
  TRANSACTION_AMOUNT  DECIMAL(17,2),
  PAYER_USER_ID  VARCHAR(20),
  PAYER_CATEGORY_CODE  VARCHAR(20),
  PAYEE_USER_ID  VARCHAR(20),
  PAYEE_CATEGORY_CODE  VARCHAR(10),
  SERVICE_CHARGE_AMOUNT  DECIMAL(17,2),
  SERVICE_TYPE  VARCHAR(10),
  TRANSFER_STATUS  VARCHAR(3),
  TRANSFER_SUBTYPE  VARCHAR(10),
  PAYER_DOMAIN_CODE  VARCHAR(10),
  PAYER_GRADE_NAME  VARCHAR(40),
  PAYER_MOBILE_GROUP_ROLE  VARCHAR(38),
  PAYER_GROUP_ROLE  VARCHAR(35),
  PAYER_MSISDN_ACC  VARCHAR(15),
  PARENT_PAYER_USER_ID  VARCHAR(20),
  PARENT_PAYER_USER_MSISDN  VARCHAR(15),
  OWNER_PAYER_USER_ID  VARCHAR(20),
  OWNER_PAYER_USER_MSISDN  VARCHAR(15),
  PAYER_WALLET_NUMBER  VARCHAR(25),
  PAYEE_DOMAIN_CODE  VARCHAR(10),
  PAYEE_GRADE_NAME  VARCHAR(40),
  PAYEE_MOBILE_GROUP_ROLE  VARCHAR(38),
  PAYEE_GROUP_ROLE  VARCHAR(35),
  PAYEE_MSISDN_ACC  VARCHAR(15),
  PARENT_PAYEE_USER_ID  VARCHAR(20),
  PARENT_PAYEE_USER_MSISDN  VARCHAR(15),
  OWNER_PAYEE_USER_ID  VARCHAR(20),
  OWNER_PAYEE_USER_MSISDN  VARCHAR(15),
  PAYEE_WALLET_NUMBER  VARCHAR(25)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/SERVICES_CHARGES_DETAILS'
TBLPROPERTIES ('serialization.null.format'='')
;