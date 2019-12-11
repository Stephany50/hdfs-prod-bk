CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_OMNY_RECIEVER_KYC (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
MSISDN               VARCHAR(15),
UNREG_USER_ID        VARCHAR(30),
UNREG_FIRST_NAME     VARCHAR(80),
UNREG_LAST_NAME      VARCHAR(80),
UNREG_DOB            VARCHAR(20),
UNREG_ID_NUMBER      VARCHAR(20)
)
COMMENT 'OM RECIEVER_KYC'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/RECIEVER_KYC'
TBLPROPERTIES ('serialization.null.format'='');