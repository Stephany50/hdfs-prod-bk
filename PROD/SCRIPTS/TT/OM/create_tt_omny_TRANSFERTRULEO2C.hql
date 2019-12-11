CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_OMNY_TRANSFERTRULEO2C (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
TRANSFER_RULE_ID         VARCHAR(20),
PAYEE_DOMAIN_CODE        VARCHAR(20),
PAYEE_CATEGORY_CODE      VARCHAR(20),
APPROVAL_LIMIT_1         VARCHAR(20)
)
COMMENT 'OM TRANSFERTRULEO2C'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/TRANSFERTRULEO2C'
TBLPROPERTIES ('serialization.null.format'='');
