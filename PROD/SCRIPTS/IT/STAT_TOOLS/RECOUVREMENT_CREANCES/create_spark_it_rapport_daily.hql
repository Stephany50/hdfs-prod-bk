CREATE TABLE CDR.SPARK_IT_RAPPORT_DAILY (
ORIGINAL_FILE_NAME VARCHAR(150),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
CODE_COMPTABLE     INT,
MARCHE             VARCHAR(10),
NUM_RECU           VARCHAR(50),
TYPE_PAIEMENT      VARCHAR(50),
TYPE_PAIEMENT_DETAIL     VARCHAR(50),
MONTANT             FLOAT,
CODE_CLIENT         INT,
ACCOUNT_NUMBER      VARCHAR(100),
CUSTOMER_NAME       VARCHAR(255),
INVOICE_NUMBER      INT,
INVOICE_DATE        DATE,
TYPE_CLIENT         VARCHAR(100),
CUSTOMER_SEGMENT    VARCHAR(60),
CHECK_NUMBER        VARCHAR(100),
CREDIT_CARD_NUMBER   VARCHAR(100),
BANK_DEPOSIT_NUMBER   VARCHAR(100),
BANK_TRANSFER_NUMBER   VARCHAR(100),
PAYMENT_METHON_NAME     VARCHAR(100),
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE  DATE
)
PARTITIONED BY (DATE_SAISIE DATE)
STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY")