CREATE TABLE MON.SPARK_FT_OM_APGL_TRANSACTION

(

  ACCOUNTING_TYPE               VARCHAR(100),

  ACCOUNT_NO                    VARCHAR(100),

  POSTING_DATE                  DATE,

  DOCUMENT_NO                   CHAR(1),

  DESCRIPTION                   VARCHAR(302),

  TRANSACTION_LINE_DESCRIPTION  CHAR(1),

  CURRENCY_CODE                 CHAR(1),

  AMOUNT                        INT,

  AMOUNT_LVC                    INT,

  SOUS_COMPTE                   CHAR(1),

  ORGANISATION                  CHAR(1),

  OFFRE                         CHAR(1),

  PARTNER                       CHAR(1),

  SITE                          CHAR(1),

  PROJECT                       CHAR(1),

  FLUX                          CHAR(2),

  SC8                           CHAR(1),

  SALESPERSON_CODE              CHAR(1),

  DUE_DATE                      CHAR(1),

  PAYMENT_DISCOUNT              CHAR(1),

  PAYMENT_TERMS_CODE            CHAR(1),

  EXTERNAL_DOC_NO               CHAR(1),

  INSERT_DATE                   DATE

) COMMENT 'FT_OM_APGL_TRANSACTION - FT'
  PARTITIONED BY (DOCUMENT_DATE    DATE)
  STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')