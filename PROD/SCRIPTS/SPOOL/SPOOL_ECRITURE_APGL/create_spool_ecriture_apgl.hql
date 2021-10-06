CREATE TABLE SPOOL.SPOOL_ECRITURE_APGL
(
    ACCOUNTING_TYPE varchar(100),
    ACCOUNT_NO varchar(100),
    DOCUMENT_DATE date,
    POSTING_DATE date,
    DOCUMENT_NO varchar(1),
    DESCRIPTION varchar(300),
    TRANSACTION_LINE_DESCRIPTION varchar(1),
    CURRENCY_CODE varchar(1),
    AMOUNT decimal(17,2),
    AMOUNT_LVC decimal(17,2),
    SOUS_COMPTE varchar(100),
    ORGANISATION varchar(100),
    OFFRE varchar(100),
    PARTNER varchar(100),
    SITE varchar(100),
    PROJECT varchar(100),
    FLUX varchar(100),
    SC8 varchar(1),
    SALESPERSON_CODE varchar(1),
    DUE_DATE varchar(1),
    PAYMENT_DISCOUNT varchar(1),
    PAYMENT_TERMS_CODE varchar(1),
    EXTERNAL_DOC_NO varchar(1),
    INSERT_DATE timestamp
)
PARTITIONED BY (EVENT_DATE date)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')