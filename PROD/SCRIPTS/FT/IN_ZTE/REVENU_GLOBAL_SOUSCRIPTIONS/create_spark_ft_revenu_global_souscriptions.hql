CREATE TABLE MON.SPARK_FT_REVENU_GLOBAL_SOUSCRIPTIONS (
    SERVICE_CODE varchar(50),
    TAXED_AMOUNT varchar(50),
    UNTAXED_AMOUNT varchar(50)
    )
COMMENT 'SPARK_FT_REVENU_GLOBAL_SOUSCRIPTIONS'
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
