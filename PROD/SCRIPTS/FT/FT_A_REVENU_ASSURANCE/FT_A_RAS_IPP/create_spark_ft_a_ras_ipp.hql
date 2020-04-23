CREATE TABLE AGG.SPARK_FT_A_RAS_IPP(
TRANSACTION_DATE  DATE,
SUBSCRIPTION_SERVICE_DETAILS    varchar(50),
NBRE_OCCURENCE    INT,
INSERT_DATE  TIMESTAMP
)
COMMENT ' SPARK_FT_A_RAS_IPP '
 PARTITIONED BY (EVENT_DATE DATE)
 STORED AS PARQUET
 TBLPROPERTIES ("parquet.compress"="SNAPPY")