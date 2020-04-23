CREATE TABLE MON.SEVERAL_SUBSCRIPTIONS(
TRANSACTION_DATE    DATE,
SUBSCRIPTION_SERVICE_DETAILS   varchar(50),
BENEFIT_BALANCE_LIST         varchar(1000) ,
BENEFIT_UNIT_LIST        varchar(255) ,
OCCURENCE        int,
BORNE_SUP   double,
DISPERSION  double,
INSERT_DATE  TIMESTAMP
)
COMMENT ' FT_SEVERAL_SUBSCRIPTIONS '
 PARTITIONED BY (EVENT_DATE DATE)
 STORED AS PARQUET
 TBLPROPERTIES ("parquet.compress"="SNAPPY")