CREATE TABLE MON.spark_ft_msisdn_subs_bal
(
    msisdn varchar(100),
    bdle_name varchar(1000),
    TRANSACTION_TIME varchar(6),
    bal_id bigint,
    ACCT_RES_RATING_UNIT varchar(20),
    BEN_ACCT_ADD_VAL decimal(20, 2),
    revenu_for_bal decimal(17, 2),
    validite bigint,
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')