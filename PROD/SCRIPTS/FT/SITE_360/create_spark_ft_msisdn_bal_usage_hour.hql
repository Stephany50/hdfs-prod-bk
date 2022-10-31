CREATE TABLE MON.spark_ft_msisdn_bal_usage_hour
(
    msisdn VARCHAR(100)
    , hour_period varchar(2)
    , bal_id bigint
    , ACCT_RES_ID bigint
    , ACCT_RES_NAME varchar(50)
    , ACCT_RES_RATING_TYPE varchar(50)
    , ACCT_RES_RATING_UNIT varchar(50)
    , SERVICE varchar(50)
    , used_volume bigint
    , insert_date timestamp
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

CREATE TABLE MON.spark_ft_msisdn_bal_usage_hour_new
(
    msisdn VARCHAR(100)
    , hour_period varchar(2)
    , bal_id bigint
    , ACCT_RES_ID bigint
    , ACCT_RES_NAME varchar(50)
    , ACCT_RES_RATING_TYPE varchar(50)
    , ACCT_RES_RATING_UNIT varchar(50)
    , SERVICE varchar(50)
    , used_volume bigint
    , insert_date timestamp
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

