CREATE TABLE AGG.SPARK_FT_A_RAS_RECHARGE_2(
REFILL_DATE      DATE  ,
REFILL_TIME     varchar(25) ,
Refill_month  INT,
Refill_day    INT,
Refill_year  INT,
RECEIVER_MSISDN     varchar(25) ,
SENDER_MSISDN           varchar(25) ,
REFILL_MEAN     varchar(25) ,
REFILL_TYPE      varchar(25) ,
REFILL_AMOUNT    double,
REFILL_BONUS      double,
INSERT_DATE  TIMESTAMP
)
COMMENT ' SPARK_FT_A_RAS_RECHARGE_2 '
 PARTITIONED BY (EVENT_DATE DATE)
 STORED AS PARQUET
 TBLPROPERTIES ("parquet.compress"="SNAPPY")