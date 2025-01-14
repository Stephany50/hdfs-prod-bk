CREATE TABLE  MON.SPARK_FT_VAS_RETAILLER_IRIS
(
START_TIME  VARCHAR(100),
TRANSACTION_ID  VARCHAR(100),
RET_MSISDN  VARCHAR(4000),
SUB_MSISDN  VARCHAR(4000),
OPTION_NUMBER   INT,
OFFER_NAME  VARCHAR(50),
OFFER_TYPE  VARCHAR(75),
SUBSCRIBER_BONUS   INT,
RECHARGE_AMOUNT   INT,
RETAILER_COMMISSION   INT,
CAMPAIGN_NAME  VARCHAR(100),
CHANNEL  VARCHAR(10),
PRETUPS_TRANSACTION_ID_OUT  VARCHAR(100),
PRETUPS_STATUSCODE  VARCHAR(100),
VAS_NODE  VARCHAR(25),
VAS_STATUS_CODE  VARCHAR(100),
VAS_TRANSACTION_ID_IN  VARCHAR(100),
SMS_STATUS_CODE  VARCHAR(100),
TRANSACTION_STATUS_OVERALL  VARCHAR(50),
INSERT_DATE  TIMESTAMP

)
PARTITIONED BY (SDATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')


