CREATE TABLE AGG.SPARK_FT_A_RAF_SUBSCRIPTION_DD (
TRANSACTION_DATE      DATE,
COMMERCIAL_OFFER      VARCHAR(50),
SUBSCRIPTION_CHANNEL  VARCHAR(50),
SERVICE_CODE          VARCHAR(50),
SUBSCRIPTION_SERVICE_DETAILS   VARCHAR(50),
MT_DEBIT             FLOAT,
NBR_SOUSCRIPTION  INT,
MT_VOICE_ONNET    FLOAT,
MT_VOICE_OFNET    FLOAT,
MT_VOICE_INT      FLOAT,
MT_VOICE_ROAM     FLOAT,
MT_SMS_ROAM       FLOAT,
MT_DATA           FLOAT,
MT_SVA            FLOAT,
MT_SMS_ONNET      FLOAT,
MT_SMS_OFNET      FLOAT,
MT_SMS_INTER      FLOAT,
MT_CREDIT         FLOAT,
INSERT_DATE TIMESTAMP
) COMMENT 'SPARK_FT_A_RAF_SUBSCRIPTION_DD'
PARTITIONED BY (EVENT_DATE DATE)
 STORED AS PARQUET
 TBLPROPERTIES ("parquet.compress"="SNAPPY")