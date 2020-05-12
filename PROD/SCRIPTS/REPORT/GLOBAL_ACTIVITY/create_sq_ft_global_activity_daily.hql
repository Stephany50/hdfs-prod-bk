    CREATE TABLE IF NOT EXISTS TMP.SQ_TMP_GLOBAL_ACTIVITY_DAILY (
TRANSACTION_DATE             DATE     ,
COMMERCIAL_OFFER_CODE        VARCHAR(50) ,
TRANSACTION_TYPE             VARCHAR(50) ,
SUB_ACCOUNT                  VARCHAR(50) ,
TRANSACTION_SIGN             VARCHAR(1)  ,
SOURCE_PLATFORM              VARCHAR(50) ,
SOURCE_DATA                  VARCHAR(50) ,
SERVED_SERVICE               VARCHAR(50) ,
SERVICE_CODE                 VARCHAR(50) ,
DESTINATION                  VARCHAR(50) ,
OTHER_PARTY_ZONE             VARCHAR(50) ,
MEASUREMENT_UNIT             VARCHAR(50) ,
RATED_COUNT                  BIGINT       ,
RATED_VOLUME                 BIGINT       ,
TAXED_AMOUNT                 DOUBLE       ,
UNTAXED_AMOUNT               DOUBLE       ,
INSERT_DATE                  TIMESTAMP ,
TRAFFIC_MEAN                 VARCHAR(50) ,
OPERATOR_CODE                VARCHAR(50) 

) COMMENT 'SPARK_FT_GLOBAL_ACTIVITY_DAILY table'
 STORED AS PARQUET     TBLPROPERTIES ("parquet.compress"="SNAPPY");
