
CREATE TABLE AGG.SPARK_FT_A_SUBSCRIPTION (
TRANSACTION_TIME    VARCHAR (6),
CONTRACT_TYPE   VARCHAR (255),
OPERATOR_CODE       VARCHAR (50),
MAIN_USAGE_SERVICE_CODE VARCHAR(255),
COMMERCIAL_OFFER    VARCHAR (255),
PREVIOUS_COMMERCIAL_OFFER   VARCHAR (255),
SUBS_SERVICE VARCHAR(255),
SUBS_BENEFIT_NAME VARCHAR(255),
SUBS_CHANNEL VARCHAR(255),
SUBS_RELATED_SERVICE VARCHAR(255),
SUBS_TOTAL_COUNT INT,
SUBS_AMOUNT DECIMAL(20,2),
SOURCE_PLATFORM VARCHAR(25),
SOURCE_DATA VARCHAR(40),
INSERT_DATE TIMESTAMP,
SERVICE_CODE        VARCHAR (50),
MSISDN_COUNT INT,
SUBS_EVENT_RATED_COUNT INT,
SUBS_PRICE_UNIT DECIMAL(20,2)
)
COMMENT 'ZTE Subscriptions - FT_A'
PARTITIONED BY (TRANSACTION_DATE    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


