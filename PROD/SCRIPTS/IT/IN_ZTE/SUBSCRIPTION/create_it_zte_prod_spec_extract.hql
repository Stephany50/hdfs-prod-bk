CREATE TABLE CDR.IT_ZTE_SUBSCRIPTION (
  ACC_NBR VARCHAR(16),
  CHANNEL_ID	INT,
  SUBS_EVENT_ID	INT,
  NQ_CREATEDDATE	TIMESTAMP,
  OLD_SUBS_STATE	VARCHAR(3),
  NEW_SUBS_STATE	VARCHAR(3),
  EVENT_COST	INT,
  BENEFIT_NAME	VARCHAR(50),
  BENEFIT_BAL_LIST	VARCHAR(2000),
  OLD_PROD_SPEC_CODE	VARCHAR(10),
  PROD_SPEC_CODE	VARCHAR(10),
  OLD_PRICE_PLAN_CODE	VARCHAR(10),
  PRICE_PLAN_CODE	VARCHAR(50),
  OLD_RELATED_PROD_CODE	VARCHAR(10),
  RELATED_PROD_CODE	VARCHAR(10),
  ACTIVE_DATE	TIMESTAMP,
  EXPIRED_DATE	TIMESTAMP,
  PROVIDER_ID	INT,
  PREPAY_FLAG	INT,
  ORIGINAL_FILE_NAME VARCHAR(100),
  ORIGINAL_FILE_DATE DATE,
  INSERT_DATE TIMESTAMP
)
PARTITIONED BY (CREATEDDATE DATE)
CLUSTERED BY(PROD_SPEC_CODE,PRICE_PLAN_CODE,SUBS_EVENT_ID,CHANNEL_ID,OLD_PROD_SPEC_CODE,OLD_PRICE_PLAN_CODE) INTO 5 BUCKETS
STORED AS ORC 
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
