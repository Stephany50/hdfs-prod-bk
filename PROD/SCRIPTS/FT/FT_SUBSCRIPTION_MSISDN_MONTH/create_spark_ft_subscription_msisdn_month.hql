CREATE TABLE MON.SPARK_FT_SUBSCRIPTION_MSISDN_MONTH
(
  MSISDN                  VARCHAR(55),
  CUSTOMER_PROFILE        VARCHAR(253),
  CONTRACT_TYPE           VARCHAR(253),
  TOTAL_SUBS_COUNT        DOUBLE,
  TOTAL_SUBS_AMOUNT       DOUBLE,
  VOICE_BUNDLE_COUNT      DOUBLE,
  VOICE_BUNDLE_MINUTES    DOUBLE,
  VOICE_BUNDLE_AMOUNT     DOUBLE,
  SMS_BUNDLE_COUNT        DOUBLE,
  SMS_BUNDLE_VOLUME       DOUBLE,
  SMS_BUNDLE_AMOUNT       DOUBLE,
  DATA_BUNDLE_COUNT       DOUBLE,
  DATA_BUNDLE_BYTES       DOUBLE,
  DATA_BUNDLE_AMOUNT      DOUBLE,
  VOI_DAT_SMS_BUN_COUNT   DOUBLE,
  VOI_DAT_SMS_BUN_AMOUNT  DOUBLE,
  VOI_SMS_BUN_COUNT       DOUBLE,
  VOI_SMS_BUN_AMOUNT      DOUBLE,
  VOI_DAT_BUN_COUNT       DOUBLE,
  VOI_DAT_BUN_AMOUNT      DOUBLE,
  DAT_SMS_BUN_COUNT       DOUBLE,
  DAT_SMS_BUN_AMOUNT      DOUBLE,
  OTHER_SUBS_COUNT        DOUBLE,
  OTHER_SUBS_AMOUNT       DOUBLE,
  SRC_TABLE               VARCHAR(55),
  OPERATOR_CODE           VARCHAR(55),
  INSERT_DATE             DATE,
  ACTIVE_DAYS_COUNT       DOUBLE,
  FIRST_ACTIVE_DAY        DATE,
  LAST_ACTIVE_DAY         DATE
)     COMMENT 'FT_SUBSCRIPTION_MSISDN_MONTH'
    PARTITIONED BY (EVENT_MONTH VARCHAR(25))
    STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY")