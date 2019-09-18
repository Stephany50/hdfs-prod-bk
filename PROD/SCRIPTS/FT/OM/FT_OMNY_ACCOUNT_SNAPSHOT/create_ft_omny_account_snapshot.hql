CREATE TABLE MON.FT_OMNY_ACCOUNT_SNAPSHOT
(

  USER_ID               VARCHAR(100),
  ACCOUNT_NUMBER        VARCHAR(100),
  MSISDN                VARCHAR(50),
  USER_FIRST_NAME       VARCHAR(200),
  USER_LAST_NAME        VARCHAR(200),
  REGISTERED_ON         TIMESTAMP,
  CITY                  VARCHAR(100),
  ADDRESS               VARCHAR(200),
  SEX                   VARCHAR(50),
  ID_NUMBER             VARCHAR(50),
  ACCOUNT_STATUS        VARCHAR(50),
  CREATED_BY            VARCHAR(50),
  FIRST_TRANSACTION_ON  TIMESTAMP,
  USER_DOMAIN           VARCHAR(100),
  USER_CATEGORY         VARCHAR(100),
  GEOGRAPHICAL_DOMAIN   VARCHAR(100),
  USER_TYPE             VARCHAR(50),
  DELETED_ON            TIMESTAMP,
  DEACTIVATION_BY       VARCHAR(100),
  BILL_COMPANY_CODE     VARCHAR(100),
  COMPANY_TYPE          VARCHAR(50),
  NOTIFICATION_TYPE     VARCHAR(50),
  PROFILE_ID            VARCHAR(50),
  PARENT_USER_ID        VARCHAR(50),
  CREATION_DATE         TIMESTAMP,
  MODIFIED_BY           VARCHAR(50),
  MODIFIED_ON           TIMESTAMP,
  BIRTH_DATE            TIMESTAMP,
  ACCOUNT_BALANCE       DECIMAL(17,2),
  CREATED_BY_MSISDN     VARCHAR(50),
  INSERT_DATE           TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
 TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")