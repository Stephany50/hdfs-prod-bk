
CREATE EXTERNAL TABLE CDR.tt_zte_voice_sms (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  EVENT_INST_ID  BIGINT,
  RE_ID  INT,
  BILLING_NBR  VARCHAR(30),
  BILLING_IMSI  VARCHAR(30),
  CALLING_NBR  VARCHAR(30),
  CALLED_NBR  VARCHAR(30),
  THIRD_PART_NBR  VARCHAR(30),
  START_TIME  TIMESTAMP,
  DURATION  INT,
  LAC_A  VARCHAR(30),
  CELL_A  VARCHAR(30),
  LAC_B  VARCHAR(30),
  CELL_B  VARCHAR(30),
  CALLING_IMEI  VARCHAR(30),
  CALLED_IMEI  VARCHAR(30),
  PRICE_ID1  INT,
  PRICE_ID2  INT,
  PRICE_ID3  INT,
  PRICE_ID4  INT,
  PRICE_PLAN_ID1  INT,
  PRICE_PLAN_ID2  INT,
  PRICE_PLAN_ID3  INT,
  PRICE_PLAN_ID4  INT,
  ACCT_RES_ID1  INT,
  ACCT_RES_ID2  INT,
  ACCT_RES_ID3  INT,
  ACCT_RES_ID4  INT,
  CHARGE1  BIGINT,
  CHARGE2  BIGINT,
  CHARGE3  BIGINT,
  CHARGE4  BIGINT,
  BAL_ID1  BIGINT,
  BAL_ID2  BIGINT,
  BAL_ID3  BIGINT,
  BAL_ID4  BIGINT,
  ACCT_ITEM_TYPE_ID1  INT,
  ACCT_ITEM_TYPE_ID2  INT,
  ACCT_ITEM_TYPE_ID3  INT,
  ACCT_ITEM_TYPE_ID4  INT,
  PREPAY_FLAG  TINYINT,
  PRE_BALANCE1  BIGINT,
  BALANCE1  BIGINT,
  PRE_BALANCE2  BIGINT,
  BALANCE2  BIGINT,
  PRE_BALANCE3  BIGINT,
  BALANCE3  BIGINT,
  PRE_BALANCE4  BIGINT,
  BALANCE4  BIGINT,
  INTERNATIONAL_ROAMING_FLAG  TINYINT,
  CALL_TYPE  TINYINT,
  BYTE_UP  BIGINT,
  BYTE_DOWN  BIGINT,
  BYTES  BIGINT,
  PRICE_PLAN_CODE  VARCHAR(20),
  SESSION_ID  VARCHAR(70),
  RESULT_CODE  VARCHAR(25),
  PROD_SPEC_STD_CODE  VARCHAR(25),
  YZDISCOUNT  INT,
  BYZCHARGE1  BIGINT,
  BYZCHARGE2  BIGINT,
  BYZCHARGE3  BIGINT,
  BYZCHARGE4  BIGINT,
  ONNET_OFFNET  INT,
  PROVIDER_ID  INT,
  PROD_SPEC_ID  INT,
  TERMINATION_CAUSE  INT,
  B_PROD_SPEC_ID  VARCHAR(300),
  B_PRICE_PLAN_CODE  VARCHAR(50),
  CALLSPETYPE  INT,
  CHARGINGRATIO  INT,
  DUMMY  VARCHAR(20)

)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/PROD/TT/GOOGLE_RCS/CLIENT'
TBLPROPERTIES ('serialization.null.format'='')
;