---***********************************************************---
------------ TT DATA create-------------------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_ZTE_DATA_POST (
    EVENT_INST_ID BIGINT, 
    RE_ID INT, 
    BILLING_NBR VARCHAR(30), 
    BILLING_IMSI VARCHAR(30), 
    CALLING_NBR VARCHAR(30), 
    CALLED_NBR VARCHAR(30), 
    THIRD_PART_NBR VARCHAR(30), 
    START_TIME TIMESTAMP, 
    DURATION INT, 
    LAC_A VARCHAR(30), 
    CELL_A VARCHAR(30), 
    LAC_B VARCHAR(30), 
    CELL_B VARCHAR(30), 
    CALLING_IMEI VARCHAR(30), 
    CALLED_IMEI VARCHAR(30), 
    PRICE_ID1 INT, 
    PRICE_ID2 INT, 
    PRICE_ID3 INT, 
    PRICE_ID4 INT, 
    PRICE_PLAN_ID1 INT, 
    PRICE_PLAN_ID2 INT, 
    PRICE_PLAN_ID3 INT, 
    PRICE_PLAN_ID4 INT, 
    ACCT_RES_ID1 INT, 
    ACCT_RES_ID2 INT, 
    ACCT_RES_ID3 INT, 
    ACCT_RES_ID4 INT, 
    CHARGE1 BIGINT, 
    CHARGE2 BIGINT, 
    CHARGE3 BIGINT, 
    CHARGE4 BIGINT, 
    BAL_ID1 BIGINT, 
    BAL_ID2 BIGINT, 
    BAL_ID3 BIGINT, 
    BAL_ID4 BIGINT, 
    ACCT_ITEM_TYPE_ID1 INT, 
    ACCT_ITEM_TYPE_ID2 INT, 
    ACCT_ITEM_TYPE_ID3 INT, 
    ACCT_ITEM_TYPE_ID4 INT, 
    PREPAY_FLAG INT, 
    PRE_BALANCE1 BIGINT, 
    BALANCE1 BIGINT, 
    PRE_BALANCE2 BIGINT, 
    BALANCE2 BIGINT, 
    PRE_BALANCE3 BIGINT, 
    BALANCE3 BIGINT, 
    PRE_BALANCE4 BIGINT, 
    BALANCE4 BIGINT, 
    INTERNATIONAL_ROAMING_FLAG INT, 
    CALL_TYPE INT, 
    BYTE_UP BIGINT, 
    BYTE_DOWN BIGINT, 
    BYTES BIGINT, 
    PRICE_PLAN_CODE VARCHAR(20), 
    SESSION_ID VARCHAR(50), 
    RESULT_CODE VARCHAR(5), 
    PROD_SPEC_STD_CODE VARCHAR(25), 
    YZDISCOUNT INT, 
    BYZCHARGE1 BIGINT, 
    BYZCHARGE2 BIGINT, 
    BYZCHARGE3 BIGINT, 
    BYZCHARGE4 BIGINT, 
    ONNET_OFFNET INT, 
    PROVIDER_ID INT, 
    PROD_SPEC_ID INT, 
    TERMINATION_CAUSE INT, 
    B_PROD_SPEC_ID VARCHAR(300), 
    B_PRICE_PLAN_CODE VARCHAR(50), 
    CALLSPETYPE INT, 
    CHARGINGRATIO VARCHAR(50), 
    SGSN_ADDRESS VARCHAR(200), 
    GGSN_ADDRESS VARCHAR(200), 
    RATING_GROUP VARCHAR(200), 
    CALLED_STATION_ID VARCHAR(200), 
    PDP_ADDRESS VARCHAR(200), 
    GPP_PDP_TYPE VARCHAR(200), 
    GPP_USER_LOCATION_INFO VARCHAR(300), 
    CHARGE_UNIT VARCHAR(200), 
    ISMP_PRODUCT_OFFER_ID VARCHAR(50), 
    ISMP_PROVIDE_ID VARCHAR(75), 
    MNP_PREFIX VARCHAR(20), 
    FILE_TAP_ID VARCHAR(100), 
    ISMP_PRODUCT_ID VARCHAR(100), 
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT

)
COMMENT 'DATA external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/DATA_POST'
TBLPROPERTIES ('serialization.null.format'='')
;
