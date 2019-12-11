INSERT INTO CDR.IT_ZTE_DATA PARTITION (START_DATE,FILE_DATE)
SELECT
 EVENT_INST_ID,
 RE_ID ,
 BILLING_NBR ,
 BILLING_IMSI ,
 CALLING_NBR ,
 CALLED_NBR ,
 THIRD_PART_NBR ,
 START_TIME,
 DURATION ,
 LAC_A ,
 CELL_A ,
 LAC_B ,
 CELL_B ,
 CALLING_IMEI ,
 CALLED_IMEI ,
 PRICE_ID1 ,
 PRICE_ID2 ,
 PRICE_ID3 ,
 PRICE_ID4 ,
 PRICE_PLAN_ID1 ,
 PRICE_PLAN_ID2 ,
 PRICE_PLAN_ID3 ,
 PRICE_PLAN_ID4 ,
 ACCT_RES_ID1 ,
 ACCT_RES_ID2 ,
 ACCT_RES_ID3 ,
 ACCT_RES_ID4 ,
 CHARGE1 ,
 CHARGE2 ,
 CHARGE3 ,
 CHARGE4 ,
 BAL_ID1 ,
 BAL_ID2 ,
 BAL_ID3 ,
 BAL_ID4 ,
 ACCT_ITEM_TYPE_ID1 ,
 ACCT_ITEM_TYPE_ID2 ,
 ACCT_ITEM_TYPE_ID3 ,
 ACCT_ITEM_TYPE_ID4 ,
 PREPAY_FLAG ,
 PRE_BALANCE1 ,
 BALANCE1 ,
 PRE_BALANCE2 ,
 BALANCE2 ,
 PRE_BALANCE3 ,
 BALANCE3 ,
 PRE_BALANCE4 ,
 BALANCE4 ,
 INTERNATIONAL_ROAMING_FLAG ,
 CALL_TYPE ,
 BYTE_UP ,
 BYTE_DOWN ,
 BYTES ,
 PRICE_PLAN_CODE ,
 SESSION_ID,
 RESULT_CODE ,
 PROD_SPEC_STD_CODE,
 YZDISCOUNT ,
 BYZCHARGE1 ,
 BYZCHARGE2 ,
 BYZCHARGE3 ,
 BYZCHARGE4 ,
 ONNET_OFFNET ,
 PROVIDER_ID ,
 PROD_SPEC_ID ,
 TERMINATION_CAUSE ,
 B_PROD_SPEC_ID ,
 B_PRICE_PLAN_CODE ,
 CALLSPETYPE ,
 CHARGINGRATIO ,
 SGSN_ADDRESS ,
 GGSN_ADDRESS ,
 RATING_GROUP ,
 CALLED_STATION_ID ,
 PDP_ADDRESS ,
 GPP_PDP_TYPE ,
 GPP_USER_LOCATION_INFO ,
 CHARGE_UNIT ,
 IF(LENGTH(NVL(GPP_USER_LOCATION_INFO,'')) <>16,NULL,CONCAT(SUBSTRING(GPP_USER_LOCATION_INFO,4,1),SUBSTRING(GPP_USER_LOCATION_INFO,3,1),SUBSTRING(GPP_USER_LOCATION_INFO,6,1))) MCC,
 IF(LENGTH(NVL(GPP_USER_LOCATION_INFO,'')) <>16,NULL,CONCAT(SUBSTRING(GPP_USER_LOCATION_INFO,8,1),SUBSTRING(GPP_USER_LOCATION_INFO,7,1))) MNC,
 IF(LENGTH(NVL(GPP_USER_LOCATION_INFO,'')) <>16,'',CONV(SUBSTRING(GPP_USER_LOCATION_INFO,-4),16,10)) CI,
 IF(LENGTH(NVL(GPP_USER_LOCATION_INFO,'')) <>16,'',CONV(SUBSTRING(GPP_USER_LOCATION_INFO,-9,5),16,10)) LAC,
 ISMP_PRODUCT_OFFER_ID ,
 IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ISMP_PRODUCT_ID,NULL) ISMP_PRODUCT_ID,
 IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_NAME,FILE_TAP_ID) ORIGINAL_FILE_NAME,
 FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_NAME,FILE_TAP_ID), -25, 15),'yyyyMMddHHmmss')) ORIGINAL_FILE_DATE,
 CURRENT_TIMESTAMP() INSERT_DATE,
 IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_SIZE,CAST(ISMP_PRODUCT_ID AS INT)) ORIGINAL_FILE_SIZE,
 IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_LINE_COUNT,CAST(ORIGINAL_FILE_NAME AS INT)) ORIGINAL_FILE_LINE_COUNT,
 TO_DATE(START_TIME) START_DATE,
 TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (IF(ORIGINAL_FILE_NAME LIKE '%in_pr%',ORIGINAL_FILE_NAME,FILE_TAP_ID), -25, 15),'yyyyMMddHHmmss'))) FILE_DATE
FROM CDR.TT_ZTE_DATA  C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_ZTE_DATA WHERE START_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL