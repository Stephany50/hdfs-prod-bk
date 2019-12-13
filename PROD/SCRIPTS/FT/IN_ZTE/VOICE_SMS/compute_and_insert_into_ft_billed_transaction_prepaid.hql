INSERT INTO MON.FT_BILLED_TRANSACTION_PREPAID PARTITION(TRANSACTION_DATE)
SELECT
  FN_GET_NNP_MSISDN_9DIGITS (CALLING_NBR)  SERVED_PARTY
, FN_GET_NNP_MSISDN_9DIGITS (CALLED_NBR) OTHER_PARTY
, MAX(CASE WHEN (NVL(CHARGE1,0) + NVL(CHARGE2,0) + NVL(CHARGE3,0) + NVL(CHARGE4,0)) > 0 THEN NVL(DURATION,0) ELSE 0 END) RATED_DURATION
, MAX(NVL(DURATION,0)) CALL_PROCESS_TOTAL_DURATION
, NULL RATED_VOLUME
, NULL UNIT_OF_MEASUREMENT
, NVL(RATING_EVENT_SERVICE,CAST(RE_ID AS STRING)) SERVICE_CODE
, NULL TELESERVICE_INDICATOR
, NULL NETWORK_EVENT_TYPE
, NULL NETWORK_ELEMENT_ID
, (CASE WHEN RATING_EVENT_NAME LIKE 'VOICE%' THEN TRIM(SUBSTR(RATING_EVENT_NAME,6))
    WHEN RATING_EVENT_NAME LIKE 'SMS%' THEN TRIM(SUBSTR(RATING_EVENT_NAME,4))
    WHEN RATING_EVENT_NAME LIKE 'DATA%' THEN TRIM(SUBSTR(RATING_EVENT_NAME,5))
    WHEN RATING_EVENT_NAME LIKE 'FAX%' THEN TRIM(SUBSTR(RATING_EVENT_NAME,4))
    ELSE NVL(RATING_EVENT_NAME,CAST(RE_ID AS STRING)) END
    ) OTHER_PARTY_ZONE
, TRIM(IF(RATING_EVENT_OPERATOR='OFFNET',
    CASE
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'OCM' THEN 'ONNET'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'INTERNATIONAL' THEN 'INT'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'VIETTEL' THEN 'NEXTTEL'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'INTERNATIONAL_CMR' THEN 'INT'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'CAMTEL' THEN 'CAM'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'OCM_SHORT' THEN 'VAS'
        ELSE FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR))
    END ,
    NVL(RATING_EVENT_OPERATOR,CAST(RE_ID AS STRING)))) CALL_DESTINATION_CODE
, CAST(RESULT_CODE AS STRING) BILLING_TERM_INDICATOR
, NULL NETWORK_TERM_INDICATOR
, BILLING_IMSI SERVED_PARTY_IMSI
, NULL RAW_SPECIFIC_CHARGINGINDICATOR
, NULL RAW_LOAD_LEVEL_INDICATOR
, NULL FEE_NAME
, NULL REFILL_TOPUP_PROFILE
, NULL MAIN_REFILL_AMOUNT
, NULL BUNDLE_REFILL_AMOUNT
, NULL RAW_TRANSNUM_USED_FOR_REFILL
, NVL(PP1.PRICE_PLAN_NAME,NVL(PP2.PRICE_PLAN_NAME,NVL(PP3.PRICE_PLAN_NAME,NVL(PP4.PRICE_PLAN_NAME,CAST(PRICE_PLAN_ID4 AS STRING))))) COMMERCIAL_OFFER
, NVL(PC1.PROFILE_NAME,CAST(A.PROD_SPEC_ID AS STRING)) COMMERCIAL_PROFILE
, MAX(SUBSTR(LAC_A,1,3)) LOCATION_MCC
, MAX(SUBSTR(LAC_A,4,2)) LOCATION_MNC
, MAX(SUBSTR(LAC_A,6,4)) LOCATION_LAC
, MAX(CELL_A) LOCATION_CI
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'MB' THEN NVL(CHARGE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'MB' THEN NVL(CHARGE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'MB' THEN NVL(CHARGE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'MB' THEN NVL(CHARGE4,0)
        ELSE 0 END))/100 MAIN_RATED_AMOUNT
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'PB' THEN NVL(CHARGE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'PB' THEN NVL(CHARGE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'PB' THEN NVL(CHARGE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'PB' THEN NVL(CHARGE4,0)
        ELSE 0 END))/100 PROMO_RATED_AMOUNT
, NULL BUNDLE_IDENTIFIER
, NULL BUNDLE_UNIT
, NULL BUNDLE_CONSUMED_VOLUME
, NULL BUNDLE_DISCARDED_VOLUME
, NULL BUNDLE_REMAINING_VOLUME
, NULL BUNDLE_REFILL_VOLUME
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'PB' THEN NVL(CHARGE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'PB' THEN NVL(CHARGE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'PB' THEN NVL(CHARGE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'PB' THEN NVL(CHARGE4,0)
        ELSE 0 END))/100 RATED_AMOUNT_IN_BUNDLE
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'MB' THEN -NVL(BALANCE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'MB' THEN -NVL(BALANCE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'MB' THEN -NVL(BALANCE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'MB' THEN -NVL(BALANCE4,0)
        ELSE 0 END))/100 MAIN_REMAINING_CREDIT
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'PB' THEN -NVL(BALANCE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'PB' THEN -NVL(BALANCE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'PB' THEN -NVL(BALANCE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'PB' THEN -NVL(BALANCE4,0)
        ELSE 0 END))/100 PROMO_REMAINING_CREDIT
, NVL(RATING_EVENT_SPECIFIC_TARIF,CAST(RE_ID AS STRING)) SPECIFIC_TARIFF_INDICATOR
, NULL LOCATION_NUMBER
, NULL MAIN_DISCARDED_CREDIT
, NULL PROMO_DISCARDED_CREDIT
, NULL SMS_DISCARDED_VOLUME
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'BS' THEN NVL(CHARGE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'BS' THEN NVL(CHARGE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'BS' THEN NVL(CHARGE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'BS' THEN NVL(CHARGE4,0)
        ELSE 0 END)) SMS_USED_VOLUME
, NVL(PP5.PRICE_PLAN_NAME,CAST(A.PRICE_PLAN_CODE AS STRING)) RAW_TARIFF_PLAN
, NULL RAW_EVENT_COST
, NULL RAW_REFILL_MEANS
, NULL RAW_CALL_TYPE
, (CASE
        WHEN RATING_EVENT_NAME LIKE '%URGENT%' AND FN_GET_NNP_MSISDN_9DIGITS (CALLED_NBR) LIKE '69%' THEN 'ONNET'
        WHEN RATING_EVENT_NAME LIKE '%URGENT%' AND FN_GET_NNP_MSISDN_9DIGITS (CALLED_NBR) NOT LIKE '69%' THEN 'OFFNET'
        ELSE NVL(RATING_EVENT_ZONE,CAST(RE_ID AS STRING))
   END) CALL_DESTINATION_TYPE
, INTERNATIONAL_ROAMING_FLAG ROAMING_INDICATOR
, (CASE WHEN PROVIDER_ID = 0 OR PROVIDER_ID = 2 THEN 'OCM'
    WHEN PROVIDER_ID = 1 THEN 'SET'
    WHEN PROVIDER_ID IS NULL AND BILLING_NBR IS NULL THEN 'OCM'
    WHEN PROVIDER_ID IS NULL AND FN_GET_NNP_MSISDN_9DIGITS (BILLING_NBR) NOT LIKE '692%' THEN 'OCM'
    WHEN PROVIDER_ID IS NULL AND FN_GET_NNP_MSISDN_9DIGITS (BILLING_NBR) LIKE '692%' THEN 'SET'
    ELSE CAST(PROVIDER_ID AS STRING) END) OPERATOR_CODE
, NVL(YZDISCOUNT,0) YZDISCOUNT
, MAX(NVL(LPAD(CONV(IF(SUBSTR(CAST(LAC_a AS STRING),6,4)="",NULL,SUBSTR(CAST(LAC_a AS STRING),6,4)), 16, 10) ,5,0),'ND')) location_lac_decimal
, MAX(NVL(LPAD(CONV(IF(CAST(cell_a AS STRING)="",NULL,CAST(cell_a AS STRING)), 16, 10) ,5,0),'ND')) location_ci_decimal
, MAX(NVL(CHARGE1,0) + NVL(CHARGE2,0) + NVL(CHARGE3,0) + NVL(CHARGE4,0)) CHARGE_SUM
, MAX(NVL(BYZCHARGE1,0) + NVL(BYZCHARGE2,0) + NVL(BYZCHARGE3,0) + NVL(BYZCHARGE4,0)) BYZ_RATED_AMOUNT
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'BS' THEN NVL(CHARGE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'BS' THEN NVL(CHARGE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'BS' THEN NVL(CHARGE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'BS' THEN NVL(CHARGE4,0)
        ELSE 0 END)) BUNDLE_SMS_USED_VOLUME
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'BV' THEN NVL(CHARGE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'BV' THEN NVL(CHARGE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'BV' THEN NVL(CHARGE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'BV' THEN NVL(CHARGE4,0)
        ELSE 0 END)) BUNDLE_TIME_USED_VOLUME
, MAX((CASE WHEN NVL(BTI1.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') THEN NVL(CHARGE1,0)
        ELSE 0 END)
    + (CASE WHEN NVL(BTI2.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') THEN NVL(CHARGE2,0)
        ELSE 0 END)
    + (CASE WHEN NVL(BTI3.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') THEN NVL(CHARGE3,0)
        ELSE 0 END)
    + (CASE WHEN NVL(BTI4.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') THEN NVL(CHARGE4,0)
        ELSE 0 END)) UNKNOWN_USED_VOLUME
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'BS' THEN -NVL(BALANCE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'BS' THEN -NVL(BALANCE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'BS' THEN -NVL(BALANCE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'BS' THEN -NVL(BALANCE4,0)
        ELSE 0 END)) BUNDLE_SMS_REMAINING_VOLUME
, MAX((CASE WHEN BTI1.ACCT_RES_RATING_TYPE = 'BV' THEN -NVL(BALANCE1,0)
        ELSE 0 END)
    + (CASE WHEN BTI2.ACCT_RES_RATING_TYPE = 'BV' THEN -NVL(BALANCE2,0)
        ELSE 0 END)
    + (CASE WHEN BTI3.ACCT_RES_RATING_TYPE = 'BV' THEN -NVL(BALANCE3,0)
        ELSE 0 END)
    + (CASE WHEN BTI4.ACCT_RES_RATING_TYPE = 'BV' THEN -NVL(BALANCE4,0)
        ELSE 0 END)) BUNDLE_TIME_REMAINING_VOLUME
, MAX(BYTE_UP/1024) UPLOAD_VOLUME
, MAX(BYTE_DOWN/1024) DOWNLOAD_VOLUME
, MAX( CONCAT_WS('|',
    NVL(NVL(bti1.ACCT_RES_NAME,CAST(a.ACCT_ITEM_TYPE_ID1 AS STRING)), ''),
    NVL(NVL(bti2.ACCT_RES_NAME,CAST(a.ACCT_ITEM_TYPE_ID2 AS STRING)), ''),
    NVL(NVL(bti3.ACCT_RES_NAME,CAST(a.ACCT_ITEM_TYPE_ID3 AS STRING)), ''),
    NVL(NVL(bti4.ACCT_RES_NAME,CAST(a.ACCT_ITEM_TYPE_ID4 AS STRING)), ''))
    ) Identifier_list
, MAX( CONCAT_WS('|',
    NVL(bti1.ACCT_RES_RATING_UNIT,''),
    NVL(bti2.ACCT_RES_RATING_UNIT,''),
    NVL(bti3.ACCT_RES_RATING_UNIT,''),
    NVL(bti4.ACCT_RES_RATING_UNIT,''))
    ) Unit_Of_Measurement_List
,  MAX( CONCAT_WS('|',
     CAST((CASE
                WHEN bti1.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(NVL(charge1,0) AS DOUBLE)/100
                 WHEN bti1.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN NVL(charge1,0)
                 ELSE 0
           END) AS STRING)
     ,CAST((CASE
                 WHEN bti2.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(NVL(charge2,0) AS DOUBLE)/100
                 WHEN bti2.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN NVL(charge2,0)
                 ELSE 0
             END) AS STRING)
     ,CAST((CASE
                 WHEN bti3.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(NVL(charge3,0) AS DOUBLE)/100
                 WHEN bti3.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN NVL(charge3,0)
                 ELSE 0
             END) AS STRING)
     ,CAST((CASE
                 WHEN bti4.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(NVL(charge4,0) AS DOUBLE)/100
                 WHEN bti4.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN NVL(charge4,0)
                 ELSE 0
             END) AS STRING))
    ) Rated_Volume_list
, MAX( CONCAT_WS('|',
    CAST((CASE
         WHEN bti1.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(-NVL(balance1,0) AS DOUBLE)/100
         WHEN bti1.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN -NVL(balance1,0)
         ELSE 0
    END) AS STRING)
     ,CAST((CASE
         WHEN bti2.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(-NVL(balance2,0) AS DOUBLE)/100
         WHEN bti2.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN -NVL(balance2,0)
         ELSE 0
      END) AS STRING)
     ,CAST((CASE
         WHEN bti3.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(-NVL(balance3,0) AS DOUBLE)/100
         WHEN bti3.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN -NVL(balance3,0)
         ELSE 0
     END) AS STRING)
     ,CAST((CASE
         WHEN bti4.ACCT_RES_RATING_TYPE IN ('PB','MB') THEN CAST(-NVL(balance4,0) AS DOUBLE)/100
         WHEN bti4.ACCT_RES_RATING_TYPE IN ('BS','BV') THEN -NVL(balance4,0)
         ELSE 0
     END) AS STRING)) ) Remaining_Volume_list
, (CASE WHEN CALL_TYPE = 0 THEN 'UNK'
        WHEN CALL_TYPE = 1 THEN 'OUT'
        WHEN CALL_TYPE = 2 THEN 'INC'
        WHEN CALL_TYPE = 3 THEN 'FWD'
        ELSE CAST(CALL_TYPE AS STRING)
    END ) TRANSACTION_TYPE
, CALLING_IMEI SERVED_PARTY_IMEI
, FN_GET_NNP_MSISDN_9DIGITS (BILLING_NBR) CHARGED_PARTY
, RESULT_CODE TRANSACTION_TERM_INDICATOR
, MAX( CONCAT_WS('|',
     CAST(IF(NVL(bti1.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') , NVL(charge1,0),0) AS STRING),
     CAST(IF(NVL(bti2.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') , NVL(charge2,0),0) AS STRING),
     CAST(IF(NVL(bti3.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') , NVL(charge3,0),0) AS STRING),
     CAST(IF(NVL(bti4.ACCT_RES_RATING_TYPE,'') NOT IN ('BV','BS','PB', 'MB') , NVL(charge4,0),0) AS STRING))
    ) Unknown_Volume_List
, MAX( CONCAT_WS('|',
    CAST(NVL(charge1,0) AS STRING),
    CAST(NVL(charge2,0) AS STRING),
    CAST(NVL(charge3,0) AS STRING),
    CAST(NVL(charge4,0) AS STRING))
    ) Volume_List
, NVL(PC1.PROFILE_NAME,CAST(A.PROD_SPEC_ID AS STRING)) ORIGINAL_COMMERCIAL_PROFILE
, DATE_FORMAT(start_time,'HHmmss') TRANSACTION_TIME
, MAX(ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME
, 'IN_ZTE' SOURCE_PLATEFORM
, 'IT_ZTE_VOICE_SMS_DATA' SOURCE_DATA
, CURRENT_TIMESTAMP INSERT_DATE
, START_DATE TRANSACTION_DATE
FROM CDR.IT_ZTE_VOICE_SMS A
LEFT JOIN DIM.DT_RATING_EVENT B ON A.RE_ID = B.RATING_EVENT_ID
LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) BTI1 ON A.ACCT_ITEM_TYPE_ID1 = BTI1.ACCT_ITEM_TYPE_ID
LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) BTI2 ON ACCT_ITEM_TYPE_ID2 = BTI2.ACCT_ITEM_TYPE_ID
LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) BTI3 ON A.ACCT_ITEM_TYPE_ID3 = BTI3.ACCT_ITEM_TYPE_ID
LEFT JOIN (SELECT ACCT_RES_ID, ACCT_ITEM_TYPE_ID, UPPER(ACCT_RES_NAME) ACCT_RES_NAME, ACCT_RES_RATING_TYPE, ACCT_RES_RATING_UNIT FROM DIM.DT_BALANCE_TYPE_ITEM) BTI4 ON A.ACCT_ITEM_TYPE_ID4 = BTI4.ACCT_ITEM_TYPE_ID
LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp1 ON ( a.PRICE_PLAN_ID1 = pp1.PRICE_PLAN_ID)
LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM cdr.IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp2 ON ( a.PRICE_PLAN_ID2 = pp2.PRICE_PLAN_ID)
LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp3 ON ( a.PRICE_PLAN_ID3 = pp3.PRICE_PLAN_ID)
LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM cdr.IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp4 ON ( a.PRICE_PLAN_ID4 = pp4.PRICE_PLAN_ID)
LEFT JOIN (SELECT PRICE_PLAN_ID, PRICE_PLAN_NAME, PRICE_PLAN_CODE FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' ) pp5 ON ( a.PRICE_PLAN_CODE = pp5.PRICE_PLAN_CODE)
LEFT JOIN (SELECT PROFILE_ID, MAX(UPPER(PROFILE_NAME)) PROFILE_NAME FROM CDR.SPARK_IT_ZTE_PROFILE GROUP BY PROFILE_ID) PC1 ON A.PROD_SPEC_ID = PC1.PROFILE_ID
WHERE a.START_DATE = '###SLICE_VALUE###'
GROUP BY
(CASE
    WHEN CALL_TYPE = 0 THEN 'UNK'
    WHEN CALL_TYPE = 1 THEN 'OUT'
    WHEN CALL_TYPE = 2 THEN 'INC'
    WHEN CALL_TYPE = 3 THEN 'FWD'
    ELSE CAST(CALL_TYPE AS STRING)
 END )
, START_TIME
, START_DATE
, FN_GET_NNP_MSISDN_9DIGITS (CALLING_NBR)
, BILLING_IMSI
, CALLING_IMEI
, NVL(PP1.PRICE_PLAN_NAME,NVL(PP2.PRICE_PLAN_NAME,NVL(PP3.PRICE_PLAN_NAME,NVL(PP4.PRICE_PLAN_NAME,CAST(PRICE_PLAN_ID4 AS STRING)))))
, NVL(PP5.PRICE_PLAN_NAME,CAST(A.PRICE_PLAN_CODE AS STRING))
, NVL(PC1.PROFILE_NAME,CAST(A.PROD_SPEC_ID AS STRING))
, NVL(PC1.PROFILE_NAME,CAST(A.PROD_SPEC_ID AS STRING))
, FN_GET_NNP_MSISDN_9DIGITS (BILLING_NBR)
, FN_GET_NNP_MSISDN_9DIGITS (CALLED_NBR)
, (CASE WHEN RATING_EVENT_NAME LIKE '%URGENT%' AND FN_GET_NNP_MSISDN_9DIGITS (CALLED_NBR) LIKE '69%' THEN 'ONNET'
    WHEN RATING_EVENT_NAME LIKE '%URGENT%' AND FN_GET_NNP_MSISDN_9DIGITS (CALLED_NBR) NOT LIKE '69%' THEN 'OFFNET'
    ELSE NVL(RATING_EVENT_ZONE,CAST(RE_ID AS STRING)) END )
, IF(RATING_EVENT_OPERATOR='OFFNET',
    CASE
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'OCM' THEN 'ONNET'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'INTERNATIONAL' THEN 'INT'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'VIETTEL' THEN 'NEXTTEL'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'INTERNATIONAL_CMR' THEN 'INT'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'CAMTEL' THEN 'CAM'
        WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR)) = 'OCM_SHORT' THEN 'VAS'
        ELSE FN_GET_NNP_MSISDN_SIMPLE_DESTN(FN_GET_NNP_MSISDN(CALLED_NBR))
    END ,
    NVL(RATING_EVENT_OPERATOR,CAST(RE_ID AS STRING)))
, INTERNATIONAL_ROAMING_FLAG
, (CASE WHEN PROVIDER_ID = 0 OR PROVIDER_ID = 2 THEN 'OCM'
    WHEN PROVIDER_ID = 1 THEN 'SET'
    WHEN PROVIDER_ID IS NULL AND BILLING_NBR IS NULL THEN 'OCM'
    WHEN PROVIDER_ID IS NULL AND FN_GET_NNP_MSISDN_9DIGITS (BILLING_NBR) NOT LIKE '692%' THEN 'OCM'
    WHEN PROVIDER_ID IS NULL AND FN_GET_NNP_MSISDN_9DIGITS (BILLING_NBR) LIKE '692%' THEN 'SET'
    ELSE CAST(PROVIDER_ID AS STRING) END)
, (CASE WHEN RATING_EVENT_NAME LIKE 'VOICE%' THEN TRIM(SUBSTR(RATING_EVENT_NAME,6))
    WHEN RATING_EVENT_NAME LIKE 'SMS%' THEN TRIM(SUBSTR(RATING_EVENT_NAME,4))
    WHEN RATING_EVENT_NAME LIKE 'DATA%' THEN TRIM(SUBSTR(RATING_EVENT_NAME,5))
    WHEN RATING_EVENT_NAME LIKE 'FAX%' THEN TRIM(SUBSTR(RATING_EVENT_NAME,4))
    ELSE NVL(RATING_EVENT_NAME,CAST(RE_ID AS STRING))
   END)
, NVL(RATING_EVENT_SERVICE,CAST(RE_ID AS STRING))
, NVL(RATING_EVENT_SPECIFIC_TARIF,CAST(RE_ID AS STRING))
, NVL(YZDISCOUNT,0)
, RESULT_CODE
, CAST(RESULT_CODE AS STRING)
, 'IN_ZTE'
, 'IT_ZTE_VOICE_SMS_DATA'
, CURRENT_TIMESTAMP
;