INSERT INTO AGG.FT_GSM_TRAFFIC_REVENUE_POST PARTITION(TRANSACTION_DATE)
SELECT
  TRANSACTION_HOUR
  , OFFER_PROFILE_CODE
  , SERVICE_CODE
  , DESTINATION
  , CALL_DESTINATION_CODE
  , OTHER_PARTY_ZONE
  , SPECIFIC_TARIFF_INDICATOR
  , CITYZONE_NUMBER
  , TVA
  , TOTAL_COUNT
  , RATED_TOTAL_COUNT
  , DURATION
  , RATED_DURATION
  , MAIN_RATED_AMOUNT
  , PROMO_RATED_AMOUNT
  , RATED_AMOUNT
  , MAIN_DISCARDED_CREDIT
  , PROMO_DISCARDED_CREDIT
  , SMS_DISCARDED_VOLUME
  , SMS_USED_VOLUME
  , SOURCE_PLATFORM
  , SOURCE_DATA
  , INSERT_DATE
  , SUB_SERVICE
  , SERVED_SERVICE
  , OTHER_PARTY_PRESENT
  , CREDIT_AMOUNT
  , DEBIT_AMOUNT
  , TARIFF_PLAN
  , PROMO_RATED_AMOUNT_PROMO
  , PROMO_RATED_AMOUNT_ONNET
  , PROMO_RATED_AMOUNT_XPRESS
    , CONCAT_WS('', 'MAIN=', CAST(ROUND(MAIN_RATED_AMOUNT, 2) AS STRING), '\;PROMO=', CAST(ROUND(PROMO_RATED_AMOUNT_PROMO, 2) AS STRING), '\;PROMO_ONNET=', CAST(ROUND(PROMO_RATED_AMOUNT_ONNET, 2) AS STRING), '\;PROMO_XPRESS=', CAST(ROUND(PROMO_RATED_AMOUNT_XPRESS, 2) AS STRING)) RATED_AMOUNT_DETAILS
  , OPERATOR_CODE
  , TIME_USED_VOLUME
  , BUNDLE_SMS_USED_VOLUME
  , TRANSACTION_DATE
FROM
(
  SELECT
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('###SLICE_VALUE###', 'yyyy-MM-dd'))) TRANSACTION_DATE
    , SUBSTR(TRANSACTION_TIME, 0, 2) TRANSACTION_HOUR
    , COMMERCIAL_PROFILE OFFER_PROFILE_CODE
    , (CASE
      WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
      WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
      WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
      WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
      WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
      WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
      WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
      WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
      WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
      WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
      WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
      WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
      ELSE 'AUT'
      END ) SERVICE_CODE
    , NETWORK_EVENT_TYPE SUB_SERVICE
    , (CASE WHEN A.NETWORK_ELEMENT_ID IS NOT NULL THEN NETWORK_ELEMENT_ID
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('SMSMO', 'OC','OCFWD','OCRMG','TCRMG','SMSRMG') THEN 'IN_TRAFFIC'
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('TOPUP','REFILL VIA MENU') THEN 'IN_REFILL'
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('DIRECT-DEBIT','DIRECT-CREDIT','DEPOSIT') THEN 'IN_DEPOSIT'
        WHEN SERVICE_CODE = 'TEL' THEN 'IN_TRAFFIC'
        WHEN SERVICE_CODE = 'SMS' THEN 'IN_TRAFFIC'
        ELSE 'IN_SELFCARE'
        END ) SERVED_SERVICE
    , (CASE WHEN A.CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
          WHEN A.CALL_DESTINATION_CODE IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
          WHEN A.CALL_DESTINATION_CODE IN ('NEXTTEL') THEN 'OUT_NAT_MOB_NEX'
          WHEN A.CALL_DESTINATION_CODE IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
          WHEN A.CALL_DESTINATION_CODE IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
          WHEN A.CALL_DESTINATION_CODE = 'VAS' THEN 'OUT_SVA'
          WHEN A.CALL_DESTINATION_CODE = 'EMERG' THEN 'OUT_CCSVA'
          WHEN A.CALL_DESTINATION_CODE = 'OCRMG' THEN 'OUT_ROAM_MO'
          WHEN A.CALL_DESTINATION_CODE = 'INT' THEN 'OUT_INT'
          WHEN A.CALL_DESTINATION_CODE = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
          WHEN A.CALL_DESTINATION_CODE = 'TCRMG' THEN 'OUT_ROAM_MT'
          ELSE CALL_DESTINATION_CODE END ) DESTINATION
    , CALL_DESTINATION_CODE CALL_DESTINATION_CODE
    , (CASE WHEN A.OTHER_PARTY IS NOT NULL THEN 'YES' ELSE 'NO' END) OTHER_PARTY_PRESENT
    , OTHER_PARTY_ZONE OTHER_PARTY_ZONE
    , SPECIFIC_TARIFF_INDICATOR
    , 'Not_Defined' CITYZONE_NUMBER
    , 0.1925 TVA
    , RAW_TARIFF_PLAN TARIFF_PLAN
    , (CASE WHEN OPERATOR_CODE IS NULL THEN IF(CHARGED_PARTY IS NULL, 'OCM', FN_GET_OPERATOR_CODE(CHARGED_PARTY))
        ELSE OPERATOR_CODE END
       ) OPERATOR_CODE
    , SUM(1) TOTAL_COUNT
    , SUM(CASE WHEN MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT > 0 THEN 1 ELSE 0 END) RATED_TOTAL_COUNT
    , SUM(CALL_PROCESS_TOTAL_DURATION) DURATION
    , SUM(CASE WHEN A.MAIN_RATED_AMOUNT + A.PROMO_RATED_AMOUNT > 0 THEN CALL_PROCESS_TOTAL_DURATION ELSE 0 END) RATED_DURATION
    , SUM(A.MAIN_RATED_AMOUNT) MAIN_RATED_AMOUNT
    , SUM(A.PROMO_RATED_AMOUNT) PROMO_RATED_AMOUNT
    , SUM(A.MAIN_RATED_AMOUNT + A.PROMO_RATED_AMOUNT) RATED_AMOUNT
    , SUM(NVL(MAIN_DISCARDED_CREDIT,0)) MAIN_DISCARDED_CREDIT
    , SUM(NVL(PROMO_DISCARDED_CREDIT,0)) PROMO_DISCARDED_CREDIT
    , SUM(NVL(SMS_DISCARDED_VOLUME,0)) SMS_DISCARDED_VOLUME
    , SUM(NVL(SMS_USED_VOLUME,0)) SMS_USED_VOLUME
    , SUM(NVL(MAIN_REFILL_AMOUNT,0)) CREDIT_AMOUNT
    , SUM(0) DEBIT_AMOUNT
    , SUM(0) PROMO_RATED_AMOUNT_PROMO
    , SUM(0) PROMO_RATED_AMOUNT_ONNET
    , SUM(0) PROMO_RATED_AMOUNT_XPRESS
    , 'ZTE' SOURCE_PLATFORM
    , 'FT_BILLED_TRANSACTION_POSTPAID' SOURCE_DATA
    , CURRENT_TIMESTAMP() INSERT_DATE
    , SUM(BUNDLE_SMS_USED_VOLUME) BUNDLE_SMS_USED_VOLUME
    , SUM(NVL(BUNDLE_TIME_USED_VOLUME,0)) TIME_USED_VOLUME
  FROM (
      SELECT A.*
      FROM MON.SPARK_FT_BILLED_TRANSACTION_POSTPAID A
      WHERE
        TRANSACTION_DATE = '###SLICE_VALUE###'
        AND MAIN_RATED_AMOUNT >= 0
        AND PROMO_RATED_AMOUNT >= 0
  ) A
  GROUP BY
    A.COMMERCIAL_PROFILE
    , SUBSTR(TRANSACTION_TIME, 0, 2)
    , (CASE
      WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
      WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
      WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
      WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
      WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
      WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
      WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
      WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
      WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
      WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
      WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
      WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
      ELSE 'AUT'
      END )
    , NETWORK_EVENT_TYPE
    , (CASE WHEN A.NETWORK_ELEMENT_ID IS NOT NULL THEN NETWORK_ELEMENT_ID
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('SMSMO', 'OC','OCFWD','OCRMG','TCRMG','SMSRMG') THEN 'IN_TRAFFIC'
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('TOPUP','REFILL VIA MENU') THEN 'IN_REFILL'
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('DIRECT-DEBIT','DIRECT-CREDIT','DEPOSIT') THEN 'IN_DEPOSIT'
        WHEN SERVICE_CODE = 'TEL' THEN 'IN_TRAFFIC'
        WHEN SERVICE_CODE = 'SMS' THEN 'IN_TRAFFIC'
        ELSE 'IN_SELFCARE'
        END )
    , (CASE WHEN A.CALL_DESTINATION_CODE IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
          WHEN A.CALL_DESTINATION_CODE IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
          WHEN A.CALL_DESTINATION_CODE IN ('NEXTTEL') THEN 'OUT_NAT_MOB_NEX'
          WHEN A.CALL_DESTINATION_CODE IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
          WHEN A.CALL_DESTINATION_CODE IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
          WHEN A.CALL_DESTINATION_CODE = 'VAS' THEN 'OUT_SVA'
          WHEN A.CALL_DESTINATION_CODE = 'EMERG' THEN 'OUT_CCSVA'
          WHEN A.CALL_DESTINATION_CODE = 'OCRMG' THEN 'OUT_ROAM_MO'
          WHEN A.CALL_DESTINATION_CODE = 'INT' THEN 'OUT_INT'
          WHEN A.CALL_DESTINATION_CODE = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
          WHEN A.CALL_DESTINATION_CODE = 'TCRMG' THEN 'OUT_ROAM_MT'
          ELSE CALL_DESTINATION_CODE END )
    , CALL_DESTINATION_CODE
    , (CASE WHEN A.OTHER_PARTY IS NOT NULL THEN 'YES' ELSE 'NO' END)
    , OTHER_PARTY_ZONE
    , SPECIFIC_TARIFF_INDICATOR
    , RAW_TARIFF_PLAN
    , (CASE WHEN OPERATOR_CODE IS NULL THEN IF(CHARGED_PARTY IS NULL, 'OCM', FN_GET_OPERATOR_CODE(CHARGED_PARTY))
        ELSE OPERATOR_CODE END
       )
) T;

