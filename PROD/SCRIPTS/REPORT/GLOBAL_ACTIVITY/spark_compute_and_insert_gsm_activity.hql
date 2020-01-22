INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
    COMMERCIAL_OFFER_CODE
     , TRANSACTION_TYPE
     , SUB_ACCOUNT
     , TRANSACTION_SIGN
     , SOURCE_PLATFORM
     , SOURCE_DATA
     , SERVED_SERVICE
     , SERVICE_CODE
     , DESTINATION_CODE
     , SERVED_LOCATION
     , MEASUREMENT_UNIT
     , RATED_COUNT
     , RATED_VOLUME
     , TAXED_AMOUNT
     , UNTAXED_AMOUNT
     , INSERT_DATE
     , TRAFFIC_MEAN
     , OPERATOR_CODE
     , LOCATION_CI
     , TRANSACTION_DATE
FROM(
        SELECT
            OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
             ,(CASE SERVICE_CODE
                   WHEN 'VOI_VOX' THEN 'VOICE'
                   WHEN 'NVX_SMS' THEN 'SMS'
                   WHEN 'NVX_USS' THEN 'USSD'
                   ELSE SERVICE_CODE
            END) TRANSACTION_TYPE
             ,'MAIN' SUB_ACCOUNT
             ,'+' TRANSACTION_SIGN
             , SOURCE_PLATFORM
             ,'FT_GSM_TRAFFIC_REVENUE_DAILY'  SOURCE_DATA
             , SERVED_SERVICE
             , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE) SERVICE_CODE
             , DESTINATION DESTINATION_CODE
             , OTHER_PARTY_ZONE SERVED_LOCATION
             , (CASE SERVICE_CODE
                    WHEN 'VOI_VOX' THEN 'DURATION'
                    WHEN 'NVX_SMS' THEN 'HIT'
                    WHEN 'NVX_USS' THEN 'HIT'
                    ELSE 'HIT' END ) MEASUREMENT_UNIT
             , SUM (TOTAL_COUNT) RATED_COUNT
             , SUM (CASE SERVICE_CODE
                        WHEN 'VOI_VOX' THEN DURATION
                        WHEN 'NVX_SMS' THEN TOTAL_COUNT
                        WHEN 'NVX_USS' THEN TOTAL_COUNT
                        ELSE TOTAL_COUNT END) RATED_VOLUME
             , SUM (MAIN_RATED_AMOUNT) TAXED_AMOUNT
             , SUM ((1-0.1925) * MAIN_RATED_AMOUNT) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             , 'REVENUE' TRAFFIC_MEAN
             , OPERATOR_CODE
             , TRANSACTION_DATE
             , LOCATION_CI
        FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
        WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(MAIN_RATED_AMOUNT,0) > 0
        GROUP BY
            OFFER_PROFILE_CODE
               ,(CASE SERVICE_CODE
                     WHEN 'VOI_VOX' THEN 'VOICE'
                     WHEN 'NVX_SMS' THEN 'SMS'
                     WHEN 'NVX_USS' THEN 'USSD'
                     ELSE SERVICE_CODE
            END)
               , SERVED_SERVICE
               , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE)
               , DESTINATION
               , SOURCE_PLATFORM
               , OTHER_PARTY_ZONE
               , (CASE SERVICE_CODE
                      WHEN 'VOI_VOX' THEN 'DURATION'
                      WHEN 'NVX_SMS' THEN 'HIT'
                      WHEN 'NVX_USS' THEN 'HIT'
                      ELSE 'HIT' END )
               , OPERATOR_CODE
               , TRANSACTION_DATE
               , LOCATION_CI
        UNION
        SELECT
            OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
             ,(CASE SERVICE_CODE
                   WHEN 'VOI_VOX' THEN 'VOICE'
                   WHEN 'NVX_SMS' THEN 'SMS'
                   WHEN 'NVX_USS' THEN 'USSD'
                   ELSE SERVICE_CODE
            END) TRANSACTION_TYPE
             ,'PROMO' SUB_ACCOUNT
             ,'+' TRANSACTION_SIGN
             , SOURCE_PLATFORM
             ,'FT_GSM_TRAFFIC_REVENUE_DAILY'  SOURCE_DATA
             , SERVED_SERVICE
             , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE) SERVICE_CODE
             , DESTINATION DESTINATION_CODE
             , OTHER_PARTY_ZONE SERVED_LOCATION
             , (CASE SERVICE_CODE
                    WHEN 'VOI_VOX' THEN 'DURATION'
                    WHEN 'NVX_SMS' THEN 'HIT'
                    WHEN 'NVX_USS' THEN 'HIT'
                    ELSE 'HIT' END ) MEASUREMENT_UNIT
             , SUM (0) RATED_COUNT
             , SUM (0) RATED_VOLUME
             , SUM (PROMO_RATED_AMOUNT) TAXED_AMOUNT
             , SUM ((1-0.1925) * PROMO_RATED_AMOUNT) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             , 'REVENUE' TRAFFIC_MEAN
             , OPERATOR_CODE
             , TRANSACTION_DATE
             , LOCATION_CI
        FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
        WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(PROMO_RATED_AMOUNT,0) > 0
        GROUP BY
            OFFER_PROFILE_CODE
               ,(CASE SERVICE_CODE
                     WHEN 'VOI_VOX' THEN 'VOICE'
                     WHEN 'NVX_SMS' THEN 'SMS'
                     WHEN 'NVX_USS' THEN 'USSD'
                     ELSE SERVICE_CODE
            END)
               , SERVED_SERVICE
               , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE)
               , DESTINATION
               , SOURCE_PLATFORM
               , OTHER_PARTY_ZONE
               , (CASE SERVICE_CODE
                      WHEN 'VOI_VOX' THEN 'DURATION'
                      WHEN 'NVX_SMS' THEN 'HIT'
                      WHEN 'NVX_USS' THEN 'HIT'
                      ELSE 'HIT' END )
               , OPERATOR_CODE
               , TRANSACTION_DATE
               , LOCATION_CI
    ) A

;

