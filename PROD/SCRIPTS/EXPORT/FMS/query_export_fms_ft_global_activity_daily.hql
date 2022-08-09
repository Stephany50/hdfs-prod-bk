select 
    date_format(transaction_date,'dd/MM/yyyy') transaction_date,
    commercial_offer_code,
    transaction_type,
    sub_account,
    transaction_sign,
    source_platform,
    source_data,
    served_service,
    service_code,
    destination,
    other_party_zone,
    measurement_unit,
    sum(rated_count) rated_count,
    cast(sum(rated_volume) as decimal(19,2)) rated_volume,
    cast(sum(taxed_amount) as decimal(19,2)) taxed_amount,
    cast(sum(untaxed_amount) as decimal(19,2)) untaxed_amount,
    date_format(INSERT_DATE,'yyyy-MM-dd HH:mm:ss') INSERT_DATE,
    traffic_mean,
    operator_code
FROM
(
    (-- GLOBAL ACTIVITY
        SELECT 
            * 
        FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY
        WHERE TRANSACTION_DATE ='###SLICE_VALUE###' 
            AND SOURCE_DATA NOT IN ('FT_GSM_TRAFFIC_REVENUE_DAILY', 'FT_A_GPRS_ACTIVITY_POST', 'FT_A_GPRS_ACTIVITY', 'FT_GSM_TRAFFIC_REVENUE_POST')
    )
    UNION ALL
    ( -- DATA
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
                    UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
                    ,'GPRS' TRANSACTION_TYPE
                    ,'MAIN' SUB_ACCOUNT
                    ,'+' TRANSACTION_SIGN
                    ,'VOLUBILL' SOURCE_PLATFORM
                    ,'FT_A_GPRS_ACTIVITY' SOURCE_DATA
                    ,'GPRS_TRAFFIC' SERVED_SERVICE
                    , (CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)  SERVICE_CODE
                    , 'DEST_ND' DESTINATION_CODE
                    , NULL SERVED_LOCATION
                    , MEASUREMENT_UNIT MEASUREMENT_UNIT
                    , SUM(1) RATED_COUNT
                    , SUM(nvl(bytes_recv, 0) + nvl(bytes_send, 0)) RATED_VOLUME
                    , SUM(MAIN_COST) TAXED_AMOUNT
                    , SUM ((1-0.1925) * MAIN_COST) UNTAXED_AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    ,'REVENUE' TRAFFIC_MEAN
                    , OPERATOR_CODE OPERATOR_CODE
                    , DATECODE TRANSACTION_DATE
                    , LOCATION_CI
                FROM  AGG.SPARK_FT_A_GPRS_ACTIVITY
                WHERE DATECODE = '###SLICE_VALUE###'
                GROUP BY
                    DATECODE
                    ,UPPER(COMMERCIAL_OFFER)
                    , (CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) 
                    , MEASUREMENT_UNIT
                    , OPERATOR_CODE
                    , LOCATION_CI
                UNION
                SELECT
                    UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
                    ,'GPRS' TRANSACTION_TYPE
                    ,'PROMO' SUB_ACCOUNT
                    ,'+' TRANSACTION_SIGN
                    ,'VOLUBILL' SOURCE_PLATFORM
                    ,'FT_A_GPRS_ACTIVITY' SOURCE_DATA
                    ,'GPRS_TRAFFIC' SERVED_SERVICE
                    , (CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)  SERVICE_CODE
                    , 'DEST_ND' DESTINATION_CODE
                    , NULL SERVED_LOCATION
                    , MEASUREMENT_UNIT MEASUREMENT_UNIT
                    , SUM(1) RATED_COUNT
                    , SUM(nvl(bytes_recv, 0) + nvl(bytes_send, 0)) RATED_VOLUME
                    , SUM(PROMO_COST) TAXED_AMOUNT
                    , SUM ((1-0.1925) * PROMO_COST) UNTAXED_AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    ,'REVENUE' TRAFFIC_MEAN
                    , OPERATOR_CODE OPERATOR_CODE
                    , DATECODE TRANSACTION_DATE
                    , LOCATION_CI
                FROM  AGG.SPARK_FT_A_GPRS_ACTIVITY
                WHERE DATECODE = '###SLICE_VALUE###' AND PROMO_COST > 0
                GROUP BY
                    DATECODE
                    ,UPPER(COMMERCIAL_OFFER)
                    , (CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) 
                    , MEASUREMENT_UNIT
                    , OPERATOR_CODE
                    , LOCATION_CI
            ) A
    )
    UNION ALL
    (-- DATA POST
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
            , NULL LOCATION_CI
            , TRANSACTION_DATE
        FROM(
                SELECT
                    UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
                    ,'GPRS' TRANSACTION_TYPE
                    ,'MAIN' SUB_ACCOUNT
                    ,'+' TRANSACTION_SIGN
                    ,'VOLUBILL' SOURCE_PLATFORM
                    ,'FT_A_GPRS_ACTIVITY_POST' SOURCE_DATA
                    ,'GPRS_TRAFFIC' SERVED_SERVICE
                    , (CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)  SERVICE_CODE
                    , 'DEST_ND' DESTINATION_CODE
                    , NULL SERVED_LOCATION
                    , MEASUREMENT_UNIT MEASUREMENT_UNIT
                    , SUM(1) RATED_COUNT
                    , SUM(nvl(bytes_recv, 0) + nvl(bytes_send, 0)) RATED_VOLUME
                    , SUM(MAIN_COST) TAXED_AMOUNT
                    , SUM ((1-0.1925) * MAIN_COST) UNTAXED_AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    ,'REVENUE_POST' TRAFFIC_MEAN
                    , OPERATOR_CODE OPERATOR_CODE
                    , DATECODE TRANSACTION_DATE
                FROM  AGG.SPARK_FT_A_GPRS_ACTIVITY_POST
                WHERE DATECODE = '###SLICE_VALUE###'
                GROUP BY
                    DATECODE
                    ,UPPER(COMMERCIAL_OFFER)
                    , (CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) 
                    , MEASUREMENT_UNIT
                    , OPERATOR_CODE
                UNION
                SELECT
                    UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
                    ,'GPRS' TRANSACTION_TYPE
                    ,'PROMO' SUB_ACCOUNT
                    ,'+' TRANSACTION_SIGN
                    ,'VOLUBILL' SOURCE_PLATFORM
                    ,'FT_A_GPRS_ACTIVITY_POST' SOURCE_DATA
                    ,'GPRS_TRAFFIC' SERVED_SERVICE
                    , (CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)  SERVICE_CODE
                    , 'DEST_ND' DESTINATION_CODE
                    , NULL SERVED_LOCATION
                    , MEASUREMENT_UNIT MEASUREMENT_UNIT
                    , SUM(1) RATED_COUNT
                    , SUM(nvl(bytes_recv, 0) + nvl(bytes_send, 0)) RATED_VOLUME
                    , SUM(PROMO_COST) TAXED_AMOUNT
                    , SUM ((1-0.1925) * PROMO_COST) UNTAXED_AMOUNT
                    , CURRENT_TIMESTAMP INSERT_DATE
                    ,'REVENUE_POST' TRAFFIC_MEAN
                    , OPERATOR_CODE OPERATOR_CODE
                    , DATECODE TRANSACTION_DATE
                FROM  AGG.SPARK_FT_A_GPRS_ACTIVITY_POST
                WHERE DATECODE = '###SLICE_VALUE###' AND PROMO_COST > 0
                GROUP BY
                    DATECODE
                    ,UPPER(COMMERCIAL_OFFER)
                    , (CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) 
                    , MEASUREMENT_UNIT
                    , OPERATOR_CODE

            ) A
    )
    UNION ALL
    (-- VOIX-SMS
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
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
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
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
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
    )
    UNION ALL
    (-- VOIX-SMS POST
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
            , NULL LOCATION_CI
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
                    ,'FT_GSM_TRAFFIC_REVENUE_POST'  SOURCE_DATA
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
                    , 'REVENUE_POST' TRAFFIC_MEAN
                    , OPERATOR_CODE OPERATOR_CODE
                    , TRANSACTION_DATE TRANSACTION_DATE
                FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_POST
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
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
                    ,'FT_GSM_TRAFFIC_REVENUE_POST'  SOURCE_DATA
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
                    , 'REVENUE_POST' TRAFFIC_MEAN
                    , CAST(OPERATOR_CODE AS VARCHAR(30)) OPERATOR_CODE
                    , CAST(TRANSACTION_DATE AS DATE) TRANSACTION_DATE
                FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_POST
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


            ) A
    )
)
group by transaction_date,
    commercial_offer_code,
    transaction_type,
    sub_account,
    transaction_sign,
    source_platform,
    source_data,
    served_service,
    service_code,
    destination,
    other_party_zone,
    measurement_unit,
    date_format(INSERT_DATE,'yyyy-MM-dd HH:mm:ss'),
    traffic_mean,
    operator_code