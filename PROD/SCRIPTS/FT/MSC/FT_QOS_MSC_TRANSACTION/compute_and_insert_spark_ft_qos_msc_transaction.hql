INSERT INTO MON.SPARK_FT_QOS_MSC_TRANSACTION
            (
                TRANSACTION_TIME, TRANSACTION_DIRECTION, 
                TRANSACTION_TYPE, SERVED_MSISDN,
                SERVED_MSISDN_INFO, SERVED_IMSI, 
                SERVED_PARTY_LOCATION, SUBSCRIBER_TYPE, 
                OTHER_PARTY, OTHER_PARTY_INFO, 
                OTHER_PARTY_IS_NATIONAL, PARTNER_GT, 
                PARTNER_GT_INFO, PARTNER_GT_IS_NATIONAL, 
                TRANSACTION_TERM_CODE, TRUNCK_IN, 
                TRUNCK_OUT, MSC_ADRESS, SERVICE_CENTRE, 
                TRANSACTION_SERVICE_CODE, RECORD_TYPE, 
                ROAMING_NUMBER, RAW_GSMSCFADDR, RAW_USERTYPE, 
                RAW_LEVELCAMELSVC, OTHER_PARTY_TRANSLATED_NUM, 
                MEASUREMENT_UNIT, TRANSACTION_DURATION, 
                TRANSACTION_VOLUME, INITIAL_DOUBLON_COUNT, 
                CALL_COUNT, REFRESH_DATE,TRANSACTION_DATE)
            SELECT
                SUBSTR(TRANSACTION_TIME, 1, 2) TRANSACTION_TIME, TRANSACTION_DIRECTION, TRANSACTION_TYPE, 
                MON.FN_INTERCO_DESTINATION(SERVED_MSISDN) SERVED_MSISDN, 
                (CASE 
                    WHEN LENGTH(SERVED_MSISDN) IN ('3') AND SUBSTR(SERVED_MSISDN, 1, 1) IN ('9') 
                    THEN SERVED_MSISDN ELSE NULL 
                    END) SERVED_MSISDN_INFO, 
                SUBSTR(SERVED_IMSI, 1, 5) SERVED_IMSI, 
                SUBSTR(SERVED_PARTY_LOCATION, 8, 5) SERVED_PARTY_LOCATION, SUBSCRIBER_TYPE, 
                MON.FN_INTERCO_DESTINATION(OTHER_PARTY) OTHER_PARTY, 
                (CASE 
                    WHEN LENGTH(OTHER_PARTY) IN ('3') AND SUBSTR(OTHER_PARTY, 1, 1) IN ('9') 
                    THEN OTHER_PARTY ELSE NULL 
                    END) OTHER_PARTY_INFO, 
                OTHER_PARTY_IS_NATIONAL, 
                MON.FN_INTERCO_DESTINATION(PARTNER_GT) PARTNER_GT, 
                (CASE 
                    WHEN LENGTH(PARTNER_GT) IN ('3') AND SUBSTR(PARTNER_GT, 1, 1) IN ('9') 
                    THEN PARTNER_GT ELSE NULL 
                    END) PARTNER_GT_INFO, 
                PARTNER_GT_IS_NATIONAL, 
                TRANSACTION_TERM_CODE, TRUNCK_IN, TRUNCK_OUT, 
                MON.FN_INTERCO_DESTINATION(MSC_ADRESS) MSC_ADRESS, 
                MON.FN_INTERCO_DESTINATION(SERVICE_CENTRE) SERVICE_CENTRE, 
                NULL TRANSACTION_SERVICE_CODE, NULL RECORD_TYPE, 
                MON.FN_INTERCO_DESTINATION(ROAMING_NUMBER) ROAMING_NUMBER, 
                MON.FN_INTERCO_DESTINATION(RAW_GSMSCFADDR) RAW_GSMSCFADDR, 
                NULL RAW_USERTYPE, NULL RAW_LEVELCAMELSVC, 
                MON.FN_INTERCO_DESTINATION(OTHER_PARTY_TRANSLATED_NUM) OTHER_PARTY_TRANSLATED_NUM, 
                MEASUREMENT_UNIT, 
                SUM(NVL(TRANSACTION_DURATION, 0)) TRANSACTION_DURATION, 
                SUM(NVL(TRANSACTION_VOLUME, 0)) TRANSACTION_VOLUME, 
                SUM(NVL(INITIAL_DOUBLON_COUNT, 0)) INITIAL_DOUBLON_COUNT, 
                SUM(1) CALL_COUNT, 
                CURRENT_DATE REFRESH_DATE,
                TRANSACTION_DATE
                FROM
                    MON.SPARK_FT_MSC_TRANSACTION a
                WHERE
                    TO_DATE(TRANSACTION_DATE) = '###SLICE_VALUE###'
                GROUP BY
                    TRANSACTION_DATE, SUBSTR(TRANSACTION_TIME, 1, 2) , TRANSACTION_DIRECTION, TRANSACTION_TYPE, 
                    MON.FN_INTERCO_DESTINATION(SERVED_MSISDN), 
                    (CASE 
                        WHEN LENGTH(SERVED_MSISDN) IN ('3') AND SUBSTR(SERVED_MSISDN, 1, 1) IN ('9') 
                        THEN SERVED_MSISDN ELSE NULL 
                        END)
                    , SUBSTR(SERVED_IMSI, 1, 5)
                    , SUBSTR(SERVED_PARTY_LOCATION, 8, 5), SUBSCRIBER_TYPE
                    , MON.FN_INTERCO_DESTINATION(OTHER_PARTY)
                    , (CASE WHEN LENGTH(OTHER_PARTY) IN ('3') AND SUBSTR(OTHER_PARTY, 1, 1) IN ('9') 
                        THEN OTHER_PARTY ELSE NULL 
                        END)
                    , OTHER_PARTY_IS_NATIONAL
                    , MON.FN_INTERCO_DESTINATION(PARTNER_GT)
                    , (CASE 
                        WHEN LENGTH(PARTNER_GT) IN ('3') AND SUBSTR(PARTNER_GT, 1, 1) IN ('9') 
                        THEN PARTNER_GT ELSE NULL 
                        END)
                    , PARTNER_GT_IS_NATIONAL
                    , TRANSACTION_TERM_CODE, TRUNCK_IN, TRUNCK_OUT
                    , MON.FN_INTERCO_DESTINATION(MSC_ADRESS)
                    , MON.FN_INTERCO_DESTINATION(SERVICE_CENTRE)
                    , MON.FN_INTERCO_DESTINATION(ROAMING_NUMBER)
                    , MON.FN_INTERCO_DESTINATION(RAW_GSMSCFADDR)
                    , MON.FN_INTERCO_DESTINATION(OTHER_PARTY_TRANSLATED_NUM)
                    , MEASUREMENT_UNIT