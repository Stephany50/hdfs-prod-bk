INSERT INTO MON.SPARK_FT_ACTIVATIONS_LOCATION(
    MSISDN, LOGIN, 
    LANG, BLOCKED, 
    ACCOUNT_FORMULE, OSP_STATUS, 
    MAIN_CREDIT, PROMO_CREDIT, 
    OSP_CONTRACT_TYPE, OSP_CUSTOMER_FORMULE, 
    BSCS_ACTIVATION_DATE, BSCS_COMM_OFFER, 
    CURRENT_STATUS, CONTRACT_SNAPSHOT_DATE, 
    FIRST_LOCATION_DATE, FIRST_IMEI, 
    FIRST_LAC, FIRST_LAC_CORRECTED, 
    FIRST_DAY_DURATION_OC, FIRST_DAY_COUNT_OC, 
    FIRST_DAY_DURATION_IC, FIRST_DAY_COUNT_IC, 
    FIRST_DAY_COUNT_SMSMO, FIRST_DAY_COUNT_SMSMT, 
    INSERT_DATE, SERVED_PARTY_LOCATION,
    ACTIVATION_DATE)
    SELECT
        A.MSISDN , a.LOGIN, a.LANG, A.BLOCKED, a.ACCOUNT_FORMULE ACCOUNT_FORMULE, 
        a.OSP_STATUS, A.MAIN_CREDIT, A.PROMO_CREDIT, A.OSP_CONTRACT_TYPE OSP_CONTRACT_TYPE, 
        a.OSP_CUSTOMER_FORMULE OSP_CUSTOMER_FORMULE, BSCS_ACTIVATION_DATE, 
        BSCS_COMM_OFFER, CURRENT_STATUS, a.EVENT_DATE CONTRACT_SNAPSHOT_DATE, 
        (b.EVENT_DATE) FIRST_LOCATION_DATE, (b.IMEI) FIRST_IMEI, 
        (b.LAC) FIRST_LAC, (b.LAC_CORRECTED) FIRST_LAC_CORRECTED, 
        (b.DUREE_SORTANT) FIRST_DAY_DURATION_OC, (b.NBRE_TEL_SORTANT) FIRST_DAY_COUNT_OC, 
        (b.DUREE_ENTRANT) FIRST_DAY_DURATION_IC, (b.NBRE_TEL_ENTRANT) FIRST_DAY_COUNT_IC, 
        (b.NBRE_SMS_SORTANT) FIRST_DAY_COUNT_SMSMO,(b.NBRE_SMS_ENTRANT) FIRST_DAY_COUNT_SMSMT,
        CURRENT_TIMESTAMP INSERT_DATE,b.SERVED_PARTY_LOCATION,
        a.ACTIVATION_DATE
    FROM
         (
            SELECT
                a.EVENT_DATE, A.ACCESS_KEY MSISDN , a.LOGIN, a.LANG, A.BLOCKED, UPPER (A.PROFILE) ACCOUNT_FORMULE, 
                a.OSP_STATUS, A.MAIN_CREDIT, A.PROMO_CREDIT, NVL (A.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) OSP_CONTRACT_TYPE, 
                UPPER (NVL (a.OSP_CUSTOMER_FORMULE, A.PROFILE) ) OSP_CUSTOMER_FORMULE, 
                a.ACTIVATION_DATE, BSCS_ACTIVATION_DATE, BSCS_COMM_OFFER, CURRENT_STATUS
            FROM  MON.SPARK_FT_CONTRACT_SNAPSHOT a
            WHERE  a.EVENT_DATE = date_add("###SLICE_VALUE###",2)
                   AND a.ACTIVATION_DATE = "###SLICE_VALUE###"
                   AND a.SRC_TABLE = 'IT_ICC_ACCOUNT'
         ) a
        LEFT OUTER JOIN
         (
             SELECT  
                 b.EVENT_DATE, b.MSISDN, b.LAC, b.DUREE_SORTANT, 
                 b.NBRE_TEL_SORTANT, b.DUREE_ENTRANT, b.NBRE_TEL_ENTRANT, 
                 b.NBRE_SMS_SORTANT, b.NBRE_SMS_ENTRANT, b.REFRESH_DATE, 
                 b.IMEI, b.LAC_CORRECTED, b.SERVED_PARTY_LOCATION
             FROM
                 (
                    SELECT
                        a.msisdn, MIN (b.EVENT_DATE) FIRST_LOCATION_DATE
                    FROM
                        (
                             SELECT
                                 A.ACCESS_KEY MSISDN
                             FROM  
                                MON.SPARK_FT_CONTRACT_SNAPSHOT a
                             WHERE  
                                a.EVENT_DATE = date_add("###SLICE_VALUE###",2)
                                AND a.ACTIVATION_DATE = "###SLICE_VALUE###"
                                AND a.SRC_TABLE = 'IT_ICC_ACCOUNT'
                        ) a 
                        INNER JOIN
                            ( 
                                SELECT *  
                                FROM 
                                    MON.SPARK_FT_CLIENT_LAC_TRAFFIC_DAY
                                WHERE
                                    EVENT_DATE BETWEEN "###SLICE_VALUE###" AND date_add("###SLICE_VALUE###",2)
                            ) b
                        ON
                            a.msisdn = b.msisdn
                    GROUP BY a.msisdn
                 ) a
                 INNER JOIN 
                  (
                    SELECT * 
                    FROM 
                        MON.SPARK_FT_CLIENT_LAC_TRAFFIC_DAY
                    WHERE 
                        EVENT_DATE BETWEEN "###SLICE_VALUE###" AND date_add("###SLICE_VALUE###",2)
                   ) b
                 ON
                    (a.MSISDN = b.MSISDN
                    AND a.FIRST_LOCATION_DATE = b.EVENT_DATE)                
         ) b
        ON
            a.MSISDN = b.msisdn