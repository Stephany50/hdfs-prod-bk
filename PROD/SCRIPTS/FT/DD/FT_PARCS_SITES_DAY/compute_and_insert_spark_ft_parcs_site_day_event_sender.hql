INSERT INTO  MON.SPARK_FT_PARCS_SITE_DAY
        SELECT
             'PARC_EVENT_SENDER' AS PARC_TYPE
            , UPPER(NVL(d.PROFILE_NAME, commercial_offer )) PROFILE
            , account_status AS STATUT, SUM(NVL(Total_count,0)) EFFECTIF
            , SITE_NAME, TOWNNAME
            , ADMINISTRATIVE_REGION
            , COMMERCIAL_REGION, source AS SRC_TABLE
            , CURRENT_TIMESTAMP  INSERT_DATE
            , UPPER(a.OSP_CONTRACT_TYPE) AS CONTRACT_TYPE
            , a.OPERATOR_CODE OPERATOR_CODE, datecode EVENT_DATE
        FROM
        (
            SELECT
                  NVL(a.OSP_CONTRACT_TYPE, 'PURE PREPAID') OSP_CONTRACT_TYPE
                 , a.PROFILE commercial_offer
                 , 'ACTIF' account_status
                 , COUNT(*) AS Total_count
                 , a.SRC_TABLE source
                 , SITE_NAME
                 , TOWNNAME
                 , ADMINISTRATIVE_REGION
                 , COMMERCIAL_REGION
                 , a.OPERATOR_CODE, DATE_ADD('###SLICE_VALUE###',-1) datecode
            FROM
                (SELECT * FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                 WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###'
                  AND OSP_STATUS <> 'TERMINATED'
                  AND  SRC_TABLE = 'IT_ICC_ACCOUNT'
                  AND (TO_DATE(ACTIVATION_DATE) <= DATE_ADD('###SLICE_VALUE###',-1)
                       OR TO_DATE(BSCS_ACTIVATION_DATE) <= DATE_ADD('###SLICE_VALUE###',-1))) a
                INNER JOIN
                  (
                    SELECT DISTINCT SENDER_MSISDN AS MSISDN
                    FROM MON.SPARK_FT_REFILL
                    WHERE TO_DATE(REFILL_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
                        AND SENDER_MSISDN IS NOT NULL
                        AND NVL(REFILL_AMOUNT, 0) > 0
                    UNION
                    SELECT DISTINCT SENDER_MSISDN AS MSISDN
                    FROM MON.SPARK_FT_CREDIT_TRANSFER
                    WHERE TO_DATE(REFILL_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
                        AND SENDER_MSISDN IS NOT NULL
                        AND NVL(TRANSFER_AMT, 0) > 0
                    UNION
                    SELECT MSISDN
                    FROM MON.SPARK_FT_CONSO_MSISDN_DAY
                    WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
                        AND (NVL(SMS_COUNT,0) > 0 OR NVL(TEL_COUNT,0) > 0
                                              OR NVL(PROMOTIONAL_CALL_COST,0) > 0 OR NVL(MAIN_CALL_COST,0) > 0)
                    UNION
                    SELECT DISTINCT SERVED_PARTY_MSISDN AS MSISDN
                    FROM MON.SPARK_FT_CRA_GPRS
                    WHERE TO_DATE(SESSION_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
                        AND TOTAL_COST > 0
                    UNION
                    SELECT DISTINCT SERVED_PARTY_MSISDN AS MSISDN
                    FROM MON.SPARK_FT_SUBSCRIPTION
                    WHERE TO_DATE(TRANSACTION_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
                        AND RATED_AMOUNT > 0
                  ) b
                ON a.ACCESS_KEY = b.MSISDN
                LEFT JOIN
                  (
                    -- localisation par site des numeros
                    SELECT MSISDN, SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
                    FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
                    WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
                   ) c
                ON b.MSISDN = c.MSISDN
            WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###'
                  AND a.OSP_STATUS <> 'TERMINATED'
                  AND  a.SRC_TABLE = 'IT_ICC_ACCOUNT' -- AND ROWNUM < 1
                  AND (TO_DATE(a.ACTIVATION_DATE) <= DATE_ADD('###SLICE_VALUE###',-1)
                       OR TO_DATE(a.BSCS_ACTIVATION_DATE) <= DATE_ADD('###SLICE_VALUE###',-1))
            GROUP BY
                   a.PROFILE
                 , NVL(a.OSP_CONTRACT_TYPE, 'PURE PREPAID')
                 , a.SRC_TABLE
                 , SITE_NAME, TOWNNAME
                 , ADMINISTRATIVE_REGION
                 , COMMERCIAL_REGION
                 , a.OPERATOR_CODE
        ) a
        LEFT JOIN
           MON.VW_DT_OFFER_PROFILES d
        ON UPPER (a.commercial_offer) = d.PROFILE_CODE
        GROUP BY
             UPPER (a.OSP_CONTRACT_TYPE), account_status , source
            , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer ))
            , SITE_NAME
            , TOWNNAME
            , ADMINISTRATIVE_REGION
            , COMMERCIAL_REGION
            , a.OPERATOR_CODE, datecode