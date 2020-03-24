INSERT INTO MON.SPARK_FT_SITE_360
SELECT
    CELL_NAME,
    CI,
    TECHNOLOGIE,

    LOC_SITE_NAME,
    LOC_TOWN_NAME,
    LOC_ADMINISTRATIVE_REGION,
    LOC_COMMERCIAL_REGION,
    LOC_ZONE_PMO,
    LOC_QUARTIER,
    LOC_ARRONDISSEMENT,
    LOC_DEPARTEMENT,
    LOC_SECTOR,

    PARC_GROUPE,
    PARC_ART,
    PARC_ACTIF_PERIOD,
    PARC_OM,
    DATA_USERS,
    NULL VOICE_USERS,

    GROSS_ADD,

    NBRE_CALL_BOX,
    NULL RUPTURE_STOCK,

    NULL NBRE_FAMOCO,

    NULL SMARTPHONES_3G,
    NULL SMARTPHONES_4G,

    NULL TOTAL_REVENUE,
    NULL TOTAL_VOICE_REVENUE,
    NULL TOTAL_SMS_REVENUE,
    NULL TOTAL_SUBS_REVENUE,
    NULL ROAM_IN_VOICE_REVENUE,
    NULL ROAM_OUT_VOICE_REVENUE,
    NULL ROAM_IN_SMS_REVENUE,
    NULL ROAM_OUT_SMS_REVENUE,
    ROAM_DATA_REVENUE,
    NULL MAIN_RATED_TEL_AMOUNT,
    NULL MAIN_RATED_TEL_OCM_AMOUNT,
    NULL MAIN_RATED_TEL_MTN_AMOUNT,
    NULL MAIN_RATED_TEL_NEXTTEL_AMOUNT,
    NULL MAIN_RATED_TEL_CAMTEL_AMOUNT,
    NULL MAIN_RATED_TEL_SET_AMOUNT,
    NULL MAIN_RATED_TEL_ROAM_IN_AMOUNT,
    NULL MAIN_RATED_TEL_ROAM_OUT_AMOUNT,
    NULL MAIN_RATED_TEL_SVA_AMOUNT,
    NULL MAIN_RATED_TEL_INT_AMOUNT,
    NULL MAIN_RATED_SMS_AMOUNT,
    DATA_MAIN_RATED_AMOUNT,
    DATA_GOS_MAIN_RATED_AMOUNT,
    DATA_ROAM_MAIN_RATED_AMOUNT,
    DATA_VIA_OM,
    NULL AMOUNT_EMERGENCY_DATA,
    REVENU_OM,

    C2S_REFILL_COUNT,
    C2S_MAIN_REFILL_AMOUNT,
    C2S_PROMO_REFILL_AMOUNT,
    P2P_REFILL_COUNT,
    P2P_REFILL_AMOUNT,
    SCRATCH_REFILL_COUNT,
    SCRATCH_MAIN_REFILL_AMOUNT,
    SCRATCH_PROMO_REFILL_AMOUNT,
    P2P_REFILL_FEES,
    NULL DATA_REFILL_FEES,
    OM_REFILL_COUNT,
    OM_REFILL_AMOUNT,

    NULL OG_RATED_CALL_DURATION,
    NULL OG_TOTAL_CALL_DURATION,
    NULL RATED_TEL_OCM_DURATION,
    NULL RATED_TEL_MTN_DURATION,
    NULL RATED_TEL_NEXTTEL_DURATION,
    NULL RATED_TEL_CAMTEL_DURATION,
    NULL RATED_TEL_SET_DURATION,
    NULL RATED_TEL_ROAM_IN_DURATION,
    NULL RATED_TEL_ROAM_OUT_DURATION,
    NULL RATED_TEL_SVA_DURATION,
    NULL RATED_TEL_INT_DURATION,

    NULL OG_SMS_TOTAL_COUNT,
    NULL OG_SMS_OCM_COUNT,
    NULL OG_SMS_MTN_COUNT,
    NULL OG_SMS_NEXTTEL_COUNT,
    NULL OG_SMS_CAMTEL_COUNT,
    NULL OG_SMS_SET_COUNT,
    NULL OG_SMS_ROAM_IN_COUNT,
    NULL OG_SMS_ROAM_OUT_COUNT,
    NULL OG_SMS_SVA_COUNT,
    NULL OG_SMS_INTERNATIONAL_COUNT,

    DATA_BYTES_RECEIVED,
    DATA_BYTES_SENT,
    DATA_BYTES_USED_IN_BUNDLE_ROAM,
    DATA_BYTES_USED_PAYGO_ROAM,

    CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        A.CI,
        A.CELL_NAME,
        A.LOC_SITE_NAME,
        A.LOC_TOWN_NAME,
        A.LOC_ADMINISTRATIVE_REGION,
        A.LOC_COMMERCIAL_REGION,
        A.LOC_ZONE_PMO,
        A.LOC_QUARTIER,
        A.LOC_ARRONDISSEMENT,
        A.LOC_DEPARTEMENT,
        A.LOC_SECTOR,
        A.TECHNOLOGIE,
        B.DATA_USERS,
        B.DATA_MAIN_RATED_AMOUNT,
        B.ROAM_DATA_REVENUE,
        B.DATA_GOS_MAIN_RATED_AMOUNT,
        B.DATA_ROAM_MAIN_RATED_AMOUNT,
        B.DATA_BYTES_SENT,
        B.DATA_BYTES_RECEIVED,
        B.DATA_BYTES_USED_IN_BUNDLE_ROAM,
        B.DATA_BYTES_USED_PAYGO_ROAM,
        C.C2S_REFILL_COUNT,
        C.C2S_MAIN_REFILL_AMOUNT,
        C.C2S_PROMO_REFILL_AMOUNT,
        C.P2P_REFILL_COUNT,
        C.P2P_REFILL_AMOUNT,
        C.SCRATCH_REFILL_COUNT,
        C.SCRATCH_MAIN_REFILL_AMOUNT,
        C.SCRATCH_PROMO_REFILL_AMOUNT,
        C.P2P_REFILL_FEES,
        C.NBRE_CALL_BOX,
        D.GROSS_ADD,
        F.PARC_GROUPE,
        G.PARC_ART,
        H.PARC_ACTIF_PERIOD,
        I.PARC_OM,
        J.OM_REFILL_COUNT,
        J.OM_REFILL_AMOUNT,
        K.REVENU_OM,
        L.DATA_VIA_OM
    FROM
    (
        SELECT
            CI
            , SITE_NAME LOC_SITE_NAME
            , TOWNNAME LOC_TOWN_NAME
            , REGION LOC_ADMINISTRATIVE_REGION
            , COMMERCIAL_REGION LOC_COMMERCIAL_REGION
            , ZONEPMO LOC_ZONE_PMO
            , QUARTIER LOC_QUARTIER
            , ARRONDISSEMENT LOC_ARRONDISSEMENT
            , DEPARTEMENT LOC_DEPARTEMENT
            , SECTEUR LOC_SECTOR
            , TECHNOLOGIE
            , CELLNAME CELL_NAME
        FROM DIM.DT_GSM_CELL_CODE
    ) A
    LEFT JOIN
    (
        SELECT
            LOCATION_CI CI
            , COUNT(DISTINCT CHARGED_PARTY_MSISDN) DATA_USERS
            , SUM(CASE WHEN NVL(SDP_GOS_SERV_NAME, 'NOT_GOS') = 'NOT_GOS' THEN NVL(MAIN_COST, 0) ELSE 0 END) DATA_MAIN_RATED_AMOUNT
            , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(MAIN_COST, 0) ELSE 0 END) ROAM_DATA_REVENUE
            , SUM(CASE WHEN UPPER(SERVICE_CODE) LIKE '%GOS%SDP%' THEN NVL(MAIN_COST, 0) ELSE 0 END) DATA_GOS_MAIN_RATED_AMOUNT
            , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(MAIN_COST, 0) ELSE 0 END) DATA_ROAM_MAIN_RATED_AMOUNT 
            , SUM(NVL(BYTES_SENT, 0)) DATA_BYTES_SENT
            , SUM(NVL(BYTES_RECEIVED, 0)) DATA_BYTES_RECEIVED
            , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(BUNDLE_BYTES_USED_VOLUME, 0) ELSE 0 END) DATA_BYTES_USED_IN_BUNDLE_ROAM
            , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(BYTES_SENT, 0) + NVL(BYTES_RECEIVED, 0) - NVL(BUNDLE_BYTES_USED_VOLUME, 0) ELSE 0 END) DATA_BYTES_USED_PAYGO_ROAM
        FROM MON.SPARK_FT_CRA_GPRS
        WHERE SESSION_DATE = '###SLICE_VALUE###' AND NVL(MAIN_COST, 0) >= 0
        GROUP BY LOCATION_CI
    ) B ON A.CI = B.CI
    LEFT JOIN
    (
        SELECT
            SUM(C2S_REFILL_COUNT) C2S_REFILL_COUNT,
            SUM(C2S_MAIN_REFILL_AMOUNT) C2S_MAIN_REFILL_AMOUNT,
            SUM(C2S_PROMO_REFILL_AMOUNT) C2S_PROMO_REFILL_AMOUNT,
            SUM(P2P_REFILL_COUNT) P2P_REFILL_COUNT,
            SUM(P2P_REFILL_AMOUNT) P2P_REFILL_AMOUNT,
            SUM(SCRATCH_REFILL_COUNT) SCRATCH_REFILL_COUNT,
            SUM(SCRATCH_MAIN_REFILL_AMOUNT) SCRATCH_MAIN_REFILL_AMOUNT,
            SUM(SCRATCH_PROMO_REFILL_AMOUNT) SCRATCH_PROMO_REFILL_AMOUNT,
            SUM(P2P_REFILL_FEES) P2P_REFILL_FEES,
            (
                CASE
                    WHEN SUM(SCRATCH_REFILL_COUNT) = 0 THEN COUNT(DISTINCT MSISDN_CALL_BOX)
                    ELSE COUNT(DISTINCT MSISDN_CALL_BOX) - 1 --- CAR SI LE SCRATCH_REFILL_COUNT EST DIFFERENT DE 0 L'ON A FORCÉMENT AU MOINS UNE RECHARGE PAR CARTE DONC LE DISTINCT VA RENVOYER AUSSI NULL
                END
            ) NBRE_CALL_BOX,
            CI
        FROM
        (
            SELECT
                NVL(C2S_REFILL_COUNT, 0) AS C2S_REFILL_COUNT,
                NVL(C2S_MAIN_REFILL_AMOUNT, 0) AS C2S_MAIN_REFILL_AMOUNT,
                NVL(C2S_PROMO_REFILL_AMOUNT, 0) AS C2S_PROMO_REFILL_AMOUNT,
                NVL(P2P_REFILL_COUNT, 0) AS P2P_REFILL_COUNT,
                NVL(P2P_REFILL_AMOUNT, 0) AS P2P_REFILL_AMOUNT,
                NVL(SCRATCH_REFILL_COUNT, 0) AS SCRATCH_REFILL_COUNT,
                NVL(SCRATCH_MAIN_REFILL_AMOUNT, 0) AS SCRATCH_MAIN_REFILL_AMOUNT,
                NVL(SCRATCH_PROMO_REFILL_AMOUNT, 0) AS SCRATCH_PROMO_REFILL_AMOUNT,
                NVL(P2P_REFILL_FEES, 0) AS P2P_REFILL_FEES,
                C00.SENDER_MSISDN MSISDN_CALL_BOX,
                NVL(C00.SENDER_MSISDN, C01.SENDER_MSISDN) SENDER_MSISDN,
                NVL(C00.REFILL_TIME, C01.REFILL_TIME) REFILL_TIME
            FROM
            (
                SELECT
                    C001.SENDER_MSISDN
                    , CASE WHEN C001.REFILL_MEAN='C2S' THEN 1 ELSE 0 END C2S_REFILL_COUNT
                    , CASE WHEN C001.REFILL_MEAN='C2S' THEN C001.REFILL_AMOUNT ELSE 0 END C2S_MAIN_REFILL_AMOUNT
                    , CASE WHEN C001.REFILL_MEAN='C2S' THEN C001.REFILL_BONUS ELSE 0 END C2S_PROMO_REFILL_AMOUNT
                    , CASE WHEN C001.REFILL_MEAN='SCRATCH' THEN 1 ELSE 0 END SCRATCH_REFILL_COUNT
                    , CASE WHEN C001.REFILL_MEAN='SCRATCH' THEN C001.REFILL_AMOUNT ELSE 0 END SCRATCH_MAIN_REFILL_AMOUNT
                    , CASE WHEN C001.REFILL_MEAN='SCRATCH' THEN C001.REFILL_BONUS ELSE 0 END SCRATCH_PROMO_REFILL_AMOUNT
                    , C001.REFILL_TIME
                FROM MON.SPARK_FT_REFILL C001
                WHERE C001.REFILL_DATE = '###SLICE_VALUE###'
                    AND C001.TERMINATION_IND='200'
            ) C00
            FULL JOIN
            (
                SELECT
                    C011.SENDER_MSISDN
                    , 1 P2P_REFILL_COUNT
                    , C011.TRANSFER_AMT P2P_REFILL_AMOUNT
                    , C011.TRANSFER_FEES P2P_REFILL_FEES
                    , C011.REFILL_TIME
                FROM MON.SPARK_FT_CREDIT_TRANSFER C011
                WHERE C011.REFILL_DATE = '###SLICE_VALUE###'
                    AND C011.TERMINATION_IND='000'
            ) C01 ON C00.SENDER_MSISDN = C01.SENDER_MSISDN
        ) C0
        LEFT JOIN
        (
            SELECT
                SUBSTRING(SERVED_PARTY_LOCATION, 14, 5) CI,
                SERVED_MSISDN,
                TRANSACTION_TIME
            FROM MON.SPARK_FT_MSC_TRANSACTION
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                AND OTHER_PARTY IN ('393337', '393338') -------- IL RESTE LE OTHER_PARTY POUR LES RECHARGES
        ) C1 ON C0.SENDER_MSISDN = C1.SERVED_MSISDN
        WHERE UNIX_TIMESTAMP(CONCAT_WS(' ', '###SLICE_VALUE###', CONCAT_WS(':', SUBSTRING(C0.REFILL_TIME, 1, 2), SUBSTRING(C0.REFILL_TIME, 3, 2), SUBSTRING(C0.REFILL_TIME, 5, 2)))) - 
            UNIX_TIMESTAMP(CONCAT_WS(' ', '###SLICE_VALUE###', CONCAT_WS(':', SUBSTRING(C1.TRANSACTION_TIME, 1, 2), SUBSTRING(C1.TRANSACTION_TIME, 3, 2), SUBSTRING(C1.TRANSACTION_TIME, 5, 2)))) 
            BETWEEN -120 AND 120
        GROUP BY CI
    ) C ON A.CI = C.CI
    LEFT JOIN
    (
        SELECT
            CI,
            SUM(
                CASE WHEN NVL(ACTIVATION_DATE, BSCS_ACTIVATION_DATE) = '###SLICE_VALUE###' AND
                (
                    CASE WHEN NVL(OSP_STATUS, CURRENT_STATUS)='ACTIVE' THEN 'ACTIF'
                    WHEN NVL(OSP_STATUS, CURRENT_STATUS)='a' THEN 'ACTIF'
                    WHEN NVL(OSP_STATUS, CURRENT_STATUS)='d' THEN 'DEACT'
                    WHEN NVL(OSP_STATUS, CURRENT_STATUS)='s' THEN 'INACT'
                    WHEN NVL(OSP_STATUS, CURRENT_STATUS)='DEACTIVATED' THEN 'DEACT'
                    WHEN NVL(OSP_STATUS, CURRENT_STATUS)='INACTIVE' THEN 'INACT'
                    WHEN NVL(OSP_STATUS, CURRENT_STATUS)='VALID' THEN 'VALIDE'
                    ELSE NVL(OSP_STATUS, CURRENT_STATUS)
                    END
                ) IN ('ACTIF', 'INACT') THEN 1
                ELSE 0
                END
            ) GROSS_ADD
        FROM
        (
            SELECT
                ACTIVATION_DATE,
                BSCS_ACTIVATION_DATE,
                OSP_STATUS,
                CURRENT_STATUS,
                ACCESS_KEY
            FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
            WHERE
                EVENT_DATE = DATE_SUB('###SLICE_VALUE###', -1)
                AND (NVL(ACTIVATION_DATE, BSCS_ACTIVATION_DATE) <= '###SLICE_VALUE###')
        ) D0
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(IDENTIFICATEUR) IDENTIFICATEUR
            FROM DIM.SPARK_DT_BASE_IDENTIFICATION
            GROUP BY MSISDN
        ) D1 ON D0.ACCESS_KEY = D1.MSISDN
        LEFT JOIN TMP.TT_SITE_360_1 D2 ON D1.IDENTIFICATEUR = D2.MSISDN
        GROUP BY CI
    ) D ON A.CI = D.CI
    LEFT JOIN
    (
        SELECT
            CI
            , COUNT(*) PARC_GROUPE
        FROM
        (
            SELECT
                F00.MSISDN
            FROM
            (
                SELECT
                    UPPER(F000.PROFILE) PROFILE
                    , NVL (F001.GP_STATUS, 'INACT') STATUT
                    , F000.ACCESS_KEY MSISDN
                FROM
                (
                    SELECT
                        PROFILE
                        , ACCESS_KEY
                    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE = '###SLICE_VALUE###'
                ) F000
                LEFT JOIN (
                    SELECT
                        GP_STATUS
                        , MSISDN
                    FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
                    WHERE EVENT_DATE = '###SLICE_VALUE###'
                ) F001 ON F000.ACCESS_KEY = F001.MSISDN
                UNION ALL
                SELECT
                    UPPER(F002.FORMULE) PROFILE
                    , NVL(F002.GP_STATUS, 'INACT') STATUT
                    , F002.MSISDN
                FROM
                (
                    SELECT
                        MSISDN,
                        FORMULE,
                        GP_STATUS
                    FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
                    WHERE EVENT_DATE = '###SLICE_VALUE###'
                ) F002
                LEFT JOIN
                (
                    SELECT
                        F0030.MSISDN
                    FROM
                    (
                        SELECT
                            MSISDN
                        FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
                        WHERE EVENT_DATE = '###SLICE_VALUE###' AND NVL(GP_STATUS, 'INACT') = 'ACTIF'
                    ) F0030
                    LEFT JOIN
                    (
                        SELECT
                            ACCESS_KEY MSISDN
                        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                        WHERE EVENT_DATE = '###SLICE_VALUE###'
                    ) F0031 ON F0030.MSISDN = F0031.MSISDN
                    WHERE F0031.MSISDN IS NULL
                ) F003 ON F002.MSISDN = F003.MSISDN
                LEFT JOIN DIM.DT_OFFER_PROFILES F004 ON F002.FORMULE = F004.PROFILE_CODE
                WHERE NVL(UPPER(F004.CONTRACT_TYPE), 'PURE PREPAID' ) IN ('PURE PREPAID', 'HYBRID')
                    AND F003.MSISDN IS NOT NULL
            ) F00
            LEFT JOIN DIM.DT_OFFER_PROFILES F01 ON UPPER(F00.PROFILE) = F01.PROFILE_CODE
            WHERE F00.STATUT='ACTIF'
                AND NVL(F01.OPERATOR_CODE, 'OCM') <> 'SET'
                AND (
                    CASE
                        WHEN F00.PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN 1
                        ELSE 0
                    END
                ) = 0
        ) F0
        LEFT JOIN TMP.TT_SITE_360_1 F1 ON F0.MSISDN = F1.MSISDN
        GROUP BY CI
    ) F ON A.CI = F.CI
    LEFT JOIN
    (
        SELECT
            CI
            , COUNT(*) PARC_ART
        FROM
        (
            SELECT
                MSISDN
            FROM
            (
                SELECT
                    G000.ACCESS_KEY MSISDN
                    , G001.COMGP_STATUS ACCOUNT_STATUS
                FROM
                (
                    SELECT ACCESS_KEY
                    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE= '###SLICE_VALUE###'
                        AND ACTIVATION_DATE <= '###SLICE_VALUE###'
                        AND NVL(OSP_CONTRACT_TYPE, 'PURE PREPAID') IN ('PURE PREPAID', 'HYBRID')
                ) G000
                LEFT JOIN 
                (
                    SELECT
                        MSISDN
                        , COMGP_STATUS
                    FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
                    WHERE EVENT_DATE = '###SLICE_VALUE###'
                ) G001 ON G000.ACCESS_KEY = G001.MSISDN
                UNION ALL
                SELECT
                    MSISDN
                    , G002.COMGP_STATUS ACCOUNT_STATUS
                FROM 
                (
                    SELECT
                        G0020.MSISDN
                        , ACTIVATION_DATE
                        , PROFILE
                        , COMGP_STATUS
                    FROM
                    (
                        SELECT
                            MSISDN
                            , ACTIVATION_DATE
                            , FORMULE PROFILE
                            , COMGP_STATUS
                        FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
                        WHERE EVENT_DATE = '###SLICE_VALUE###' AND NVL(COMGP_STATUS, 'INACT') = 'ACTIF'
                    ) G0020
                    LEFT JOIN
                    (
                        SELECT
                            ACCESS_KEY MSISDN
                        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                        WHERE EVENT_DATE = '###SLICE_VALUE###'
                    ) G0021 ON G0020.MSISDN = G0021.MSISDN
                    WHERE G0021.MSISDN IS NULL
                ) G002
                LEFT JOIN MON.VW_DT_OFFER_PROFILES G003 ON G002.PROFILE = G003.PROFILE_CODE
                WHERE
                    G002.ACTIVATION_DATE <= '###SLICE_VALUE###'
                    AND NVL(G003.CONTRACT_TYPE, 'PURE PREPAID') IN ('PURE PREPAID', 'HYBRID')
                UNION ALL
                SELECT
                    ACCESS_KEY MSISDN
                    ,  (
                        CASE
                            WHEN CURRENT_STATUS IN ('a', 's')  THEN 'ACTIF'
                            ELSE 'INACT'
                        END 
                    ) ACCOUNT_STATUS
                FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                WHERE EVENT_DATE= '###SLICE_VALUE###'
                    AND (NVL(BSCS_ACTIVATION_DATE, ACTIVATION_DATE) <= '###SLICE_VALUE###' )
                    AND NVL(OSP_CONTRACT_TYPE, 'PURE PREPAID') = 'PURE POSTPAID'
            ) G00
            WHERE ACCOUNT_STATUS = 'ACTIF'
        ) G0
        LEFT JOIN TMP.TT_SITE_360_1 G1 ON G0.MSISDN = G1.MSISDN
        GROUP BY CI
    ) G ON A.CI = G.CI
    LEFT JOIN
    (
        SELECT
            CI
            , COUNT(*) PARC_ACTIF_PERIOD
        FROM
        (
            SELECT MSISDN
            FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
            WHERE EVENT_DATE = '###SLICE_VALUE###' AND DATEDIFF('###SLICE_VALUE###', OG_CALL) < 30
        ) H0
        LEFT JOIN TMP.TT_SITE_360_1 H1 ON H0.MSISDN = H1.MSISDN
        GROUP BY CI
    ) H ON A.CI = H.CI
    LEFT JOIN
    (
        SELECT
            CI
            , COUNT(*) PARC_OM
        FROM
        (
            SELECT
                MSISDN
            FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
            WHERE EVENT_DATE = '###SLICE_VALUE###' AND UPPER(USER_TYPE) = 'SUBSCRIBER'
        ) I0
        LEFT JOIN TMP.TT_SITE_360_1 I1 ON I0.MSISDN = I1.MSISDN
        GROUP BY CI
    ) I ON A.CI = I.CI
    LEFT JOIN
    (
        SELECT
            CI
            , COUNT(*) OM_REFILL_COUNT
            , SUM(NVL(TRANSACTION_AMOUNT, 0)) OM_REFILL_AMOUNT
        FROM
        (
            SELECT
                SENDER_MSISDN
                , TRANSACTION_AMOUNT
            FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
            WHERE TRANSFER_DATETIME = '###SLICE_VALUE###'
                AND SERVICE_TYPE = 'RC'
                AND TRANSFER_STATUS = 'TS'
        ) J0
        LEFT JOIN TMP.TT_SITE_360_1 J1 ON J0.SENDER_MSISDN = J1.MSISDN
        GROUP BY CI
    ) J ON A.CI = J.CI
    LEFT JOIN
    (
        SELECT
            CI
            , SUM(NVL(SERVICE_CHARGE_RECEIVED, 0)) REVENU_OM
        FROM
        (
            SELECT
                SENDER_MSISDN
                , SERVICE_CHARGE_RECEIVED
            FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
            WHERE TRANSFER_DATETIME = '###SLICE_VALUE###'
                AND TRANSFER_STATUS = 'TS'
                AND SENDER_CATEGORY_CODE = 'SUBS'
        ) K0
        LEFT JOIN TMP.TT_SITE_360_1 K1 ON K0.SENDER_MSISDN = K1.MSISDN
        GROUP BY CI
    ) K ON A.CI = K.CI
    LEFT JOIN
    (
        SELECT
            CI
            , SUM(NVL(RATED_AMOUNT, 0)) DATA_VIA_OM
        FROM
        (
            SELECT
                SERVED_PARTY_MSISDN
                , RATED_AMOUNT
            FROM MON.SPARK_FT_SUBSCRIPTION
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                AND SUBSCRIPTION_CHANNEL = '32'
        ) L0
        LEFT JOIN TMP.TT_SITE_360_1 L1 ON L0.SERVED_PARTY_MSISDN = L1.MSISDN
        GROUP BY CI
    ) L ON A.CI = L.CI
) T