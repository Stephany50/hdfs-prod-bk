INSERT INTO MON.SPARK_FT_SITE_360
SELECT
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
    TOTAL_VOICE_REVENUE,
    TOTAL_SMS_REVENUE,
    NULL TOTAL_SUBS_REVENUE,
    ROAM_IN_VOICE_REVENUE,
    ROAM_OUT_VOICE_REVENUE,
    ROAM_IN_SMS_REVENUE,
    ROAM_OUT_SMS_REVENUE,
    ROAM_DATA_REVENUE,
    MAIN_RATED_TEL_AMOUNT,
    MAIN_RATED_TEL_OCM_AMOUNT,
    MAIN_RATED_TEL_MTN_AMOUNT,
    MAIN_RATED_TEL_NEXTTEL_AMOUNT,
    MAIN_RATED_TEL_CAMTEL_AMOUNT,
    MAIN_RATED_TEL_SET_AMOUNT,
    MAIN_RATED_TEL_ROAM_IN_AMOUNT,
    MAIN_RATED_TEL_ROAM_OUT_AMOUNT,
    MAIN_RATED_TEL_SVA_AMOUNT,
    MAIN_RATED_TEL_INT_AMOUNT,
    MAIN_RATED_SMS_AMOUNT,
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

    OG_RATED_CALL_DURATION,
    OG_TOTAL_CALL_DURATION,
    RATED_TEL_OCM_DURATION,
    RATED_TEL_MTN_DURATION,
    RATED_TEL_NEXTTEL_DURATION,
    RATED_TEL_CAMTEL_DURATION,
    RATED_TEL_SET_DURATION,
    RATED_TEL_ROAM_IN_DURATION,
    RATED_TEL_ROAM_OUT_DURATION,
    RATED_TEL_SVA_DURATION,
    RATED_TEL_INT_DURATION,

    OG_SMS_TOTAL_COUNT,
    OG_SMS_OCM_COUNT,
    OG_SMS_MTN_COUNT,
    OG_SMS_NEXTTEL_COUNT,
    OG_SMS_CAMTEL_COUNT,
    OG_SMS_SET_COUNT,
    OG_SMS_ROAM_IN_COUNT,
    OG_SMS_ROAM_OUT_COUNT,
    OG_SMS_SVA_COUNT,
    OG_SMS_INTERNATIONAL_COUNT,

    DATA_BYTES_RECEIVED,
    DATA_BYTES_SENT,
    DATA_BYTES_USED_IN_BUNDLE_ROAM,
    DATA_BYTES_USED_PAYGO_ROAM,

    CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        A.LOC_SITE_NAME,
        A.LOC_TOWN_NAME,
        A.LOC_ADMINISTRATIVE_REGION,
        A.LOC_COMMERCIAL_REGION,
        A.LOC_ZONE_PMO,
        A.LOC_QUARTIER,
        A.LOC_ARRONDISSEMENT,
        A.LOC_DEPARTEMENT,
        A.LOC_SECTOR,
        B.DATA_USERS,
        B.DATA_MAIN_RATED_AMOUNT,
        B.ROAM_DATA_REVENUE,
        B.DATA_GOS_MAIN_RATED_AMOUNT,
        B.DATA_ROAM_MAIN_RATED_AMOUNT,
        B.DATA_BYTES_SENT,
        B.DATA_BYTES_RECEIVED,
        B.DATA_BYTES_USED_IN_BUNDLE_ROAM,
        B.DATA_BYTES_USED_PAYGO_ROAM,
        B.TOTAL_VOICE_REVENUE,
        B.TOTAL_VOICE_DURATION,
        B.TOTAL_SMS_REVENUE,
        B.ROAM_IN_VOICE_REVENUE,
        B.ROAM_OUT_VOICE_REVENUE,
        B.ROAM_IN_SMS_REVENUE,
        B.ROAM_OUT_SMS_REVENUE,
        B.MAIN_RATED_TEL_AMOUNT,
        B.MAIN_RATED_TEL_ROAM_IN_AMOUNT,
        B.MAIN_RATED_TEL_ROAM_OUT_AMOUNT,
        B.MAIN_RATED_TEL_SVA_AMOUNT,
        B.MAIN_RATED_TEL_INT_AMOUNT,
        B.MAIN_RATED_SMS_AMOUNT,
        B.OG_RATED_CALL_DURATION,
        B.OG_TOTAL_CALL_DURATION,
        B.RATED_TEL_OCM_DURATION,
        B.RATED_TEL_MTN_DURATION,
        B.RATED_TEL_NEXTTEL_DURATION,
        B.RATED_TEL_CAMTEL_DURATION,
        B.RATED_TEL_SET_DURATION,
        B.RATED_TEL_ROAM_IN_DURATION,
        B.RATED_TEL_ROAM_OUT_DURATION,
        B.RATED_TEL_SVA_DURATION,
        B.RATED_TEL_INT_DURATION,
        B.OG_SMS_TOTAL_COUNT,
        B.OG_SMS_OCM_COUNT,
        B.OG_SMS_MTN_COUNT,
        B.OG_SMS_NEXTTEL_COUNT,
        B.OG_SMS_CAMTEL_COUNT,
        B.OG_SMS_SET_COUNT,
        B.OG_SMS_ROAM_IN_COUNT,
        B.OG_SMS_ROAM_OUT_COUNT,
        B.OG_SMS_SVA_COUNT,
        B.OG_SMS_INTERNATIONAL_COUNT,
        B.MAIN_RATED_TEL_MTN_AMOUNT,
        B.MAIN_RATED_TEL_CAMTEL_AMOUNT,
        B.MAIN_RATED_TEL_NEXTTEL_AMOUNT,
        B.MAIN_RATED_TEL_OCM_AMOUNT,
        B.MAIN_RATED_TEL_SET_AMOUNT,
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
            SITE_NAME LOC_SITE_NAME
            , MAX(TOWNNAME) LOC_TOWN_NAME
            , MAX(REGION) LOC_ADMINISTRATIVE_REGION
            , MAX(COMMERCIAL_REGION) LOC_COMMERCIAL_REGION
            , MAX(ZONEPMO) LOC_ZONE_PMO
            , MAX(QUARTIER) LOC_QUARTIER
            , MAX(ARRONDISSEMENT) LOC_ARRONDISSEMENT
            , MAX(DEPARTEMENT) LOC_DEPARTEMENT
            , MAX(SECTEUR) LOC_SECTOR
        FROM DIM.DT_GSM_CELL_CODE
        GROUP BY SITE_NAME
    ) A
    FULL JOIN
    ( -- RECUPÉRATION DANS CELL 360
        SELECT
            B0.*,
            B1.SITE_NAME
        FROM
        (
            SELECT *
            FROM MON.SPARK_FT_CELL_360
            WHERE EVENT_DATE = '###SLICE_VALUE###'
        ) B0
        LEFT JOIN DIM.DT_GSM_CELL_CODE B1
        ON B0.CI = B1.CI
    ) B ON A.LOC_SITE_NAME = B.SITE_NAME
    FULL JOIN
    (
        SELECT
            NVL(SUM(C2S_REFILL_COUNT), 0) C2S_REFILL_COUNT,
            NVL(SUM(C2S_MAIN_REFILL_AMOUNT), 0) C2S_MAIN_REFILL_AMOUNT,
            NVL(SUM(C2S_PROMO_REFILL_AMOUNT), 0) C2S_PROMO_REFILL_AMOUNT,
            NVL(SUM(P2P_REFILL_COUNT), 0) P2P_REFILL_COUNT,
            NVL(SUM(P2P_REFILL_AMOUNT) ,0) P2P_REFILL_AMOUNT,
            NVL(SUM(SCRATCH_REFILL_COUNT), 0) SCRATCH_REFILL_COUNT,
            NVL(SUM(SCRATCH_MAIN_REFILL_AMOUNT), 0) SCRATCH_MAIN_REFILL_AMOUNT,
            NVL(SUM(SCRATCH_PROMO_REFILL_AMOUNT), 0) SCRATCH_PROMO_REFILL_AMOUNT,
            NVL(SUM(P2P_REFILL_FEES), 0) P2P_REFILL_FEES,
            NVL((
                CASE
                    WHEN SUM(SCRATCH_REFILL_COUNT) = 0 THEN COUNT(DISTINCT MSISDN_CALL_BOX)
                    ELSE COUNT(DISTINCT MSISDN_CALL_BOX) - 1 --- CAR SI LE SCRATCH_REFILL_COUNT EST DIFFERENT DE 0 L'ON A FORCÉMENT AU MOINS UNE RECHARGE PAR CARTE DONC LE DISTINCT VA RENVOYER AUSSI NULL
                END
            ), 0) NBRE_CALL_BOX,
            SITE_NAME
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
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) C1 ON C0.SENDER_MSISDN = C1.MSISDN
        GROUP BY SITE_NAME
    ) C ON A.LOC_SITE_NAME = C.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME,
            NVL(SUM(
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
            ), 0) GROSS_ADD
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
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) D2 ON D1.IDENTIFICATEUR = D2.MSISDN
        GROUP BY SITE_NAME
    ) D ON A.LOC_SITE_NAME = D.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(COUNT(*), 0) PARC_GROUPE
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
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F1 ON F0.MSISDN = F1.MSISDN
        GROUP BY SITE_NAME
    ) F ON A.LOC_SITE_NAME = F.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(COUNT(*), 0) PARC_ART
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
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) G1 ON G0.MSISDN = G1.MSISDN
        GROUP BY SITE_NAME
    ) G ON A.LOC_SITE_NAME = G.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(COUNT(*), 0) PARC_ACTIF_PERIOD
        FROM
        (
            SELECT MSISDN
            FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
            WHERE EVENT_DATE = '###SLICE_VALUE###' AND DATEDIFF('###SLICE_VALUE###', OG_CALL) < 30
        ) H0
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) H1 ON H0.MSISDN = H1.MSISDN
        GROUP BY SITE_NAME
    ) H ON A.LOC_SITE_NAME = H.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(COUNT(*), 0) PARC_OM
        FROM
        (
            SELECT
                MSISDN
            FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
            WHERE EVENT_DATE = '###SLICE_VALUE###' AND UPPER(USER_TYPE) = 'SUBSCRIBER'
        ) I0
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) I1 ON I0.MSISDN = I1.MSISDN
        GROUP BY SITE_NAME
    ) I ON A.LOC_SITE_NAME = I.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(COUNT(*), 0) OM_REFILL_COUNT
            , NVL(SUM(NVL(TRANSACTION_AMOUNT, 0)), 0.0) OM_REFILL_AMOUNT
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
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) J1 ON J0.SENDER_MSISDN = J1.MSISDN
        GROUP BY SITE_NAME
    ) J ON A.LOC_SITE_NAME = J.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(SUM(NVL(SERVICE_CHARGE_RECEIVED, 0)), 0) REVENU_OM
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
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) K1 ON K0.SENDER_MSISDN = K1.MSISDN
        GROUP BY SITE_NAME
    ) K ON A.LOC_SITE_NAME = K.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(SUM(NVL(RATED_AMOUNT, 0)), 0) DATA_VIA_OM
        FROM
        (
            SELECT
                SERVED_PARTY_MSISDN
                , RATED_AMOUNT
            FROM MON.SPARK_FT_SUBSCRIPTION
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                AND SUBSCRIPTION_CHANNEL = '32'
        ) L0
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) L1 ON L0.SERVED_PARTY_MSISDN = L1.MSISDN
        GROUP BY SITE_NAME
    ) L ON A.LOC_SITE_NAME = L.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , COUNT(*) RUPTURE_STOCK
        FROM
        (
            SELECT SENDER_MSISDN
            FROM
            (
                SELECT
                    SENDER_MSISDN,
                    FIRST_VALUE(SENDER_POST_BAL) OVER(PARTITION BY SENDER_MSISDN ORDER BY REFILL_TIME DESC) LAST_STOCK
                FROM MON.SPARK_FT_REFILL M001
                WHERE M001.REFILL_DATE = '###SLICE_VALUE###'
                    AND M001.TERMINATION_IND='200'
            ) M00
            WHERE LAST_STOCK >= 1000
        ) M0
        LEFT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) M1 ON M0.SENDER_MSISDN = M1.MSISDN
        GROUP BY SITE_NAME
    ) M ON A.LOC_SITE_NAME = M.SITE_NAME
) T