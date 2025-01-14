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
    VOICE_USERS,

    GROSS_ADD,

    NBRE_CALL_BOX,
    RUPTURE_STOCK,

    NBRE_FAMOCO,

    SMARTPHONES_3G,
    SMARTPHONES_4G,

    TOTAL_REVENUE,
    TOTAL_VOICE_REVENUE,
    TOTAL_SMS_REVENUE,
    TOTAL_SUBS_REVENUE,
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
    AMOUNT_EMERGENCY_DATA,
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
    DATA_REFILL_FEES,
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
    DATA_BYTES_USED_OUT_BUNDLE_ROAM,
    DATA_BYTES_USED_IN_BUNDLE,
    DATA_PROMO_RATED_AMOUNT,
    DATA_ROAM_PROMO_RATED_AMOUNT,

    CURRENT_TIMESTAMP() INSERT_DATE,
    CATEGORY_SITE,
    REVENU_SUBS_VOIX,
    REVENU_SUBS_DATA,
    REVENU_SUBS_SMS,
    PDM_OCM,
    PDM_MTN,
    PDM_CAMTEL,
    PDM_NEXTTEL,
    PDM_NATIONAL,
    PARC_ACTIF_OM,
    --REVENU_SUBS_DATA_COMBO,
    --REVENU_SUBS_DATA_PUR,
    --REVENU_SUBS_VOIX_COMBO,
    --REVENU_SUBS_VOIX_PUR,
    --REVENU_SUBS_SMS_COMBO,
    --REVENU_SUBS_SMS_PUR,
    --RGS_1_1M,
    --RGS_1_5M,
    --RGS_10_1M,
    --RGS_10_5M,
    --RGS_20_1M,
    --RGS_20_5M,
    --RGS_30_1M,
    --RGS_30_5M,
    --RGS_60_1M,
    --RGS_60_5M,
    --RGS_90_1M,
    --RGS_90_5M,
    --VOICE_USER_ONLY,
    --SMS_USER_ONLY,
    --VOICE_DATA_USER,
    --DATA_SMS_USER,
    --VOICE_DATA_SMS_USER,
    --INC_RATED_TEL_MTN_DURATION,
    --INC_RATED_TEL_NEXTTEL_DURATION,
    --INC_RATED_TEL_CAMTEL_DURATION,
    --INC_RATED_TEL_INT_DURATION,
    --TOTAL_INC_RATED_TEL_DURATION,
    --INC_SMS_MTN_COUNT,
    --INC_SMS_NEXTTEL_COUNT,
    --INC_SMS_CAMTEL_COUNT,
    --INC_SMS_INTERNATIONAL_COUNT,
    --TOTAL_INC_SMS_COUNT,
    --DATE_FORMAT('###SLICE_VALUE###', 'YYYY-MM') EVENT_MONTH,
    --WEEKOFYEAR('###SLICE_VALUE###') EVENT_WEEK,
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
        A.CATEGORY_SITE,
        NVL(B.TOTAL_VOICE_REVENUE, 0) + NVL(B.TOTAL_SMS_REVENUE, 0) + NVL(N.TOTAL_SUBS_REVENUE, 0) + NVL(B.DATA_MAIN_RATED_AMOUNT, 0) AS TOTAL_REVENUE,
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
        B.DATA_PROMO_RATED_AMOUNT,
        B.DATA_BYTES_USED_IN_BUNDLE,
        B.DATA_BYTES_USED_OUT_BUNDLE_ROAM,
        B.DATA_ROAM_PROMO_RATED_AMOUNT,
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
        L.DATA_VIA_OM,
        M.RUPTURE_STOCK,
        N.TOTAL_SUBS_REVENUE,
        O.DATA_USERS,
        P.VOICE_USERS,
        Q.NBRE_FAMOCO,
        R.SMARTPHONES_3G,
        R.SMARTPHONES_4G,
        S.AMOUNT_EMERGENCY_DATA,
        T.DATA_REFILL_FEES,
        U.REVENU_SUBS_VOIX,
        U.REVENU_SUBS_DATA,
        U.REVENU_SUBS_SMS,
        U.REVENU_SUBS_DATA_COMBO,
        U.REVENU_SUBS_DATA_PUR,
        U.REVENU_SUBS_VOIX_COMBO,
        U.REVENU_SUBS_VOIX_PUR,
        U.REVENU_SUBS_SMS_COMBO,
        U.REVENU_SUBS_SMS_PUR,
        V.PDM_OCM,
        V.PDM_MTN,
        V.PDM_CAMTEL,
        V.PDM_NEXTTEL,
        V.PDM_NATIONAL,
        W.PARC_ACTIF_OM
        --X.RGS_1_1M,
        --X.RGS_1_5M,
        --X.RGS_10_1M,
        --X.RGS_10_5M,
        --X.RGS_20_1M,
        --X.RGS_20_5M,
        --X.RGS_30_1M,
        --X.RGS_30_5M,
        --X.RGS_60_1M,
        --X.RGS_60_5M,
        --X.RGS_90_1M,
        --X.RGS_90_5M,
        --Y.VOICE_USER_ONLY,
        --Y.SMS_USER_ONLY,
        --Y.VOICE_DATA_USER,
        --Y.DATA_SMS_USER,
        --Y.VOICE_DATA_SMS_USER,
        --Z.INC_RATED_TEL_MTN_DURATION,
        --Z.INC_RATED_TEL_NEXTTEL_DURATION,
        --Z.INC_RATED_TEL_CAMTEL_DURATION,
        --Z.INC_RATED_TEL_INT_DURATION,
        --Z.TOTAL_INC_RATED_TEL_DURATION,
        --Z.INC_SMS_MTN_COUNT,
        --Z.INC_SMS_NEXTTEL_COUNT,
        --Z.INC_SMS_CAMTEL_COUNT,
        --Z.INC_SMS_INTERNATIONAL_COUNT,
        --Z.TOTAL_INC_SMS_COUNT
    FROM
    (
        SELECT
            NVL(A0.LOC_SITE_NAME, A1.LOC_SITE_NAME) LOC_SITE_NAME,
            NVL(A0.LOC_ADMINISTRATIVE_REGION, A1.LOC_ADMINISTRATIVE_REGION) LOC_ADMINISTRATIVE_REGION,
            A0.LOC_COMMERCIAL_REGION,
            A0.LOC_ZONE_PMO,
            A0.LOC_QUARTIER,
            A0.LOC_ARRONDISSEMENT,
            A0.LOC_DEPARTEMENT,
            A0.LOC_SECTOR,
            A0.LOC_TOWN_NAME,
            NVL(A0.CATEGORY_SITE, A1.CATEGORY_SITE) CATEGORY_SITE
        FROM
        (
            SELECT
                UPPER(SITE_NAME) LOC_SITE_NAME
                , MAX(TOWNNAME) LOC_TOWN_NAME
                , MAX(REGION) LOC_ADMINISTRATIVE_REGION
                , MAX(COMMERCIAL_REGION) LOC_COMMERCIAL_REGION
                , MAX(ZONEPMO) LOC_ZONE_PMO
                , MAX(QUARTIER) LOC_QUARTIER
                , MAX(ARRONDISSEMENT) LOC_ARRONDISSEMENT
                , MAX(DEPARTEMENT) LOC_DEPARTEMENT
                , MAX(SECTEUR) LOC_SECTOR
                , MAX(CATEGORIE_SITE) CATEGORY_SITE
            FROM DIM.SPARK_DT_GSM_CELL_CODE
            GROUP BY UPPER(SITE_NAME)
        ) A0
        FULL JOIN
        (
            SELECT
                UPPER(SITE_NAME) LOC_SITE_NAME
                , (
                    CASE
                        WHEN MAX(REGION) = 'ADM' THEN 'Adamaoua'
                        WHEN MAX(REGION) = 'EXN' THEN 'Extreme-Nord'
                        WHEN MAX(REGION) = 'NRD' THEN 'Nord'
                        WHEN MAX(REGION) = 'NRO' THEN 'Nord-Ouest'
                        WHEN MAX(REGION) = 'SUD' THEN 'Sud'
                        WHEN MAX(REGION) = 'CTR' THEN 'Centre'
                        WHEN MAX(REGION) = 'EST' THEN 'Est'
                        WHEN MAX(REGION) = 'LIT' THEN 'Littoral'
                        WHEN MAX(REGION) = 'OST' THEN 'Ouest'
                        ELSE MAX(REGION)
                    END
                ) LOC_ADMINISTRATIVE_REGION
                , 'AMN' CATEGORY_SITE
            FROM DIM.DT_CI_LAC_SITE_AMN
            GROUP BY UPPER(SITE_NAME)
        ) A1 ON A0.LOC_SITE_NAME = A1.LOC_SITE_NAME
    ) A
    FULL JOIN
    (
        SELECT
            SUM(NVL(B0.TOTAL_VOICE_REVENUE, 0)) TOTAL_VOICE_REVENUE,
            SUM(NVL(B0.TOTAL_VOICE_DURATION, 0)) TOTAL_VOICE_DURATION,
            SUM(NVL(B0.TOTAL_SMS_REVENUE, 0)) TOTAL_SMS_REVENUE,
            SUM(NVL(B0.ROAM_IN_VOICE_REVENUE, 0)) ROAM_IN_VOICE_REVENUE,
            SUM(NVL(B0.ROAM_OUT_VOICE_REVENUE, 0)) ROAM_OUT_VOICE_REVENUE,
            SUM(NVL(B0.ROAM_IN_SMS_REVENUE, 0)) ROAM_IN_SMS_REVENUE,
            SUM(NVL(B0.ROAM_OUT_SMS_REVENUE, 0)) ROAM_OUT_SMS_REVENUE,
            SUM(NVL(B0.ROAM_DATA_REVENUE, 0)) ROAM_DATA_REVENUE,
            SUM(NVL(B0.MAIN_RATED_TEL_AMOUNT, 0)) MAIN_RATED_TEL_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_OCM_AMOUNT, 0)) MAIN_RATED_TEL_OCM_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_MTN_AMOUNT, 0)) MAIN_RATED_TEL_MTN_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_NEXTTEL_AMOUNT, 0)) MAIN_RATED_TEL_NEXTTEL_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_CAMTEL_AMOUNT, 0)) MAIN_RATED_TEL_CAMTEL_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_SET_AMOUNT, 0)) MAIN_RATED_TEL_SET_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_ROAM_IN_AMOUNT, 0)) MAIN_RATED_TEL_ROAM_IN_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_ROAM_OUT_AMOUNT, 0)) MAIN_RATED_TEL_ROAM_OUT_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_SVA_AMOUNT, 0)) MAIN_RATED_TEL_SVA_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_TEL_INT_AMOUNT, 0)) MAIN_RATED_TEL_INT_AMOUNT,
            SUM(NVL(B0.MAIN_RATED_SMS_AMOUNT, 0)) MAIN_RATED_SMS_AMOUNT,
            SUM(NVL(B0.DATA_MAIN_RATED_AMOUNT, 0)) DATA_MAIN_RATED_AMOUNT,
            SUM(NVL(B0.DATA_GOS_MAIN_RATED_AMOUNT, 0)) DATA_GOS_MAIN_RATED_AMOUNT,
            SUM(NVL(B0.DATA_ROAM_MAIN_RATED_AMOUNT, 0)) DATA_ROAM_MAIN_RATED_AMOUNT,
            SUM(NVL(B0.OG_RATED_CALL_DURATION, 0)) OG_RATED_CALL_DURATION,
            SUM(NVL(B0.OG_TOTAL_CALL_DURATION, 0)) OG_TOTAL_CALL_DURATION,
            SUM(NVL(B0.RATED_TEL_OCM_DURATION, 0)) RATED_TEL_OCM_DURATION,
            SUM(NVL(B0.RATED_TEL_MTN_DURATION, 0)) RATED_TEL_MTN_DURATION,
            SUM(NVL(B0.RATED_TEL_NEXTTEL_DURATION, 0)) RATED_TEL_NEXTTEL_DURATION,
            SUM(NVL(B0.RATED_TEL_CAMTEL_DURATION, 0)) RATED_TEL_CAMTEL_DURATION,
            SUM(NVL(B0.RATED_TEL_SET_DURATION, 0)) RATED_TEL_SET_DURATION,
            SUM(NVL(B0.RATED_TEL_ROAM_IN_DURATION, 0)) RATED_TEL_ROAM_IN_DURATION,
            SUM(NVL(B0.RATED_TEL_ROAM_OUT_DURATION, 0)) RATED_TEL_ROAM_OUT_DURATION,
            SUM(NVL(B0.RATED_TEL_SVA_DURATION, 0)) RATED_TEL_SVA_DURATION,
            SUM(NVL(B0.RATED_TEL_INT_DURATION, 0)) RATED_TEL_INT_DURATION,
            SUM(NVL(B0.OG_SMS_TOTAL_COUNT, 0)) OG_SMS_TOTAL_COUNT,
            SUM(NVL(B0.OG_SMS_OCM_COUNT, 0)) OG_SMS_OCM_COUNT,
            SUM(NVL(B0.OG_SMS_MTN_COUNT, 0)) OG_SMS_MTN_COUNT,
            SUM(NVL(B0.OG_SMS_NEXTTEL_COUNT, 0)) OG_SMS_NEXTTEL_COUNT,
            SUM(NVL(B0.OG_SMS_CAMTEL_COUNT, 0)) OG_SMS_CAMTEL_COUNT,
            SUM(NVL(B0.OG_SMS_SET_COUNT, 0)) OG_SMS_SET_COUNT,
            SUM(NVL(B0.OG_SMS_ROAM_IN_COUNT, 0)) OG_SMS_ROAM_IN_COUNT,
            SUM(NVL(B0.OG_SMS_ROAM_OUT_COUNT, 0)) OG_SMS_ROAM_OUT_COUNT,
            SUM(NVL(B0.OG_SMS_SVA_COUNT, 0)) OG_SMS_SVA_COUNT,
            SUM(NVL(B0.OG_SMS_INTERNATIONAL_COUNT, 0)) OG_SMS_INTERNATIONAL_COUNT,
            SUM(NVL(B0.DATA_BYTES_RECEIVED, 0)) DATA_BYTES_RECEIVED,
            SUM(NVL(B0.DATA_BYTES_SENT, 0)) DATA_BYTES_SENT,
            SUM(NVL(B0.DATA_BYTES_USED_IN_BUNDLE_ROAM, 0)) DATA_BYTES_USED_IN_BUNDLE_ROAM,
            SUM(NVL(B0.DATA_BYTES_USED_PAYGO_ROAM, 0)) DATA_BYTES_USED_PAYGO_ROAM,
            SUM(NVL(B0.DATA_BYTES_USED_OUT_BUNDLE_ROAM, 0)) DATA_BYTES_USED_OUT_BUNDLE_ROAM,
            SUM(NVL(B0.DATA_BYTES_USED_IN_BUNDLE, 0)) DATA_BYTES_USED_IN_BUNDLE,
            SUM(NVL(B0.DATA_PROMO_RATED_AMOUNT, 0)) DATA_PROMO_RATED_AMOUNT,
            SUM(NVL(B0.DATA_ROAM_PROMO_RATED_AMOUNT, 0)) DATA_ROAM_PROMO_RATED_AMOUNT,
            B1.SITE_NAME
        FROM
        (
            SELECT *
            FROM MON.SPARK_FT_CELL_360
            WHERE EVENT_DATE = '###SLICE_VALUE###'
        ) B0
        LEFT JOIN
        (
            SELECT
                NVL(B10.CI, B11.CI) CI,
                NVL(B10.lac, B11.lac) lac,
                UPPER(NVL(B10.SITE_NAME, B11.SITE_NAME)) SITE_NAME
            FROM
            (
                SELECT
                    cast(ci as int) CI
                    , cast(lac as int) lac
                    , MAX(SITE_NAME) SITE_NAME
                FROM DIM.SPARK_DT_GSM_CELL_CODE
                GROUP BY cast(ci as int), cast(lac as int)
            ) B10
            FULL JOIN
            (
                SELECT
                    cast(ci as int) CI,
                    cast(lac as int) lac,
                    MAX(SITE_NAME) SITE_NAME
                FROM DIM.DT_CI_LAC_SITE_AMN
                GROUP BY cast(ci as int), cast(lac as int)
            ) B11
            ON B10.CI = B11.CI and B10.lac = B11.lac
        ) B1
        ON B0.CI = B1.CI and B0.lac = B1.lac
        GROUP BY B1.SITE_NAME
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
                    WHEN SUM(SCRATCH_REFILL_COUNT) = 0 THEN COUNT(MSISDN_CALL_BOX)
                    ELSE COUNT(MSISDN_CALL_BOX) - 1
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
                NVL(C00.SENDER_MSISDN, C01.SENDER_MSISDN) SENDER_MSISDN
            FROM
            (
                SELECT
                    C001.SENDER_MSISDN
                    , SUM(CASE WHEN C001.REFILL_MEAN='C2S' THEN 1 ELSE 0 END) C2S_REFILL_COUNT
                    , SUM(CASE WHEN C001.REFILL_MEAN='C2S' THEN C001.REFILL_AMOUNT ELSE 0 END) C2S_MAIN_REFILL_AMOUNT
                    , SUM(CASE WHEN C001.REFILL_MEAN='C2S' THEN C001.REFILL_BONUS ELSE 0 END) C2S_PROMO_REFILL_AMOUNT
                    , SUM(CASE WHEN C001.REFILL_MEAN='SCRATCH' THEN 1 ELSE 0 END) SCRATCH_REFILL_COUNT
                    , SUM(CASE WHEN C001.REFILL_MEAN='SCRATCH' THEN C001.REFILL_AMOUNT ELSE 0 END) SCRATCH_MAIN_REFILL_AMOUNT
                    , SUM(CASE WHEN C001.REFILL_MEAN='SCRATCH' THEN C001.REFILL_BONUS ELSE 0 END) SCRATCH_PROMO_REFILL_AMOUNT
                FROM MON.SPARK_FT_REFILL C001
                WHERE C001.REFILL_DATE = '###SLICE_VALUE###'
                    AND C001.TERMINATION_IND='200'
                GROUP BY SENDER_MSISDN
            ) C00
            FULL JOIN
            (
                SELECT
                    C011.SENDER_MSISDN
                    , COUNT(*) P2P_REFILL_COUNT
                    , SUM(C011.TRANSFER_AMT) P2P_REFILL_AMOUNT
                    , SUM(C011.TRANSFER_FEES) P2P_REFILL_FEES
                FROM MON.SPARK_FT_CREDIT_TRANSFER C011
                WHERE C011.REFILL_DATE = '###SLICE_VALUE###'
                    AND C011.TERMINATION_IND='000'
                GROUP BY SENDER_MSISDN
            ) C01 ON C00.SENDER_MSISDN = C01.SENDER_MSISDN
        ) C0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 C1
        ON C0.SENDER_MSISDN = C1.MSISDN
        GROUP BY SITE_NAME
    ) C ON A.LOC_SITE_NAME = C.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , COUNT(DISTINCT SERVED_PARTY_MSISDN) GROSS_ADD
        FROM
        (
            SELECT *
            FROM MON.SPARK_FT_SUBSCRIPTION
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                AND UPPER(SUBSCRIPTION_SERVICE) LIKE '%PPS%'
        ) D0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 D1
        ON D0.SERVED_PARTY_MSISDN = D1.MSISDN
        GROUP BY SITE_NAME
        --SELECT
        --    SITE_NAME,
        --    NVL(SUM(
        --        CASE WHEN NVL(ACTIVATION_DATE, BSCS_ACTIVATION_DATE) = '###SLICE_VALUE###' AND
        --        (
        --            CASE WHEN NVL(OSP_STATUS, CURRENT_STATUS)='ACTIVE' THEN 'ACTIF'
        --            WHEN NVL(OSP_STATUS, CURRENT_STATUS)='a' THEN 'ACTIF'
        --            WHEN NVL(OSP_STATUS, CURRENT_STATUS)='d' THEN 'DEACT'
        --            WHEN NVL(OSP_STATUS, CURRENT_STATUS)='s' THEN 'INACT'
        --            WHEN NVL(OSP_STATUS, CURRENT_STATUS)='DEACTIVATED' THEN 'DEACT'
        --            WHEN NVL(OSP_STATUS, CURRENT_STATUS)='INACTIVE' THEN 'INACT'
        --            WHEN NVL(OSP_STATUS, CURRENT_STATUS)='VALID' THEN 'VALIDE'
        --            ELSE NVL(OSP_STATUS, CURRENT_STATUS)
        --            END
        --        ) IN ('ACTIF', 'INACT') THEN 1
        --        ELSE 0
        --        END
        --    ), 0) GROSS_ADD
        --FROM
        --(
        --    SELECT
        --        ACTIVATION_DATE,
        --        BSCS_ACTIVATION_DATE,
        --        OSP_STATUS,
        --        CURRENT_STATUS,
        --        ACCESS_KEY
        --    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        --    WHERE
        --        EVENT_DATE = DATE_SUB('###SLICE_VALUE###', -1)
        --        AND (NVL(ACTIVATION_DATE, BSCS_ACTIVATION_DATE) <= '###SLICE_VALUE###')
        --) D0
        --LEFT JOIN
        --(
        --    SELECT
        --        MSISDN,
        --        MAX(IDENTIFICATEUR) IDENTIFICATEUR
        --    FROM DIM.SPARK_DT_BASE_IDENTIFICATION
        --    GROUP BY MSISDN
        --) D1 ON D0.ACCESS_KEY = D1.MSISDN
        --LEFT JOIN TMP.SPARK_TMP_SITE_360 D2
        --ON D1.IDENTIFICATEUR = D2.MSISDN
        --GROUP BY SITE_NAME
    ) D ON A.LOC_SITE_NAME = D.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(SUM(effectif), 0) PARC_GROUPE
        FROM
        (
            select *
            from MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY
            where event_date=DATE_ADD('###SLICE_VALUE###', 1) and operator_code <> 'SET'
                AND (
                    CASE
                        WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN 1
                        ELSE 0
                    END
                ) = 0
        ) F0
        LEFT JOIN
        (
            SELECT
                NVL(F10.CI, F11.CI) CI,
                UPPER(NVL(F10.SITE_NAME, F11.SITE_NAME)) SITE_NAME
            FROM
            (
                SELECT
                    lpad(CI, 5, 0) ci
                    , MAX(SITE_NAME) SITE_NAME
                FROM DIM.SPARK_DT_GSM_CELL_CODE
                GROUP BY lpad(CI, 5, 0)
            ) F10
            FULL JOIN
            (
                SELECT
                    lpad(CI, 5, 0) CI,
                    MAX(SITE_NAME) SITE_NAME
                FROM DIM.DT_CI_LAC_SITE_AMN
                GROUP BY lpad(CI, 5, 0)
            ) F11
            ON F10.CI = F11.CI
        ) F1 ON lpad(F0.location_ci, 5, 0) = F1.CI
        GROUP BY SITE_NAME
        --FROM
        --(
        --    SELECT
        --        F00.MSISDN
        --    FROM
        --    (
        --        SELECT
        --            UPPER(F000.PROFILE) PROFILE
        --            , NVL (F001.GP_STATUS, 'INACT') STATUT
        --            , F000.ACCESS_KEY MSISDN
        --        FROM
        --        (
        --            SELECT
        --                PROFILE
        --                , ACCESS_KEY
        --            FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        --            WHERE EVENT_DATE = '###SLICE_VALUE###'
        --        ) F000
        --        LEFT JOIN (
        --            SELECT
        --                GP_STATUS
        --                , MSISDN
        --            FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
        --            WHERE EVENT_DATE = '###SLICE_VALUE###'
        --        ) F001 ON F000.ACCESS_KEY = F001.MSISDN
        --        UNION ALL
        --        SELECT
        --            UPPER(F002.FORMULE) PROFILE
        --            , NVL(F002.GP_STATUS, 'INACT') STATUT
        --            , F002.MSISDN
        --        FROM
        --        (
        --            SELECT
        --                MSISDN,
        --                FORMULE,
        --                GP_STATUS
        --            FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
        --            WHERE EVENT_DATE = '###SLICE_VALUE###'
        --        ) F002
        --        LEFT JOIN
        --        (
        --            SELECT
        --                F0030.MSISDN
        --            FROM
        --            (
        --                SELECT
        --                    MSISDN
        --                FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
        --                WHERE EVENT_DATE = '###SLICE_VALUE###' AND NVL(GP_STATUS, 'INACT') = 'ACTIF'
        --            ) F0030
        --            LEFT JOIN
        --            (
        --                SELECT
        --                    ACCESS_KEY MSISDN
        --                FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        --                WHERE EVENT_DATE = '###SLICE_VALUE###'
        --            ) F0031 ON F0030.MSISDN = F0031.MSISDN
        --            WHERE F0031.MSISDN IS NULL
        --        ) F003 ON F002.MSISDN = F003.MSISDN
        --        LEFT JOIN DIM.DT_OFFER_PROFILES F004 ON F002.FORMULE = F004.PROFILE_CODE
        --        WHERE NVL(UPPER(F004.CONTRACT_TYPE), 'PURE PREPAID' ) IN ('PURE PREPAID', 'HYBRID')
        --            AND F003.MSISDN IS NOT NULL
        --    ) F00
        --    LEFT JOIN DIM.DT_OFFER_PROFILES F01 ON UPPER(F00.PROFILE) = F01.PROFILE_CODE
        --    WHERE F00.STATUT='ACTIF'
        --        AND NVL(F01.OPERATOR_CODE, 'OCM') <> 'SET'
        --        AND (
        --            CASE
        --                WHEN F00.PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN 1
        --                ELSE 0
        --            END
        --        ) = 0
        --) F0
        --LEFT JOIN TMP.SPARK_TMP_SITE_360 F1
        --ON F0.MSISDN = F1.MSISDN
        --GROUP BY SITE_NAME
    ) F ON A.LOC_SITE_NAME = F.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , NVL(sum(total_count), 0) PARC_ART
        FROM
        (
            select *
            from MON.SPARK_FT_commercial_subscrib_summary
            where datecode='###SLICE_VALUE###'
                AND account_status = 'ACTIF'
        ) G0
        LEFT JOIN
        (
            SELECT
                NVL(G10.CI, G11.CI) CI,
                UPPER(NVL(G10.SITE_NAME, G11.SITE_NAME)) SITE_NAME
            FROM
            (
                SELECT
                    lpad(CI, 5, 0) CI
                    , MAX(SITE_NAME) SITE_NAME
                FROM DIM.SPARK_DT_GSM_CELL_CODE
                GROUP BY lpad(CI, 5, 0)
            ) G10
            FULL JOIN
            (
                SELECT
                    lpad(CI, 5, 0) CI,
                    MAX(SITE_NAME) SITE_NAME
                FROM DIM.DT_CI_LAC_SITE_AMN
                GROUP BY lpad(CI, 5, 0)
            ) G11
            ON G10.CI = G11.CI
        ) G1 ON lpad(G0.location_ci, 5, 0) = G1.CI
        GROUP BY SITE_NAME
        --FROM
        --(
        --    SELECT
        --        MSISDN
        --    FROM
        --    (
        --        SELECT
        --            G000.ACCESS_KEY MSISDN
        --            , G001.COMGP_STATUS ACCOUNT_STATUS
        --        FROM
        --        (
        --            SELECT ACCESS_KEY
        --            FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        --            WHERE EVENT_DATE= '###SLICE_VALUE###'
        --                AND ACTIVATION_DATE <= '###SLICE_VALUE###'
        --                AND NVL(OSP_CONTRACT_TYPE, 'PURE PREPAID') IN ('PURE PREPAID', 'HYBRID')
        --        ) G000
        --        LEFT JOIN
        --        (
        --            SELECT
        --                MSISDN
        --                , COMGP_STATUS
        --            FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
        --            WHERE EVENT_DATE = '###SLICE_VALUE###'
        --        ) G001 ON G000.ACCESS_KEY = G001.MSISDN
        --        UNION ALL
        --        SELECT
        --            MSISDN
        --            , G002.COMGP_STATUS ACCOUNT_STATUS
        --        FROM
        --        (
        --            SELECT
        --                G0020.MSISDN
        --                , ACTIVATION_DATE
        --                , PROFILE
        --                , COMGP_STATUS
        --            FROM
        --            (
        --                SELECT
        --                    MSISDN
        --                    , ACTIVATION_DATE
        --                    , FORMULE PROFILE
        --                    , COMGP_STATUS
        --                FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
        --                WHERE EVENT_DATE = '###SLICE_VALUE###' AND NVL(COMGP_STATUS, 'INACT') = 'ACTIF'
        --            ) G0020
        --            LEFT JOIN
        --            (
        --                SELECT
        --                    ACCESS_KEY MSISDN
        --                FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        --                WHERE EVENT_DATE = '###SLICE_VALUE###'
        --            ) G0021 ON G0020.MSISDN = G0021.MSISDN
        --            WHERE G0021.MSISDN IS NULL
        --        ) G002
        --        LEFT JOIN MON.VW_DT_OFFER_PROFILES G003 ON G002.PROFILE = G003.PROFILE_CODE
        --        WHERE
        --            G002.ACTIVATION_DATE <= '###SLICE_VALUE###'
        --            AND NVL(G003.CONTRACT_TYPE, 'PURE PREPAID') IN ('PURE PREPAID', 'HYBRID')
        --        UNION ALL
        --        SELECT
        --            ACCESS_KEY MSISDN
        --            ,  (
        --                CASE
        --                    WHEN CURRENT_STATUS IN ('a', 's')  THEN 'ACTIF'
        --                    ELSE 'INACT'
        --                END
        --            ) ACCOUNT_STATUS
        --        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        --        WHERE EVENT_DATE= '###SLICE_VALUE###'
        --            AND (NVL(BSCS_ACTIVATION_DATE, ACTIVATION_DATE) <= '###SLICE_VALUE###' )
        --            AND NVL(OSP_CONTRACT_TYPE, 'PURE PREPAID') = 'PURE POSTPAID'
        --    ) G00
        --    WHERE ACCOUNT_STATUS = 'ACTIF'
        --) G0
        --LEFT JOIN TMP.SPARK_TMP_SITE_360 G1
        --ON G0.MSISDN = G1.MSISDN
        --GROUP BY SITE_NAME
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
        LEFT JOIN TMP.SPARK_TMP_SITE_360 H1
        ON H0.MSISDN = H1.MSISDN
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
        LEFT JOIN TMP.SPARK_TMP_SITE_360 I1
        ON I0.MSISDN = I1.MSISDN
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
        LEFT JOIN TMP.SPARK_TMP_SITE_360 J1
        ON J0.SENDER_MSISDN = J1.MSISDN
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
        LEFT JOIN TMP.SPARK_TMP_SITE_360 K1
        ON K0.SENDER_MSISDN = K1.MSISDN
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
                and (subscription_channel='32' or (upper(subscription_channel) like '%GOS%SDP%' and upper(SUBSCRIPTION_SERVICE_DETAILS) like '%MY%WAY%DIGITAL%') )
        ) L0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 L1
        ON L0.SERVED_PARTY_MSISDN = L1.MSISDN
        GROUP BY SITE_NAME
    ) L ON A.LOC_SITE_NAME = L.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , COUNT(DISTINCT SENDER_MSISDN) RUPTURE_STOCK
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
                    AND M001.REFILL_MEAN = 'C2S'
            ) M00
            WHERE LAST_STOCK <= 1000
        ) M0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 M1
        ON M0.SENDER_MSISDN = M1.MSISDN
        GROUP BY SITE_NAME
    ) M ON A.LOC_SITE_NAME = M.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME,
            SUM(NVL(TOTAL_SUBS_AMOUNT, 0)) TOTAL_SUBS_REVENUE
        FROM
        (
            SELECT
                TOTAL_SUBS_AMOUNT,
                MSISDN
            FROM MON.SPARK_FT_SUBSCRIPTION_MSISDN_DAY N01
            WHERE N01.EVENT_DATE = '###SLICE_VALUE###'
        ) N0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 N1
        ON N0.MSISDN = N1.MSISDN
        GROUP BY SITE_NAME
    ) N ON A.LOC_SITE_NAME = N.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME,
            NVL(SUM(rated_count_1mo), 0) DATA_USERS
        FROM
        (
            --select msisdn
            --from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
            --where transaction_date = '###SLICE_VALUE###' and nbytest>(1*1024*1024)
            select *
            from MON.SPARK_FT_USERS_DATA_DAY
            where event_date = '###SLICE_VALUE###'
        ) O1
        LEFT JOIN
        (
            SELECT
                NVL(O20.CI, O21.CI) CI,
                UPPER(NVL(O20.SITE_NAME, O21.SITE_NAME)) SITE_NAME
            FROM
            (
                SELECT
                    lpad(CI, 5, 0) ci
                    , MAX(SITE_NAME) SITE_NAME
                FROM DIM.SPARK_DT_GSM_CELL_CODE
                GROUP BY lpad(CI, 5, 0)
            ) O20
            FULL JOIN
            (
                SELECT
                    lpad(CI, 5, 0) CI,
                    MAX(SITE_NAME) SITE_NAME
                FROM DIM.DT_CI_LAC_SITE_AMN
                GROUP BY lpad(CI, 5, 0)
            ) O21
            ON O20.CI = O21.CI
        ) O2 ON lpad(O1.location_ci, 5, 0) = O2.CI
        GROUP BY SITE_NAME
    ) O ON A.LOC_SITE_NAME = O.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME,
            NVL(COUNT(DISTINCT CHARGED_PARTY), 0) VOICE_USERS
        FROM
        (
            SELECT
                CHARGED_PARTY
                , (
                    CASE
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
                    END
                ) SERVICE_CODE
                , lpad(CAST(CONV(LOCATION_CI, 16, 10) AS INT), 5, 0) LOCATION_CI
                , lpad(CAST(CONV(location_lac, 16, 10) AS INT), 5, 0) location_lac
            FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID
            WHERE TRANSACTION_DATE ="###SLICE_VALUE###"
                and rated_duration > 0
                --AND MAIN_RATED_AMOUNT >= 0
                --AND PROMO_RATED_AMOUNT >= 0
        ) P1
        LEFT JOIN
        (
            SELECT
                NVL(P20.CI, P21.CI) CI,
                NVL(P20.lac, P21.lac) lac,
                UPPER(NVL(P20.SITE_NAME, P21.SITE_NAME)) SITE_NAME
            FROM
            (
                SELECT
                    lpad(CI, 5, 0) CI
                    , lpad(lac, 5, 0) lac
                    , MAX(SITE_NAME) SITE_NAME
                FROM DIM.SPARK_DT_GSM_CELL_CODE
                GROUP BY lpad(CI, 5, 0), lac
            ) P20
            FULL JOIN
            (
                SELECT
                    lpad(CI, 5, 0) CI,
                    lpad(lac, 5, 0) lac,
                    MAX(SITE_NAME) SITE_NAME
                FROM DIM.DT_CI_LAC_SITE_AMN
                GROUP BY lpad(CI, 5, 0), lac
            ) P21
            ON P20.CI = P21.CI and P20.lac = P21.lac
        ) P2 ON P1.LOCATION_CI = P2.CI and P1.location_lac = P2.lac
        WHERE SERVICE_CODE = 'VOI_VOX'
        GROUP BY SITE_NAME
    ) P ON A.LOC_SITE_NAME = P.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME,
            COUNT(DISTINCT IDENTIFICATEUR) NBRE_FAMOCO
        FROM
        (
            SELECT
                IDENTIFICATEUR
            FROM
            (
                SELECT
                    SERVED_PARTY_MSISDN
                FROM MON.SPARK_FT_SUBSCRIPTION
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND SUBSCRIPTION_SERVICE LIKE '%PPS%'
            ) Q00
            INNER JOIN
            (
                SELECT
                    MSISDN,
                    IDENTIFICATEUR
                FROM DIM.SPARK_DT_BASE_IDENTIFICATION
                WHERE DATE_IDENTIFICATION = '###SLICE_VALUE###'
            ) Q01
            ON SERVED_PARTY_MSISDN = MSISDN
            GROUP BY IDENTIFICATEUR
        ) Q0
        INNER JOIN TMP.SPARK_TMP_SITE_360 Q1
        ON Q0.IDENTIFICATEUR = Q1.MSISDN
        GROUP BY SITE_NAME
    ) Q ON A.LOC_SITE_NAME = Q.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME,
            SUM(CASE WHEN TECHNOLOGIE IN ('2.5G', '2.75G', '3G') THEN 1 ELSE 0 END) SMARTPHONES_3G,
            SUM(CASE WHEN TECHNOLOGIE = '4G' THEN 1 ELSE 0 END) SMARTPHONES_4G
        FROM
        (
            SELECT
                MSISDN,
                IMEI,
                TECHNOLOGIE
            FROM
            (
                SELECT
                    MSISDN
                    , SUBSTR(IMEI, 1, 14) IMEI
                FROM MON.SPARK_FT_IMEI_ONLINE
                WHERE SDATE='###SLICE_VALUE###'
            ) R00
            LEFT JOIN DIM.SPARK_DT_HANDSET_REF R01
            ON SUBSTR(R00.IMEI,1,8) = R01.TAC_CODE
            WHERE TERMINAL_TYPE LIKE '%HDS - SMARTPHONE%'
        ) R0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 R1
        ON R0.MSISDN = R1.MSISDN
        GROUP BY SITE_NAME
    ) R ON A.LOC_SITE_NAME = R.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME,
            -SUM(AMOUNT) AMOUNT_EMERGENCY_DATA
        FROM
        (
            SELECT
                MSISDN,
                AMOUNT
            FROM MON.SPARK_FT_EMERGENCY_DATA
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND TRANSACTION_TYPE = 'PAYBACK'
        ) S0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 S1
        ON S0.MSISDN = S1.MSISDN
        GROUP BY SITE_NAME
    ) S ON A.LOC_SITE_NAME = S.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME,
            SUM(AMOUNT_CHARGED) DATA_REFILL_FEES
        FROM
        (
            SELECT
                SENDER_MSISDN,
                AMOUNT_CHARGED
            FROM MON.SPARK_FT_DATA_TRANSFER
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
        ) T0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 T1
        ON T0.SENDER_MSISDN = T1.MSISDN
        GROUP BY SITE_NAME
    ) T ON A.LOC_SITE_NAME = T.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , SUM(DATA_COMBO + DATA_PUR) REVENU_SUBS_DATA
            , SUM(DATA_COMBO) REVENU_SUBS_DATA_COMBO
            , SUM(DATA_PUR) REVENU_SUBS_DATA_PUR
            , SUM(VOIX_COMBO + VOIX_PUR) REVENU_SUBS_VOIX
            , SUM(VOIX_COMBO) REVENU_SUBS_VOIX_COMBO
            , SUM(VOIX_PUR) REVENU_SUBS_VOIX_PUR
            , SUM(SMS_COMBO + SMS_PUR) REVENU_SUBS_SMS
            , SUM(SMS_COMBO) REVENU_SUBS_SMS_COMBO
            , SUM(SMS_PUR) REVENU_SUBS_SMS_PUR
        FROM
        (
            SELECT
                MSISDN
                , (
                    CASE WHEN (NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0)) < 100
                    THEN BDLE_COST*(NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0))/100
                    ELSE 0 END
                ) DATA_COMBO
                , (
                    CASE WHEN (NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0)) = 100
                    THEN BDLE_COST*(NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0))/100
                    ELSE 0 END
                ) DATA_PUR
                , (
                    CASE WHEN (NVL(COEFF_ONNET, 0)+NVL(COEFF_OFFNET, 0)+NVL(COEFF_INTER, 0)+NVL(COEFF_ROAMING_VOIX, 0)) < 100
                    THEN BDLE_COST*(NVL(COEFF_ONNET, 0) + NVL(COEFF_OFFNET, 0) + NVL(COEFF_INTER, 0) + NVL(COEFF_ROAMING_VOIX, 0))/100
                    ELSE 0 END
                ) VOIX_COMBO
                , (
                    CASE WHEN (NVL(COEFF_ONNET, 0)+NVL(COEFF_OFFNET, 0)+NVL(COEFF_INTER, 0)+NVL(COEFF_ROAMING_VOIX, 0)) = 100
                    THEN BDLE_COST*(NVL(COEFF_ONNET, 0) + NVL(COEFF_OFFNET, 0) + NVL(COEFF_INTER, 0) + NVL(COEFF_ROAMING_VOIX, 0))/100
                    ELSE 0 END
                ) VOIX_PUR
                , (
                    CASE WHEN (NVL(COEF_SMS, 0) + NVL(COEFF_ROAMING_SMS, 0)) < 100
                    THEN BDLE_COST*(NVL(COEF_SMS, 0) + NVL(COEFF_ROAMING_SMS, 0))/100
                    ELSE 0 END
                ) SMS_COMBO
                , (
                    CASE WHEN (NVL(COEF_SMS, 0) + NVL(COEFF_ROAMING_SMS, 0)) = 100
                    THEN BDLE_COST*(NVL(COEF_SMS, 0) + NVL(COEFF_ROAMING_SMS, 0))/100
                    ELSE 0 END
                ) SMS_PUR
            FROM
            (
                SELECT
                    MSISDN,
                    BDLE_COST,
                    BDLE_NAME
                FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
                WHERE PERIOD = '###SLICE_VALUE###'
            ) U00
            LEFT JOIN DIM.DT_CBM_REF_SOUSCRIPTION_PRICE U01
            ON UPPER(TRIM(U00.BDLE_NAME))= UPPER(TRIM(U01.BDLE_NAME))
        ) U0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 U1
        ON U0.MSISDN = U1.MSISDN
        GROUP BY SITE_NAME
    ) U ON A.LOC_SITE_NAME = U.SITE_NAME
    FULL JOIN
    (
        SELECT
            REPLACE(UPPER(SITE_NAME), 'Ã','E') SITE_NAME,
            SUM(CASE WHEN NETWORK = 'OCM_ALL' THEN SUBSCRIBER_COUNT ELSE 0 END) PDM_OCM,
            SUM(CASE WHEN NETWORK = 'MTN' THEN SUBSCRIBER_COUNT ELSE 0 END) PDM_MTN,
            SUM(CASE WHEN NETWORK = 'CAMTEL' THEN SUBSCRIBER_COUNT ELSE 0 END) PDM_CAMTEL,
            SUM(CASE WHEN NETWORK = 'VIETTEL' THEN SUBSCRIBER_COUNT ELSE 0 END) PDM_NEXTTEL,
            SUM(CASE WHEN NETWORK IN ('OCM_ALL','MTN','CAMTEL','VIETTEL') THEN SUBSCRIBER_COUNT ELSE 0 END) PDM_NATIONAL
        FROM MON.SPARK_FT_PDM_SITE_DAY
        WHERE EVENT_DATE = '###SLICE_VALUE###'
        GROUP BY REPLACE(UPPER(SITE_NAME), 'Ã','E')
    ) V ON A.LOC_SITE_NAME = V.SITE_NAME
    FULL JOIN
    (
        SELECT
            SITE_NAME
            , COUNT(DISTINCT W0.MSISDN) PARC_ACTIF_OM
        FROM
        (
            SELECT 
                SENDER_MSISDN MSISDN
            FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
            WHERE TRANSFER_STATUS='TS' AND
                TRANSFER_DATETIME BETWEEN DATE_SUB('###SLICE_VALUE###', 30) AND '###SLICE_VALUE###' AND
                SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'P2P', 'P2PNONREG', 'ENT2REG', 'RC', 'MERCHPAY', 'BILLPAY') AND
                SENDER_CATEGORY_CODE='SUBS'
            UNION 
            SELECT 
                RECEIVER_MSISDN MSISDN
            FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
            WHERE TRANSFER_STATUS='TS' AND
                TRANSFER_DATETIME BETWEEN DATE_SUB('###SLICE_VALUE###', 30) AND '###SLICE_VALUE###' AND
                SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'P2P', 'P2PNONREG', 'ENT2REG', 'RC', 'MERCHPAY', 'BILLPAY') AND
                RECEIVER_CATEGORY_CODE='SUBS'
        ) W0
        LEFT JOIN TMP.SPARK_TMP_SITE_360 W1
        ON W0.MSISDN = W1.MSISDN
        GROUP BY SITE_NAME
    ) W ON A.LOC_SITE_NAME = W.SITE_NAME
    --FULL JOIN
    --(
    --    SELECT
    --        SITE_NAME,
    --        SUM(RGS_1_1M) RGS_1_1M,
    --        SUM(RGS_1_5M) RGS_1_5M,
    --        SUM(RGS_10_1M) RGS_10_1M,
    --        SUM(RGS_10_5M) RGS_10_5M,
    --        SUM(RGS_20_1M) RGS_20_1M,
    --        SUM(RGS_20_5M) RGS_20_5M,
    --        SUM(RGS_30_1M) RGS_30_1M,
    --        SUM(RGS_30_5M) RGS_30_5M,
    --        SUM(RGS_60_1M) RGS_60_1M,
    --        SUM(RGS_60_5M) RGS_60_5M,
    --        SUM(RGS_90_1M) RGS_90_1M,
    --        SUM(RGS_90_5M) RGS_90_5M
    --    FROM
    --    (
    --        SELECT
    --            NVL(X00.MSISDN, X01.MSISDN) MSISDN,
    --            (
    --                CASE
    --                WHEN (IC_CALL_1='###SLICE_VALUE###' AND IC_CALL_2='###SLICE_VALUE###' AND IC_CALL_3='###SLICE_VALUE###' AND IC_CALL_4='###SLICE_VALUE###') OR (BYTES_RECEIVED + BYTES_SENT > 1048576 OR OG_CALL='###SLICE_VALUE###') THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_1_1M,
    --            (
    --                CASE
    --                WHEN (IC_CALL_1='###SLICE_VALUE###' AND IC_CALL_2='###SLICE_VALUE###' AND IC_CALL_3='###SLICE_VALUE###' AND IC_CALL_4='###SLICE_VALUE###') OR (BYTES_RECEIVED + BYTES_SENT > 5242880 OR OG_CALL='###SLICE_VALUE###') THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_1_5M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=10 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=10 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=10 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=10) OR (BYTES_RECEIVED + BYTES_SENT > 1048576 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=10) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_10_1M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=10 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=10 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=10 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=10) OR (BYTES_RECEIVED + BYTES_SENT > 5242880 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=10) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_10_5M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=20 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=20 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=20 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=20) OR (BYTES_RECEIVED + BYTES_SENT > 1048576 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=20) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_20_1M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=20 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=20 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=20 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=20) OR (BYTES_RECEIVED + BYTES_SENT > 5242880 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=20) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_20_5M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=30 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=30 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=30 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=30) OR (BYTES_RECEIVED + BYTES_SENT > 1048576 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=30) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_30_1M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=30 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=30 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=30 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=30) OR (BYTES_RECEIVED + BYTES_SENT > 5242880 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=30) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_30_5M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=60 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=60 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=60 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=60) OR (BYTES_RECEIVED + BYTES_SENT > 1048576 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=60) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_60_1M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=60 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=60 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=60 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=60) OR (BYTES_RECEIVED + BYTES_SENT > 5242880 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=60) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_60_5M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=90 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=90 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=90 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=90) OR (BYTES_RECEIVED + BYTES_SENT > 1048576 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=90) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_90_1M,
    --            (
    --                CASE
    --                WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')=90 AND DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')=90 AND DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')=90 AND DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')=90) OR (BYTES_RECEIVED + BYTES_SENT > 5242880 OR DATEDIFF(OG_CALL, '###SLICE_VALUE###')=90) THEN 1
    --                ELSE 0
    --                END
    --            ) RGS_90_5M
    --        FROM
    --        (
    --            SELECT
    --                MSISDN,
    --                OG_CALL,
    --                IC_CALL_1,
    --                IC_CALL_2,
    --                IC_CALL_3,
    --                IC_CALL_4
    --            FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
    --            WHERE EVENT_DATE = '###SLICE_VALUE###'
    --        ) X00
    --        FULL JOIN
    --        (
    --            SELECT
    --                SERVED_PARTY_MSISDN MSISDN,
    --                SUM(BYTES_SENT) BYTES_SENT,
    --                SUM(BYTES_RECEIVED) BYTES_RECEIVED
    --            FROM MON.SPARK_FT_CRA_GPRS
    --            WHERE SESSION_DATE = '###SLICE_VALUE###'
    --            GROUP BY MSISDN
    --        ) X01 ON X00.MSISDN = X01.MSISDN
    --    ) X0
    --    LEFT JOIN TMP.SPARK_TMP_SITE_360 X1
    --    ON X0.MSISDN = X1.MSISDN
    --    GROUP BY SITE_NAME
    --) X ON A.LOC_SITE_NAME = X.SITE_NAME
    --FULL JOIN
    --(
    --    SELECT
    --        SITE_NAME,
    --        SUM(
    --            CASE
    --            WHEN NBRE_APPEL_SORTANT>0 AND NBRE_SMS_SORTANT=0 AND DATA_USED=0 THEN 1
    --            ELSE 0
    --            END
    --        ) VOICE_USER_ONLY,
    --        SUM(
    --            CASE
    --            WHEN NBRE_APPEL_SORTANT=0 AND NBRE_SMS_SORTANT>0 AND DATA_USED=0 THEN 1
    --            ELSE 0
    --            END
    --        ) SMS_USER_ONLY,
    --        SUM(
    --            CASE
    --            WHEN NBRE_APPEL_SORTANT>0 AND NBRE_SMS_SORTANT=0 AND DATA_USED>0 THEN 1
    --            ELSE 0
    --            END
    --        ) VOICE_DATA_USER,
    --        SUM(
    --            CASE
    --            WHEN NBRE_APPEL_SORTANT=0 AND NBRE_SMS_SORTANT>0 AND DATA_USED>0 THEN 1
    --            ELSE 0
    --            END
    --        ) DATA_SMS_USER,
    --        SUM(
    --            CASE
    --            WHEN NBRE_APPEL_SORTANT>0 AND NBRE_SMS_SORTANT>0 AND DATA_USED>0 THEN 1
    --            ELSE 0
    --            END
    --        ) VOICE_DATA_SMS_USER
    --    FROM
    --    (
    --        SELECT
    --            NVL(SERVED_MSISDN, CHARGED_PARTY_MSISDN) MSISDN,
    --            NBRE_APPEL_SORTANT,
    --            NBRE_SMS_SORTANT,
    --            DATA_USED
    --        FROM
    --        (
    --            SELECT
    --                SERVED_MSISDN,
    --                SUM(
    --                    CASE
    --                    WHEN UPPER(TRANSACTION_DIRECTION) = 'SORTANT' AND UPPER(TRANSACTION_TYPE) = 'TEL' THEN 1
    --                    ELSE 0
    --                    END
    --                ) NBRE_APPEL_SORTANT,
    --                SUM(
    --                    CASE
    --                    WHEN UPPER(TRANSACTION_DIRECTION) = 'SORTANT' AND UPPER(TRANSACTION_TYPE) = 'SMS_MT' THEN 1
    --                    ELSE 0
    --                    END
    --                ) NBRE_SMS_SORTANT
    --            FROM MON.SPARK_FT_MSC_TRANSACTION
    --            WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    --            GROUP BY SERVED_MSISDN
    --        ) Y00
    --        FULL JOIN
    --        (
    --            SELECT
    --                CHARGED_PARTY_MSISDN
    --                , SUM(BYTES_SENT + BYTES_RECEIVED) DATA_USED
    --            FROM MON.SPARK_FT_CRA_GPRS
    --            WHERE SESSION_DATE = '###SLICE_VALUE###'
    --            GROUP BY CHARGED_PARTY_MSISDN
    --        ) Y01
    --        ON SERVED_MSISDN = CHARGED_PARTY_MSISDN
    --    ) Y0
    --    LEFT JOIN TMP.SPARK_TMP_SITE_360 Y1
    --    ON Y0.MSISDN = Y1.MSISDN
    --    GROUP BY SITE_NAME
    --) Y ON A.LOC_SITE_NAME = Y.SITE_NAME
    --FULL JOIN
    --(
    --    SELECT
    --        SITE_NAME,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'TEL' AND OTHER_PARTY_OPERATOR LIKE '%MTN%' THEN TRANSACTION_DURATION
    --            ELSE 0
    --            END
    --        ) INC_RATED_TEL_MTN_DURATION,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'TEL' AND OTHER_PARTY_OPERATOR LIKE '%VIETTEL%' THEN TRANSACTION_DURATION
    --            ELSE 0
    --            END
    --        ) INC_RATED_TEL_NEXTTEL_DURATION,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'TEL' AND OTHER_PARTY_OPERATOR LIKE '%CAMTEL%' THEN TRANSACTION_DURATION
    --            ELSE 0
    --            END
    --        ) INC_RATED_TEL_CAMTEL_DURATION,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'TEL' AND OTHER_PARTY_IS_NATIONAL=0 THEN TRANSACTION_DURATION
    --            ELSE 0
    --            END
    --        ) INC_RATED_TEL_INT_DURATION,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'TEL' THEN TRANSACTION_DURATION
    --            ELSE 0
    --            END
    --        ) TOTAL_INC_RATED_TEL_DURATION,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'SMS_MT' AND OTHER_PARTY_OPERATOR LIKE '%MTN%' THEN 1
    --            ELSE 0
    --            END
    --        ) INC_SMS_MTN_COUNT,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'SMS_MT' AND OTHER_PARTY_OPERATOR LIKE '%VIETTEL%' THEN 1
    --            ELSE 0
    --            END
    --        ) INC_SMS_NEXTTEL_COUNT,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'SMS_MT' AND OTHER_PARTY_OPERATOR LIKE '%CAMTEL%' THEN 1
    --            ELSE 0
    --            END
    --        ) INC_SMS_CAMTEL_COUNT,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'SMS_MT' AND OTHER_PARTY_IS_NATIONAL=0 THEN 1
    --            ELSE 0
    --            END
    --        ) INC_SMS_INTERNATIONAL_COUNT,
    --        SUM(
    --            CASE
    --            WHEN UPPER(TRANSACTION_TYPE) = 'SMS_MT' THEN 1
    --            ELSE 0
    --            END
    --        ) TOTAL_INC_SMS_COUNT
    --    FROM
    --    (
    --        SELECT
    --            SERVED_MSISDN MSISDN,
    --            FN_GET_NNP_MSISDN_SIMPLE_DESTN(OTHER_PARTY) OTHER_PARTY_OPERATOR,
    --            TRANSACTION_DURATION,
    --            TRANSACTION_TYPE,
    --            OTHER_PARTY_IS_NATIONAL
    --        FROM MON.SPARK_FT_MSC_TRANSACTION
    --        WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND UPPER(TRANSACTION_DIRECTION) = 'ENTRANT' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(SERVED_MSISDN) = 'OCM'
    --    ) Z0
    --    LEFT JOIN TMP.SPARK_TMP_SITE_360 Z1
    --    ON Z0.MSISDN = Z1.MSISDN
    --    GROUP BY SITE_NAME
    --) Z ON A.LOC_SITE_NAME = Z.SITE_NAME
) ZZ
