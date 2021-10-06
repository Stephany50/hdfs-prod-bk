 -- CALCULER LES DONNÉES 'PURE PREPAID' et 'HYBRID'
INSERT INTO  MON.SPARK_FT_PARCS_SITE_DAY
    (PARC_TYPE, PROFILE, STATUT, 
     EFFECTIF, SITE_NAME, TOWNNAME, 
     ADMINISTRATIVE_REGION, COMMERCIAL_REGION,
     SRC_TABLE, INSERT_DATE, CONTRACT_TYPE, 
     OPERATOR_CODE,EVENT_DATE)
    (
        SELECT
            'PARC_ART' AS PARC_TYPE, 
            UPPER(NVL(d.PROFILE_NAME,commercial_offer )) PROFILE_NAME,
            account_status, SUM ( NVL (total_count, 0) ) total_count, 
            SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, 
            COMMERCIAL_REGION, source, CURRENT_TIMESTAMP  REFRESH_DATE, 
            UPPER (a.OSP_CONTRACT_TYPE), 'OCM' AS OPERATOR_CODE, datecode
        FROM
            (
                SELECT
                    NVL(a.OSP_CONTRACT_TYPE, 'PURE PREPAID') OSP_CONTRACT_TYPE, 
                    a.PROFILE commercial_offer, b.COMGP_STATUS account_status, 
                    COUNT(*) AS total_count, a.SRC_TABLE source, 
                    SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, 
                    COMMERCIAL_REGION, DATE_ADD('###SLICE_VALUE###',-1) datecode
                FROM 
                    (SELECT * 
                     FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                     WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###'
                     AND (TO_DATE(ACTIVATION_DATE) <= '###SLICE_VALUE###')
                     AND NVL(OSP_CONTRACT_TYPE, 'PURE PREPAID') IN ('PURE PREPAID', 'HYBRID')) a
                    LEFT JOIN
                        (
                         SELECT  MSISDN, COMGP_STATUS
                         FROM MON.SPARK_FT_ACCOUNT_ACTIVITY  
                         WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###'
                        ) b
                    ON a.ACCESS_KEY = b.MSISDN
                    LEFT JOIN
                        ( -- localisation par site des numeros
                          SELECT MSISDN, SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
                          FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
                          WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
                        ) c
                    ON a.ACCESS_KEY = c.MSISDN
                GROUP BY
                       a.PROFILE, b.COMGP_STATUS, 
                       NVL(a.OSP_CONTRACT_TYPE, 'PURE PREPAID'), 
                       a.SRC_TABLE, SITE_NAME, TOWNNAME, 
                       ADMINISTRATIVE_REGION, COMMERCIAL_REGION
            ) a
            LEFT JOIN
             (SELECT * FROM MON.VW_DT_OFFER_PROFILES) d
            ON UPPER(a.commercial_offer) = d.PROFILE_CODE
            GROUP BY
                UPPER(a.OSP_CONTRACT_TYPE), account_status , 
                source, UPPER(NVL(d.PROFILE_NAME, commercial_offer)), 
                SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, 
                COMMERCIAL_REGION, datecode
    );