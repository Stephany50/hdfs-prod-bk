 -- CALCULER LES DONNÉES 'PURE POSTPAID'
INSERT INTO  MON.SPARK_FT_PARCS_SITE_DAY
    (PARC_TYPE, PROFILE, STATUT, 
     EFFECTIF, SITE_NAME, TOWNNAME, 
     ADMINISTRATIVE_REGION, COMMERCIAL_REGION, 
     SRC_TABLE, INSERT_DATE, CONTRACT_TYPE, 
     OPERATOR_CODE, EVENT_DATE)
(
    SELECT
       'PARC_ART' AS PARC_TYPE, 
       UPPER(NVL(d.PROFILE_NAME, commercial_offer)) PROFILE_NAME, 
       account_status, SUM (NVL(total_count, 0) ) total_count, 
       SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION, 
       source, CURRENT_TIMESTAMP  REFRESH_DATE, 
       UPPER(a.OSP_CONTRACT_TYPE), 'OCM' AS OPERATOR_CODE, datecode
    FROM
        (
            SELECT  
                'PURE POSTPAID' OSP_CONTRACT_TYPE, 
                UPPER(a.PROFILE) commercial_offer,
                (CASE
                    WHEN  a.CURRENT_STATUS IN  ('a', 's')  THEN 'ACTIF'
                    ELSE    'INACT'
                 END) account_status, 
                COUNT(*) AS total_count, 'FT_BSCS_CONTRACT' source, 
                SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, 
                COMMERCIAL_REGION, DATE_ADD('###SLICE_VALUE###',-1) datecode
            FROM 
                (SELECT * 
                 FROM MON.SPARK_FT_CONTRACT_SNAPSHOT 
                 WHERE  
                     TO_DATE(EVENT_DATE) = '2020-01-03'
                     AND (NVL(BSCS_ACTIVATION_DATE, ACTIVATION_DATE) <= '2020-01-03' )
                     AND NVL(OSP_CONTRACT_TYPE, 'PURE PREPAID') = 'PURE POSTPAID') a
                LEFT JOIN
                    (SELECT  MSISDN
                     FROM MON.SPARK_FT_ACCOUNT_ACTIVITY  
                     WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###') b
                ON  a.ACCESS_KEY = b.MSISDN
                LEFT JOIN
                      (SELECT MSISDN, SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
                       FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
                       WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###',-1)) c
                ON a.ACCESS_KEY = c.MSISDN 
            GROUP BY
                UPPER(a.PROFILE),
                (CASE
                 WHEN  a.CURRENT_STATUS IN  ('a', 's')  THEN 'ACTIF'
                 ELSE    'INACT'
                 END), 
                SITE_NAME, TOWNNAME, 
                ADMINISTRATIVE_REGION, 
                COMMERCIAL_REGION
        ) a
        LEFT JOIN
          MON.VW_DT_OFFER_PROFILES d
        ON UPPER(a.commercial_offer) = d.PROFILE_CODE
        GROUP BY
            UPPER(a.OSP_CONTRACT_TYPE), account_status , 
            source, UPPER(NVL(d.PROFILE_NAME, commercial_offer)), 
            SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, 
            COMMERCIAL_REGION, datecode
);