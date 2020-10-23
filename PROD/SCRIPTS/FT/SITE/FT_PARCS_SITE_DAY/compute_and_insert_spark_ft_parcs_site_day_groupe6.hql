-- inserer les données finales           
INSERT INTO MON.SPARK_FT_PARCS_SITE_DAY
    (PARC_TYPE, PROFILE, STATUT, EFFECTIF, 
     SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, 
     COMMERCIAL_REGION, SRC_TABLE, INSERT_DATE, 
     CONTRACT_TYPE, OPERATOR_CODE, EVENT_DATE)
( 
    SELECT
        'PARC_DECONNEXION' AS PARC_TYPE
        , UPPER (a.PROFILE) PROFILE
        , 'INACT' STATUT, COUNT(*) as EFFECTIF
        , SITE_NAME, TOWNNAME
        , ADMINISTRATIVE_REGION
        , COMMERCIAL_REGION, a.SRC_TABLE
        , CURRENT_TIMESTAMP INSERT_DATE
        , NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) CONTRACT_TYPE
        , 'OCM' AS OPERATOR_CODE, DATE_ADD('###SLICE_VALUE###',-1) AS EVENT_DATE
    FROM
        (SELECT ACCESS_KEY, PROFILE, SRC_TABLE, OSP_CONTRACT_TYPE 
         FROM MON.SPARK_FT_CONTRACT_SNAPSHOT 
         WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###') a
        RIGHT JOIN
        (
            SELECT a.MSISDN, SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
            FROM
            (
                SELECT MSISDN
                from MON.SPARK_FT_ACCOUNT_ACTIVITY
                WHERE TO_DATE(event_date) = '###SLICE_VALUE###'
                    AND GP_STATUS = 'INACT' and TO_DATE(GP_STATUS_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
             ) a
             LEFT JOIN
             (
                -- localisation par site des numeros
                SELECT MSISDN, SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
                FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
                WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
             ) b
                ON a.MSISDN = b.MSISDN
        ) b
        ON b.MSISDN = a.ACCESS_KEY
    GROUP BY
         UPPER (a.PROFILE)
        , SITE_NAME, TOWNNAME
        , ADMINISTRATIVE_REGION
        , COMMERCIAL_REGION
        , a.SRC_TABLE, NVL(a.OSP_CONTRACT_TYPE, 'PURE PREPAID')
);