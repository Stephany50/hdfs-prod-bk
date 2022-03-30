-- ajouter les données des postpaid : pure postpaid seulement car les hybrides ont été compté (puisque present à l'IN)
INSERT INTO TMP.TT_PARCS_SITE_DAY
SELECT
    'PARC_GROUPE' AS PARC_TYPE
    , UPPER (a.PROFILE) PROFILE
    ,  (CASE
          WHEN  a.CURRENT_STATUS IN  ('a', 's')  THEN 'ACTIF'
          ELSE  'INACT'
        END ) STATUT
    , COUNT(*) as EFFECTIF
    , SITE_NAME
    , TOWNNAME
    , ADMINISTRATIVE_REGION
    , COMMERCIAL_REGION
    , 'FT_BSCS_CONTRACT' SRC_TABLE
    , CURRENT_TIMESTAMP INSERT_DATE
    , NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) CONTRACT_TYPE
    , 'OCM' AS OPERATOR_CODE, DATE_ADD('###SLICE_VALUE###',-1) EVENT_DATE
FROM
    (SELECT * FROM MON.SPARK_FT_CONTRACT_SNAPSHOT 
      WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###'
      AND NVL (OSP_CONTRACT_TYPE, 'PURE PREPAID' ) = 'PURE POSTPAID') a
    LEFT JOIN
      (SELECT * FROM MON.SPARK_FT_ACCOUNT_ACTIVITY  
        WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###') b
    ON a.ACCESS_KEY = b.MSISDN
    LEFT JOIN
      (SELECT MSISDN, SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
        WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###',-1)) c
    ON a.ACCESS_KEY = c.MSISDN
GROUP BY
      UPPER (a.PROFILE)
    ,  (CASE
          WHEN  a.CURRENT_STATUS IN  ('a', 's')  THEN 'ACTIF'
          ELSE    'INACT'
        END)
    , SITE_NAME
    , TOWNNAME
    , ADMINISTRATIVE_REGION
    , COMMERCIAL_REGION
    , NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID')
