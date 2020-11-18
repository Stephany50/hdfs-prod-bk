INSERT INTO TMP.TT_PARCS_SITE_DAY
    (PARC_TYPE, PROFILE, STATUT, 
     EFFECTIF, SITE_NAME, TOWNNAME, 
     ADMINISTRATIVE_REGION, COMMERCIAL_REGION, 
     SRC_TABLE, INSERT_DATE, CONTRACT_TYPE, 
     OPERATOR_CODE, EVENT_DATE)
    SELECT 
      PARC_TYPE
      , PROFILE
      , STATUT
      , EFFECTIF
      , SITE_NAME, TOWNNAME
      , ADMINISTRATIVE_REGION
      , COMMERCIAL_REGION
      , SRC_TABLE, INSERT_DATE
      , CONTRACT_TYPE
      , OPERATOR_CODE, EVENT_DATE
    FROM 
    (
        SELECT
            'PARC_GROUPE' AS PARC_TYPE
            , UPPER (b.FORMULE) PROFILE
            , NVL(b.GP_STATUS, 'INACT') STATUT
            , COUNT(*) as EFFECTIF
            , SITE_NAME, TOWNNAME
            , ADMINISTRATIVE_REGION
            , COMMERCIAL_REGION
            , b.SRC_TABLE, CURRENT_TIMESTAMP INSERT_DATE
            , NVL(UPPER(d.CONTRACT_TYPE), 'PURE PREPAID') CONTRACT_TYPE
            , 'OCM' AS OPERATOR_CODE, DATE_ADD('###SLICE_VALUE###',-1) EVENT_DATE
            , ROW_NUMBER() OVER(ORDER BY SITE_NAME) as rownum
        FROM 
            MON.SPARK_FT_ACCOUNT_ACTIVITY b
          INNER JOIN 
            TMP.TT_TMP_MSISDN_PARC_SITE c
          ON b.MSISDN = c.MSISDN
          LEFT JOIN
            DIM.SPARK_DT_OFFER_PROFILES d
          ON b.FORMULE = d.PROFILE_CODE
          LEFT JOIN
            (SELECT MSISDN, SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
             FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
             WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###',-1)) e
          ON c.MSISDN = e.MSISDN  
        WHERE TO_DATE(b.EVENT_DATE) = '###SLICE_VALUE###'
            AND NVL(UPPER(d.CONTRACT_TYPE), 'PURE PREPAID') IN ('PURE PREPAID', 'HYBRID')
        GROUP BY
         UPPER(b.FORMULE)
        , NVL(b.GP_STATUS, 'INACT')
        , SITE_NAME, TOWNNAME
        , ADMINISTRATIVE_REGION
        , COMMERCIAL_REGION
        , b.SRC_TABLE, NVL(UPPER(d.CONTRACT_TYPE), 'PURE PREPAID')
    ) r
    WHERE-- injection petit à petit :: 2.000 par jour à partir du 20/04/2014
       r.rownum < NVL(DATEDIFF('###SLICE_VALUE###','2014-04-20')*1500,0)                        