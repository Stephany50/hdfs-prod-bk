INSERT INTO  MON.SPARK_FT_PARCS_SITE_DAY
    SELECT
        'PARC_ART' AS PARC_TYPE, UPPER(NVL(d.PROFILE_NAME, commercial_offer)) PROFILE,
        account_status STATUT, SUM(NVL(total_count, 0)) EFFECTIF,
        SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION,
        COMMERCIAL_REGION, source SRC_TABLE, CURRENT_TIMESTAMP  INSERT_DATE,
        UPPER(a.OSP_CONTRACT_TYPE) CONTRACT_TYPE, 'OCM' AS OPERATOR_CODE,datecode EVENT_DATE
    FROM
        (
            SELECT
                NVL (c.CONTRACT_TYPE, 'PURE PREPAID' ) OSP_CONTRACT_TYPE,
                b.PROFILE commercial_offer, b.COMGP_STATUS account_status,
                COUNT(*) AS total_count, b.SRC_TABLE source, SITE_NAME,
                TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION,
                DATE_ADD('###SLICE_VALUE###',-1) datecode
            FROM
                (SELECT MSISDN access_key
                   FROM (select ROW_NUMBER() OVER(ORDER BY MSISDN) as rownum, MSISDN
                         FROM TMP.TT_TMP_MISS_MSISDN_SITE) r
                   WHERE r.rownum < NVL(DATEDIFF('###SLICE_VALUE###','2014-04-20')*3000,0)) a
                LEFT JOIN
                  (SELECT MSISDN, COMGP_STATUS, FORMULE PROFILE, SRC_TABLE, ACTIVATION_DATE
                     FROM MON.SPARK_FT_ACCOUNT_ACTIVITY  WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###') b
                ON a.ACCESS_KEY = b.MSISDN
                LEFT JOIN
                   (SELECT * FROM MON.VW_DT_OFFER_PROFILES ) c
                ON b.PROFILE=c.PROFILE_CODE
                LEFT JOIN
                  (
                    --localisation par site des numeros
                    SELECT MSISDN, SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION
                    FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
                    WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###',-1)
                  ) d
                ON a.ACCESS_KEY = d.MSISDN
            WHERE
                (b.ACTIVATION_DATE <= '###SLICE_VALUE###')
                AND NVL (c.CONTRACT_TYPE, 'PURE PREPAID') IN ('PURE PREPAID', 'HYBRID')
            GROUP BY
                b.PROFILE, b.COMGP_STATUS,
                NVL (c.CONTRACT_TYPE, 'PURE PREPAID' ),
                b.SRC_TABLE, SITE_NAME, TOWNNAME,
                ADMINISTRATIVE_REGION, COMMERCIAL_REGION
        ) a
        LEFT JOIN
         MON.VW_DT_OFFER_PROFILES d
        ON UPPER(a.commercial_offer) = d.PROFILE_CODE
        GROUP BY
            datecode, UPPER (a.OSP_CONTRACT_TYPE), account_status ,
            source, UPPER(NVL(d.PROFILE_NAME, commercial_offer)),
            SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION

