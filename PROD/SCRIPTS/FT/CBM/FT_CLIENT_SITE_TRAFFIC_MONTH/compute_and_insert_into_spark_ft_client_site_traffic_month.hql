INSERT INTO MON.SPARK_FT_CLIENT_SITE_TRAFFIC_MONTH
SELECT
    FN_FORMAT_MSISDN_TO_9DIGITS(A.MSISDN) MSISDN
    , B.SITE_NAME
    , SUM(NVL(A.DUREE_SORTANT,0)) DUREE_SORTANT
    , SUM(NVL(A.NBRE_TEL_SORTANT,0)) NBRE_TEL_SORTANT
    , SUM(NVL(A.DUREE_ENTRANT,0)) DUREE_ENTRANT
    , SUM(NVL(A.NBRE_TEL_ENTRANT,0)) NBRE_TEL_ENTRANT
    , SUM(NVL(A.NBRE_SMS_SORTANT,0)) NBRE_SMS_SORTANT
    , SUM(NVL(A.NBRE_SMS_ENTRANT,0)) NBRE_SMS_ENTRANT
    , CURRENT_DATE() REFRESH_DATE
    , B.TOWNNAME
    , B.ADMINISTRATIVE_REGION
    , B.COMMERCIAL_REGION
    , OPERATOR_CODE
    , CURRENT_TIMESTAMP() INSERT_DATE
    , '###SLICE_VALUE###' EVENT_MONTH
FROM
(
    SELECT *
    FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
    WHERE EVENT_DATE BETWEEN '###SLICE_VALUE###'||'-01' AND DATE_SUB(ADD_MONTHS('###SLICE_VALUE###'||'-01', 1), 1)
) A
LEFT JOIN
(
    SELECT B0.MSISDN, B1.*
    FROM
    (
        SELECT
            DISTINCT MSISDN MSISDN
            , FIRST_VALUE(SITE_NAME) OVER (PARTITION  BY MSISDN ORDER BY NBRE DESC) SITE_NAME
        FROM
        (
            SELECT
                FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN) MSISDN
                , NVL(SITE_NAME_CORRECTED, SITE_NAME) SITE_NAME
                , SUM(NVL(DUREE_SORTANT, 0) + NVL (DUREE_ENTRANT, 0) + NVL (NBRE_SMS_SORTANT, 0)  + NVL (NBRE_SMS_ENTRANT, 0)) NBRE
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE BETWEEN '###SLICE_VALUE###'||'-01' AND DATE_SUB(ADD_MONTHS('###SLICE_VALUE###'||'-01', 1), 1)
            GROUP BY FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN), NVL (SITE_NAME_CORRECTED, SITE_NAME) 
        ) B00
    ) B0
    INNER JOIN
    (
        SELECT 
            SITE_NAME
            , MAX(TOWNNAME) TOWNNAME
            , MAX(ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION
            , MAX(COMMERCIAL_REGION) COMMERCIAL_REGION
        FROM
        (
            SELECT 
                CI
                , TOWNNAME
                , (
                    CASE 
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'NRO' THEN 'NORD-OUEST'
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'EXN' THEN 'EXTREME-NORD'
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'YDE' THEN 'CENTRE'
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'SUO' THEN 'SUD-OUEST'
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'ADM' THEN 'ADAMAOUA'
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'CTR' THEN 'CENTRE'
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'NRD' THEN 'NORD'
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'DLA' THEN 'LITTORAL'
                    WHEN UPPER (ADMINISTRATIVE_REGION) = 'OST' THEN 'OUEST'
                    ELSE UPPER (ADMINISTRATIVE_REGION)
                    END
                ) ADMINISTRATIVE_REGION
                , COMMERCIAL_REGION
                , SITE_NAME
            FROM
            (
                SELECT
                    B1000.CI,
                    UPPER (B1000.TOWNNAME) TOWNNAME,
                    UPPER (B1001.COMMERCIAL_REGION) COMMERCIAL_REGION,
                    UPPER (B1003.REGION) ADMINISTRATIVE_REGION,
                    UPPER (B1002.SITE_NAME) SITE_NAME                                 
                FROM
                (
                    SELECT
                        DISTINCT CI,
                        FIRST_VALUE(TOWNNAME) OVER (PARTITION BY CI ORDER BY NBRE DESC) TOWNNAME
                    FROM (
                        SELECT
                            CI,
                            REPLACE(REPLACE(TOWNNAME, '¿', 'e'), 'é', 'e') TOWNNAME,
                            SUM(1) NBRE
                        FROM DIM.DT_GSM_CELL_CODE
                        GROUP BY CI, REPLACE(REPLACE(TOWNNAME, '¿', 'e'), 'é', 'e')
                    ) B10000
                ) B1000
                LEFT JOIN
                (
                    SELECT
                        DISTINCT CI,
                        FIRST_VALUE(COMMERCIAL_REGION) OVER (PARTITION BY CI ORDER BY NBRE DESC) COMMERCIAL_REGION
                    FROM
                    (
                        SELECT
                            CI,
                            COMMERCIAL_REGION,
                            SUM (1) NBRE
                        FROM DIM.DT_GSM_CELL_CODE
                        GROUP BY CI, COMMERCIAL_REGION
                    ) B10010
                ) B1001 ON B1000.CI = B1001.CI
                LEFT JOIN
                (
                    SELECT
                        DISTINCT CI,
                        FIRST_VALUE (SITE_NAME) OVER (PARTITION BY CI ORDER BY NBRE DESC) SITE_NAME
                    FROM
                    (
                        SELECT
                        CI,
                        SITE_NAME,
                        SUM (1) NBRE
                        FROM DIM.DT_GSM_CELL_CODE
                        GROUP BY CI, SITE_NAME
                    ) B10020
                ) B1002 ON B1000.CI = B1002.CI
                LEFT JOIN
                (
                    SELECT
                        DISTINCT CI,
                        FIRST_VALUE (REGION) OVER (PARTITION BY CI ORDER BY NBRE DESC) REGION
                    FROM
                    (
                        SELECT
                        CI,
                        REGION,
                        SUM (1) NBRE
                        FROM DIM.DT_GSM_CELL_CODE
                        GROUP BY CI, REGION
                    ) B10030
                ) B1003 ON B1000.CI = B1003.CI
            ) B100
        ) B10
        GROUP BY SITE_NAME
    ) B1 ON B0.SITE_NAME = B1.SITE_NAME
) B ON FN_FORMAT_MSISDN_TO_9DIGITS(A.MSISDN) = B.MSISDN
GROUP BY FN_FORMAT_MSISDN_TO_9DIGITS(A.MSISDN)
    , B.SITE_NAME
    , B.TOWNNAME
    , B.ADMINISTRATIVE_REGION
    , B.COMMERCIAL_REGION
    , OPERATOR_CODE