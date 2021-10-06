INSERT INTO MON.SPARK_TF_ZEB_MAST_ZONE_PARC_MONTHLY
SELECT
    A.CATEGORY
    , A.USER_STATUS
    , NVL(B.SITE_NAME,'') SITE_NAME
    , NVL(B.TOWNNAME,'') TOWNNAME
    , NVL(B.ADMINISTRATIVE_REGION,'') ADMIN_REGION
    , NVL(B.COMMERCIAL_REGION, '') COMMERC_REGION
    , COUNT(DISTINCT A.MSISDN) TOTAL_MSISDN
    , CURRENT_TIMESTAMP() INSERT_DATE
    , '###SLICE_VALUE###' BASE_MONTH
FROM
(
    SELECT
        A0.CATEGORY
        , A0.MSISDN MSISDN
        , A0.USER_STATUS
    FROM
    (
        SELECT
            EVENT_DATE
            , MOBILE_NUMBER MSISDN
            , AVAILABLE_BALANCE / 100 ACCOUNT_BALANCE
            ,(
                CASE
                    WHEN USER_STATUS = 'SR' THEN 'Suspend Request'
                    WHEN USER_STATUS = 'Y' THEN 'Active'
                    WHEN USER_STATUS = 'S' THEN 'Suspended'
                    WHEN USER_STATUS = 'DR' THEN 'Delete Request'
                    WHEN USER_STATUS = 'N' THEN 'Inactive'
                    WHEN USER_STATUS = 'W' THEN 'New user'
                    WHEN USER_STATUS = 'C' THEN 'Canceled'
                    WHEN USER_STATUS = 'CH' THEN 'Churned'
                    WHEN USER_STATUS = 'PA' THEN 'Initial state'
                    WHEN USER_STATUS = 'EX' THEN 'Expired'
                    WHEN USER_STATUS = 'DE' THEN 'Deactivated'
                    WHEN USER_STATUS = 'NP' THEN 'Not Processing'
                    ELSE USER_STATUS
                END
            ) USER_STATUS
            , MODIFIED_ON STATUS_DATE
            , CATEGORY
        FROM  
        (
            SELECT
                EVENT_DATE
                , MOBILE_NUMBER
                , CATEGORY
                , AVAILABLE_BALANCE
                , USER_STATUS
                , MODIFIED_ON
                , ROW_NUMBER() OVER (PARTITION BY MOBILE_NUMBER ORDER BY MODIFIED_ON DESC) AS RANG
                , ROW_NUMBER() OVER (PARTITION BY MOBILE_NUMBER ORDER BY USER_STATUS DESC) AS RANG1
            FROM CDR.SPARK_IT_ZEBRA_MASTER_BALANCE
            WHERE EVENT_DATE = LAST_DAY('###SLICE_VALUE###'||'-01')
            AND EVENT_TIME = '22'
        ) A00
        WHERE RANG=1 AND RANG1=1
    ) A0
    INNER JOIN
    (
        SELECT MSISDN
        FROM MON.SPARK_FT_ACCOUNT_ACTIVITY
        WHERE EVENT_DATE = DATE_ADD(LAST_DAY('###SLICE_VALUE###'||'-01'), 1) AND GP_STATUS = 'ACTIF'
    ) A1
    WHERE A1.MSISDN = A0.MSISDN
) A
LEFT JOIN
(
    SELECT
        MSISDN
        , SITE_NAME
        , TOWNNAME
        , ADMINISTRATIVE_REGION
        , COMMERCIAL_REGION
    FROM MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION
    WHERE EVENT_MONTH = '###SLICE_VALUE###'
) B
WHERE A.MSISDN = B.MSISDN
GROUP BY BASE_MONTH
    , A.CATEGORY
    , A.USER_STATUS
    , NVL(B.SITE_NAME,'') 
    , NVL(B.TOWNNAME,'') 
    , NVL(B.ADMINISTRATIVE_REGION,'') 
    , NVL(B.COMMERCIAL_REGION, '') 