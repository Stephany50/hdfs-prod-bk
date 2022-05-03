INSERT INTO MON.SPARK_FT_CBM_HANDSET_MONTHLY
SELECT
    A.MSISDN
    , A.IMEI
    , A.TOTAL_DAYS_COUNT
    , B.REGION
    , B.TOWN
    , B.QUARTER
    , A.PH_BRAND
    , A.PH_MODEL
    , A.DATA_2G
    , A.DATA_2_5G
    , A.DATA_2_75G
    , A.DATA_3G
    , A.DATA_4G
    , C.ACTIVATION_DATE
    , C.LANG_ID
    , B.LAST_LOCATION_MONTH
    , CURRENT_TIMESTAMP() INSERT_DATE
    , '###SLICE_VALUE###' PERIOD
FROM
(
    SELECT
        IMEI
        , MSISDN
        , TOTAL_DAYS_COUNT
        , CASE WHEN TECHNOLOGIE = '2G' THEN 1 ELSE 0 END DATA_2G
        , CASE WHEN TECHNOLOGIE = '2.5G' THEN 1 ELSE 0 END DATA_2_5G
        , CASE WHEN TECHNOLOGIE = '2.75G' THEN 1 ELSE 0 END DATA_2_75G
        , CASE WHEN TECHNOLOGIE = '3G' THEN 1 ELSE 0 END DATA_3G
        , CASE WHEN TECHNOLOGIE = '4G' THEN 1 ELSE 0 END DATA_4G
        , CONSTRUCTOR PH_BRAND
        , MODEL PH_MODEL
    FROM 
    (
        select
            IMEI
            , MSISDN
            , TOTAL_DAYS_COUNT
            , CASE
            WHEN device_rank = 7 THEN '5G'
            WHEN device_rank = 6 THEN '4G'
            WHEN device_rank = 5 THEN '3G'
            WHEN device_rank = 4 THEN '2.75G'
            WHEN device_rank = 3 THEN '2.5G'
            WHEN device_rank = 2 THEN '2G'
            ELSE NULL END TECHNOLOGIE
            , CONSTRUCTOR
            , MODEL
            , ROW_NUMBER() OVER(PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) RANG
        FROM
        (
            select
                A00.IMEI IMEI
                , MSISDN
                , TOTAL_DAYS_COUNT
                , 
                (
                    CASE
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) = '5G' THEN 7
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('LTE CA', 'LTE') THEN 6
                        WHEN TRIM(UPPER(A.radio_access_techno)) = 6 THEN 6
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('3G', 'HSDPA', '3G EDGE', 'HSPA', 'HSPA+', 'HSUPA', 'UMTS') THEN 5
                        WHEN TRIM(UPPER(A.radio_access_techno)) = 5 THEN 5
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('EDGE') THEN 4
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('GPRS') THEN 3
                        WHEN TRIM(UPPER(nvl(C.TEK_RADIO, ''))) IN ('2G', 'GSM') THEN 2
                        WHEN TRIM(UPPER(C.LTE)) = 'YES' THEN 6
                        WHEN TRIM(UPPER(C.HSDPA_FLAG)) = 'T' THEN 5
                        WHEN TRIM(UPPER(C.HSUPA_FLAG)) = 'T' THEN 5
                        WHEN TRIM(UPPER(C.UMTS_FLAG)) = 'T' THEN 5
                        WHEN TRIM(UPPER(C.EDGE_FLAG)) = 'T' THEN 4
                        WHEN TRIM(UPPER(C.GPRS_FLAG)) = 'T' THEN 3
                        WHEN TRIM(UPPER(C.GSM_BAND_FLAG)) = 'T' THEN 2
                        WHEN TRIM(UPPER(A.radio_access_techno)) = 2 THEN 2
                        ELSE 1
                    END
                ) device_rank
                , supplier_name CONSTRUCTOR
                , model_name MODEL
            FROM
            (
                SELECT *
                FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY
                WHERE SMONTH = SUBSTRING('###SLICE_VALUE###', 1, 4)||SUBSTRING('###SLICE_VALUE###', 6, 2)
            ) A00
            FULL JOIN DIM.DT_NEW_HANDSET_REF C ON lpad(TRIM(SUBSTR(A00.IMEI, 1, 8)), 8, 0) = TRIM(C.TAC)
            left join 
            (
                select imei, radio_access_techno
                from DIM.SPARK_OTARIE_HANDSET
            ) A on lpad(TRIM(SUBSTR(A00.IMEI, 1, 8)), 8, 0) = lpad(TRIM(SUBSTR(A.IMEI, 1, 8)), 8, 0)
        ) A
    ) A0
    WHERE RANG = 1
) A
LEFT JOIN
(
    SELECT
        SITE_NAME QUARTER
        , MSISDN
        , ADMINISTRATIVE_REGION REGION
        , LAST_LOCATION_MONTH
        , TOWNNAME TOWN
    FROM MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION
    WHERE EVENT_MONTH = '###SLICE_VALUE###'
) B ON A.MSISDN = B.MSISDN
LEFT JOIN
(
    SELECT
        ACCESS_KEY
        , ACTIVATION_DATE
        , LANG LANG_ID
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
    WHERE EVENT_DATE = LAST_DAY('###SLICE_VALUE###'||'-01')
) C ON A.MSISDN = C.ACCESS_KEY
