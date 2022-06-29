-- @TRAITEMENT :: INSERTION DES DONNÉES
-- EVENT_DATE, FORMULE, SERVICE, ANY_DESTINATION, NATIONAL, MTN, CAMTEL, INTERNATIONAL, ONNET, SET, ROAM, INROAM, NEXTTEL
INSERT INTO MON.SPARK_FT_USERS_DAY
SELECT
    TRIM(FORMULE) FORMULE,
    TRIM(SERVICE) SERVICE,
    SUM(ANY_DESTINATION) AS ANY_DESTINATION,
    SUM(NATIONAL) AS NATIONAL,
    SUM(MTN) AS MTN,
    SUM(CAMTEL) AS CAMTEL,
    SUM(INTERNATIONAL) AS INTERNATIONAL,
    SUM(ONNET) AS ONNET,
    SUM(`SET`) AS `SET`,
    SUM(ROAM) AS ROAM,
    SUM(INROAM) AS INROAM,
    SUM(NEXTTEL) AS NEXTTEL,
    SUM(BUNDLE) AS BUNDLE,
    OPERATOR_CODE,
    CURRENT_TIMESTAMP AS INSERT_DATE,
    location_ci,
    SUM(ANY_DESTINATION_7_DAYS) AS ANY_DESTINATION_7_DAYS,
    SUM(NATIONAL_7_DAYS) AS NATIONAL_7_DAYS,
    SUM(MTN_7_DAYS) AS MTN_7_DAYS,
    SUM(CAMTEL_7_DAYS) AS CAMTEL_7_DAYS,
    SUM(INTERNATIONAL_7_DAYS) AS INTERNATIONAL_7_DAYS,
    SUM(ONNET_7_DAYS) AS ONNET_7_DAYS,
    SUM(SET_7_DAYS) AS SET_7_DAYS,
    SUM(ROAM_7_DAYS) AS ROAM_7_DAYS,
    SUM(INROAM_7_DAYS) AS INROAM_7_DAYS,
    SUM(NEXTTEL_7_DAYS) AS NEXTTEL_7_DAYS,
    SUM(BUNDLE_7_DAYS) AS BUNDLE_7_DAYS,
    SUM(ANY_DESTINATION_30_DAYS) AS ANY_DESTINATION_30_DAYS,
    SUM(NATIONAL_30_DAYS) AS NATIONAL_30_DAYS,
    SUM(MTN_30_DAYS) AS MTN_30_DAYS,
    SUM(CAMTEL_30_DAYS) AS CAMTEL_30_DAYS,
    SUM(INTERNATIONAL_30_DAYS) AS INTERNATIONAL_30_DAYS,
    SUM(ONNET_30_DAYS) AS ONNET_30_DAYS,
    SUM(SET_30_DAYS) AS SET_30_DAYS,
    SUM(ROAM_30_DAYS) AS ROAM_30_DAYS,
    SUM(INROAM_30_DAYS) AS INROAM_30_DAYS,
    SUM(NEXTTEL_30_DAYS) AS NEXTTEL_30_DAYS,
    SUM(BUNDLE_30_DAYS) AS BUNDLE_30_DAYS,
    SUM(ANY_DESTINATION_MTD) as ANY_DESTINATION_MTD,
    SUM(NATIONAL_MTD) as NATIONAL_MTD,
    SUM(MTN_MTD) as MTN_MTD,
    SUM(CAMTEL_MTD) as CAMTEL_MTD,
    SUM(INTERNATIONAL_MTD) as INTERNATIONAL_MTD,
    SUM(ONNET_MTD) as ONNET_MTD,
    SUM(SET_MTD) as SET_MTD,
    SUM(ROAM_MTD) as ROAM_MTD,
    SUM(INROAM_MTD) as INROAM_MTD,
    SUM(NEXTTEL_MTD) as NEXTTEL_MTD,
    SUM(BUNDLE_MTD) as BUNDLE_MTD,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    -- Détail users unique par date, par formule et par service
    SELECT
        --f.EVENT_DATE,
        f.FORMULE,
        f.OPERATOR_CODE,
        f.location_ci,
        CASE
            WHEN SERVICE_DESTINATION LIKE 'SMS%' THEN 'NVX_SMS'
            WHEN SERVICE_DESTINATION LIKE 'TEL%' THEN 'VOI_VOX'
        END AS SERVICE,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ANY_DESTINATION' THEN USERS_COUNT
            ELSE 0
        END AS ANY_DESTINATION,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'NATIONAL' THEN USERS_COUNT
            ELSE 0
        END AS NATIONAL,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'MTN' THEN USERS_COUNT
            ELSE 0
        END AS MTN,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'CAMTEL' THEN USERS_COUNT
            ELSE 0
        END AS CAMTEL,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'INTERNATIONAL' THEN USERS_COUNT
            ELSE 0
        END AS INTERNATIONAL,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ONNET' THEN USERS_COUNT
            ELSE 0
        END AS ONNET,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'SET' THEN USERS_COUNT
            ELSE 0
        END AS `SET`,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ROAM' THEN USERS_COUNT
            ELSE 0
        END AS ROAM,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'INROAM' THEN USERS_COUNT
            ELSE 0
        END AS INROAM,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'NEXTTEL' THEN USERS_COUNT
            ELSE 0
        END AS NEXTTEL,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'BUNDLE' THEN USERS_COUNT
            ELSE 0
        END AS BUNDLE,

        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ANY_DESTINATION' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS ANY_DESTINATION_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'NATIONAL' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS NATIONAL_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'MTN' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS MTN_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'CAMTEL' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS CAMTEL_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'INTERNATIONAL' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS INTERNATIONAL_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ONNET' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS ONNET_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'SET' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS SET_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ROAM' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS ROAM_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'INROAM' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS INROAM_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'NEXTTEL' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS NEXTTEL_7_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'BUNDLE' THEN USERS_COUNT_7_DAYS
            ELSE 0
        END AS BUNDLE_7_DAYS,

        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ANY_DESTINATION' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS ANY_DESTINATION_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'NATIONAL' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS NATIONAL_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'MTN' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS MTN_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'CAMTEL' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS CAMTEL_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'INTERNATIONAL' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS INTERNATIONAL_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ONNET' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS ONNET_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'SET' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS SET_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ROAM' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS ROAM_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'INROAM' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS INROAM_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'NEXTTEL' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS NEXTTEL_30_DAYS,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'BUNDLE' THEN USERS_COUNT_30_DAYS
            ELSE 0
        END AS BUNDLE_30_DAYS,

        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ANY_DESTINATION' THEN USERS_COUNT_MTD
            ELSE 0
        END AS ANY_DESTINATION_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'NATIONAL' THEN USERS_COUNT_MTD
            ELSE 0
        END AS NATIONAL_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'MTN' THEN USERS_COUNT_MTD
            ELSE 0
        END AS MTN_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'CAMTEL' THEN USERS_COUNT_MTD
            ELSE 0
        END AS CAMTEL_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'INTERNATIONAL' THEN USERS_COUNT_MTD
            ELSE 0
        END AS INTERNATIONAL_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ONNET' THEN USERS_COUNT_MTD
            ELSE 0
        END AS ONNET_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'SET' THEN USERS_COUNT_MTD
            ELSE 0
        END AS SET_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'ROAM' THEN USERS_COUNT_MTD
            ELSE 0
        END AS ROAM_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'INROAM' THEN USERS_COUNT_MTD
            ELSE 0
        END AS INROAM_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'NEXTTEL' THEN USERS_COUNT_MTD
            ELSE 0
        END AS NEXTTEL_MTD,
        CASE
            WHEN SUBSTR(SERVICE_DESTINATION, 5) = 'BUNDLE' THEN USERS_COUNT_MTD
            ELSE 0
        END AS BUNDLE_MTD
    FROM
    (
        -- Dépivotage en date, formule, service_destination, nbre_users
        SELECT
            --b.EVENT_DATE,
            b.FORMULE,
            a.SERVICE_DESTINATION,
            b.OPERATOR_CODE,
            b.location_ci,
            CASE a.SERVICE_DESTINATION
                WHEN 'SMS_ANY_DESTINATION' THEN b.SMS_ANY_DESTINATION
                WHEN 'TEL_ANY_DESTINATION' THEN b.TEL_ANY_DESTINATION
                WHEN 'SMS_NATIONAL' THEN b.SMS_NATIONAL
                WHEN 'TEL_NATIONAL' THEN b.TEL_NATIONAL
                WHEN 'SMS_MTN' THEN b.SMS_MTN
                WHEN 'TEL_MTN' THEN b.TEL_MTN
                WHEN 'SMS_CAMTEL' THEN b.SMS_CAMTEL
                WHEN 'TEL_CAMTEL' THEN b.TEL_CAMTEL
                WHEN 'SMS_INTERNATIONAL' THEN b.SMS_INTERNATIONAL
                WHEN 'TEL_INTERNATIONAL' THEN b.TEL_INTERNATIONAL
                WHEN 'SMS_ONNET' THEN b.SMS_ONNET
                WHEN 'TEL_ONNET' THEN b.TEL_ONNET
                WHEN 'SMS_SET' THEN b.SMS_SET
                WHEN 'TEL_SET' THEN b.TEL_SET
                WHEN 'SMS_ROAM' THEN b.SMS_ROAM
                WHEN 'TEL_ROAM' THEN b.TEL_ROAM
                WHEN 'SMS_INROAM' THEN b.SMS_INROAM
                WHEN 'TEL_INROAM' THEN b.TEL_INROAM
                WHEN 'SMS_NEXTTEL' THEN b.SMS_NEXTTEL
                WHEN 'TEL_NEXTTEL' THEN b.TEL_NEXTTEL
                WHEN 'SMS_BUNDLE' THEN b.SMS_BUNDLE
                WHEN 'TEL_BUNDLE' THEN b.TEL_BUNDLE
            END AS USERS_COUNT,

            CASE a.SERVICE_DESTINATION
                WHEN 'SMS_ANY_DESTINATION' THEN b.SMS_ANY_DESTINATION_7_DAYS
                WHEN 'TEL_ANY_DESTINATION' THEN b.TEL_ANY_DESTINATION_7_DAYS
                WHEN 'SMS_NATIONAL' THEN b.SMS_NATIONAL_7_DAYS
                WHEN 'TEL_NATIONAL' THEN b.TEL_NATIONAL_7_DAYS
                WHEN 'SMS_MTN' THEN b.SMS_MTN_7_DAYS
                WHEN 'TEL_MTN' THEN b.TEL_MTN_7_DAYS
                WHEN 'SMS_CAMTEL' THEN b.SMS_CAMTEL_7_DAYS
                WHEN 'TEL_CAMTEL' THEN b.TEL_CAMTEL_7_DAYS
                WHEN 'SMS_INTERNATIONAL' THEN b.SMS_INTERNATIONAL_7_DAYS
                WHEN 'TEL_INTERNATIONAL' THEN b.TEL_INTERNATIONAL_7_DAYS
                WHEN 'SMS_ONNET' THEN b.SMS_ONNET_7_DAYS
                WHEN 'TEL_ONNET' THEN b.TEL_ONNET_7_DAYS
                WHEN 'SMS_SET' THEN b.SMS_SET_7_DAYS
                WHEN 'TEL_SET' THEN b.TEL_SET_7_DAYS
                WHEN 'SMS_ROAM' THEN b.SMS_ROAM_7_DAYS
                WHEN 'TEL_ROAM' THEN b.TEL_ROAM_7_DAYS
                WHEN 'SMS_INROAM' THEN b.SMS_INROAM_7_DAYS
                WHEN 'TEL_INROAM' THEN b.TEL_INROAM_7_DAYS
                WHEN 'SMS_NEXTTEL' THEN b.SMS_NEXTTEL_7_DAYS
                WHEN 'TEL_NEXTTEL' THEN b.TEL_NEXTTEL_7_DAYS
                WHEN 'SMS_BUNDLE' THEN b.SMS_BUNDLE_7_DAYS
                WHEN 'TEL_BUNDLE' THEN b.TEL_BUNDLE_7_DAYS
            END AS USERS_COUNT_7_DAYS,

            CASE a.SERVICE_DESTINATION
                WHEN 'SMS_ANY_DESTINATION' THEN b.SMS_ANY_DESTINATION_30_DAYS
                WHEN 'TEL_ANY_DESTINATION' THEN b.TEL_ANY_DESTINATION_30_DAYS
                WHEN 'SMS_NATIONAL' THEN b.SMS_NATIONAL_30_DAYS
                WHEN 'TEL_NATIONAL' THEN b.TEL_NATIONAL_30_DAYS
                WHEN 'SMS_MTN' THEN b.SMS_MTN_30_DAYS
                WHEN 'TEL_MTN' THEN b.TEL_MTN_30_DAYS
                WHEN 'SMS_CAMTEL' THEN b.SMS_CAMTEL_30_DAYS
                WHEN 'TEL_CAMTEL' THEN b.TEL_CAMTEL_30_DAYS
                WHEN 'SMS_INTERNATIONAL' THEN b.SMS_INTERNATIONAL_30_DAYS
                WHEN 'TEL_INTERNATIONAL' THEN b.TEL_INTERNATIONAL_30_DAYS
                WHEN 'SMS_ONNET' THEN b.SMS_ONNET_30_DAYS
                WHEN 'TEL_ONNET' THEN b.TEL_ONNET_30_DAYS
                WHEN 'SMS_SET' THEN b.SMS_SET_30_DAYS
                WHEN 'TEL_SET' THEN b.TEL_SET_30_DAYS
                WHEN 'SMS_ROAM' THEN b.SMS_ROAM_30_DAYS
                WHEN 'TEL_ROAM' THEN b.TEL_ROAM_30_DAYS
                WHEN 'SMS_INROAM' THEN b.SMS_INROAM_30_DAYS
                WHEN 'TEL_INROAM' THEN b.TEL_INROAM_30_DAYS
                WHEN 'SMS_NEXTTEL' THEN b.SMS_NEXTTEL_30_DAYS
                WHEN 'TEL_NEXTTEL' THEN b.TEL_NEXTTEL_30_DAYS
                WHEN 'SMS_BUNDLE' THEN b.SMS_BUNDLE_30_DAYS
                WHEN 'TEL_BUNDLE' THEN b.TEL_BUNDLE_30_DAYS
            END AS USERS_COUNT_30_DAYS,

            CASE a.SERVICE_DESTINATION
                WHEN 'SMS_ANY_DESTINATION' THEN b.SMS_ANY_DESTINATION_MTD
                WHEN 'TEL_ANY_DESTINATION' THEN b.TEL_ANY_DESTINATION_MTD
                WHEN 'SMS_NATIONAL' THEN b.SMS_NATIONAL_MTD
                WHEN 'TEL_NATIONAL' THEN b.TEL_NATIONAL_MTD
                WHEN 'SMS_MTN' THEN b.SMS_MTN_MTD
                WHEN 'TEL_MTN' THEN b.TEL_MTN_MTD
                WHEN 'SMS_CAMTEL' THEN b.SMS_CAMTEL_MTD
                WHEN 'TEL_CAMTEL' THEN b.TEL_CAMTEL_MTD
                WHEN 'SMS_INTERNATIONAL' THEN b.SMS_INTERNATIONAL_MTD
                WHEN 'TEL_INTERNATIONAL' THEN b.TEL_INTERNATIONAL_MTD
                WHEN 'SMS_ONNET' THEN b.SMS_ONNET_MTD
                WHEN 'TEL_ONNET' THEN b.TEL_ONNET_MTD
                WHEN 'SMS_SET' THEN b.SMS_SET_MTD
                WHEN 'TEL_SET' THEN b.TEL_SET_MTD
                WHEN 'SMS_ROAM' THEN b.SMS_ROAM_MTD
                WHEN 'TEL_ROAM' THEN b.TEL_ROAM_MTD
                WHEN 'SMS_INROAM' THEN b.SMS_INROAM_MTD
                WHEN 'TEL_INROAM' THEN b.TEL_INROAM_MTD
                WHEN 'SMS_NEXTTEL' THEN b.SMS_NEXTTEL_MTD
                WHEN 'TEL_NEXTTEL' THEN b.TEL_NEXTTEL_MTD
                WHEN 'SMS_BUNDLE' THEN b.SMS_BUNDLE_MTD
                WHEN 'TEL_BUNDLE' THEN b.TEL_BUNDLE_MTD
            END AS USERS_COUNT_MTD
        FROM
        (
            SELECT 'SMS_ANY_DESTINATION' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_ANY_DESTINATION' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_NATIONAL' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_NATIONAL' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_MTN' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_MTN' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_CAMTEL' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_CAMTEL' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_INTERNATIONAL' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_INTERNATIONAL' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_ONNET' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_ONNET' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_SET' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_SET' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_ROAM' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_ROAM' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_INROAM' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_INROAM' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_NEXTTEL' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_NEXTTEL' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'SMS_BUNDLE' AS SERVICE_DESTINATION  UNION ALL
            SELECT 'TEL_BUNDLE' AS SERVICE_DESTINATION 
        ) a
        CROSS JOIN
        (
            SELECT
               --EVENT_DATE,
               FORMULE,
               OPERATOR_CODE,
               ci location_ci,

               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_ANY_DESTINATION,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND TEL_COUNT > 0 THEN A.MSISDN END) AS TEL_ANY_DESTINATION,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND NATIONAL_SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_NATIONAL,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND NATIONAL_DURATION > 0 THEN A.MSISDN END) AS TEL_NATIONAL,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND MTN_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_MTN,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND MTN_DURATION > 0 THEN A.MSISDN END) AS TEL_MTN,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND CAMTEL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_CAMTEL,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND CAMTEL_DURATION > 0 THEN A.MSISDN END) AS TEL_CAMTEL,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND INTERNATIONAL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_INTERNATIONAL,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND INTERNATIONAL_DURATION > 0 THEN A.MSISDN END) AS TEL_INTERNATIONAL,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND ONNET_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_ONNET,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND ONNET_MAIN_TEL_CONSO + ONNET_PROMO_TEL_CONSO > 0 THEN A.MSISDN END) AS TEL_ONNET,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND SET_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_SET,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND SET_DURATION > 0 THEN A.MSISDN END) AS TEL_SET,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND ROAM_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_ROAM,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND ROAM_DURATION > 0 THEN A.MSISDN END) AS TEL_ROAM,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND INROAM_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_INROAM,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND INROAM_DURATION> 0 THEN A.MSISDN END) AS TEL_INROAM,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND NEXTTEL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_NEXTTEL,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND NEXTTEL_DURATION > 0 THEN A.MSISDN END) AS TEL_NEXTTEL,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND BUNDLE_SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_BUNDLE,
               COUNT(DISTINCT CASE WHEN EVENT_DATE = '###SLICE_VALUE###' AND BUNDLE_TEL_DURATION > 0 THEN A.MSISDN END) AS TEL_BUNDLE,

               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_ANY_DESTINATION_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND TEL_COUNT > 0 THEN A.MSISDN END) AS TEL_ANY_DESTINATION_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND NATIONAL_SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_NATIONAL_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND NATIONAL_DURATION > 0 THEN A.MSISDN END) AS TEL_NATIONAL_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND MTN_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_MTN_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND MTN_DURATION > 0 THEN A.MSISDN END) AS TEL_MTN_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND CAMTEL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_CAMTEL_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND CAMTEL_DURATION > 0 THEN A.MSISDN END) AS TEL_CAMTEL_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND INTERNATIONAL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_INTERNATIONAL_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND INTERNATIONAL_DURATION > 0 THEN A.MSISDN END) AS TEL_INTERNATIONAL_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND ONNET_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_ONNET_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND ONNET_MAIN_TEL_CONSO + ONNET_PROMO_TEL_CONSO > 0 THEN A.MSISDN END) AS TEL_ONNET_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND SET_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_SET_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND SET_DURATION > 0 THEN A.MSISDN END) AS TEL_SET_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND ROAM_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_ROAM_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND ROAM_DURATION > 0 THEN A.MSISDN END) AS TEL_ROAM_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND INROAM_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_INROAM_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND INROAM_DURATION> 0 THEN A.MSISDN END) AS TEL_INROAM_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND NEXTTEL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_NEXTTEL_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND NEXTTEL_DURATION > 0 THEN A.MSISDN END) AS TEL_NEXTTEL_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND BUNDLE_SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_BUNDLE_7_DAYS,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN DATE_SUB('###SLICE_VALUE###', 6) AND '###SLICE_VALUE###' AND BUNDLE_TEL_DURATION > 0 THEN A.MSISDN END) AS TEL_BUNDLE_7_DAYS,

               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_ANY_DESTINATION_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and TEL_COUNT > 0 THEN A.MSISDN END) AS TEL_ANY_DESTINATION_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and NATIONAL_SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_NATIONAL_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and NATIONAL_DURATION > 0 THEN A.MSISDN END) AS TEL_NATIONAL_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and MTN_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_MTN_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and MTN_DURATION > 0 THEN A.MSISDN END) AS TEL_MTN_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and CAMTEL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_CAMTEL_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and CAMTEL_DURATION > 0 THEN A.MSISDN END) AS TEL_CAMTEL_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and INTERNATIONAL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_INTERNATIONAL_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and INTERNATIONAL_DURATION > 0 THEN A.MSISDN END) AS TEL_INTERNATIONAL_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and ONNET_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_ONNET_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and ONNET_MAIN_TEL_CONSO + ONNET_PROMO_TEL_CONSO > 0 THEN A.MSISDN END) AS TEL_ONNET_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and SET_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_SET_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and SET_DURATION > 0 THEN A.MSISDN END) AS TEL_SET_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and ROAM_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_ROAM_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and ROAM_DURATION > 0 THEN A.MSISDN END) AS TEL_ROAM_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and INROAM_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_INROAM_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and INROAM_DURATION> 0 THEN A.MSISDN END) AS TEL_INROAM_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and NEXTTEL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_NEXTTEL_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and NEXTTEL_DURATION > 0 THEN A.MSISDN END) AS TEL_NEXTTEL_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and BUNDLE_SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_BUNDLE_30_DAYS,
               COUNT(DISTINCT CASE WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and BUNDLE_TEL_DURATION > 0 THEN A.MSISDN END) AS TEL_BUNDLE_30_DAYS,

               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_ANY_DESTINATION_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND TEL_COUNT > 0 THEN A.MSISDN END) AS TEL_ANY_DESTINATION_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND NATIONAL_SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_NATIONAL_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND NATIONAL_DURATION > 0 THEN A.MSISDN END) AS TEL_NATIONAL_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND MTN_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_MTN_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND MTN_DURATION > 0 THEN A.MSISDN END) AS TEL_MTN_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND CAMTEL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_CAMTEL_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND CAMTEL_DURATION > 0 THEN A.MSISDN END) AS TEL_CAMTEL_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND INTERNATIONAL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_INTERNATIONAL_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND INTERNATIONAL_DURATION > 0 THEN A.MSISDN END) AS TEL_INTERNATIONAL_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND ONNET_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_ONNET_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND ONNET_MAIN_TEL_CONSO + ONNET_PROMO_TEL_CONSO > 0 THEN A.MSISDN END) AS TEL_ONNET_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND SET_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_SET_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND SET_DURATION > 0 THEN A.MSISDN END) AS TEL_SET_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND ROAM_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_ROAM_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND ROAM_DURATION > 0 THEN A.MSISDN END) AS TEL_ROAM_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND INROAM_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_INROAM_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND INROAM_DURATION> 0 THEN A.MSISDN END) AS TEL_INROAM_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND NEXTTEL_SMS_CONSO > 0 THEN A.MSISDN END) AS SMS_NEXTTEL_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND NEXTTEL_DURATION > 0 THEN A.MSISDN END) AS TEL_NEXTTEL_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND BUNDLE_SMS_COUNT > 0 THEN A.MSISDN END) AS SMS_BUNDLE_MTD,
               COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' AND BUNDLE_TEL_DURATION > 0 THEN A.MSISDN END) AS TEL_BUNDLE_MTD
            FROM
            (
                select *
                from MON.SPARK_FT_CONSO_MSISDN_DAY a
                WHERE EVENT_DATE between date_sub('###SLICE_VALUE###', 30) and '###SLICE_VALUE###'
            ) a
            left join
            (
                select
                    a.msisdn,
                    max(a.site_name) site_a,
                    max(b.site_name) site_b
                from (select msisdn, FIRST_VALUE(site_name) OVER(PARTITION BY msisdn ORDER BY insert_date DESC) site_name from mon.spark_ft_client_last_site_day where event_date >= date_sub('###SLICE_VALUE###', 7) )a
                left join (
                select msisdn, FIRST_VALUE(site_name) OVER(PARTITION BY msisdn ORDER BY refresh_date DESC) site_name from mon.spark_ft_client_site_traffic_day where event_date >= date_sub('###SLICE_VALUE###', 7) 
                ) b on a.msisdn = b.msisdn
                group by a.msisdn
            ) site on a.msisdn = site.msisdn
            left join
            (
                select
                    max(ci) ci,
                    upper(site_name) site_name
                from DIM.SPARK_DT_GSM_CELL_CODE
                group by upper(site_name)
            ) CELL on upper(nvl(site.site_b,site.site_a)) = upper(CELL.site_name)
            GROUP BY --EVENT_DATE,
                FORMULE,
                OPERATOR_CODE,
                ci
        ) b
    ) f
) T
GROUP BY FORMULE,
    SERVICE,
    OPERATOR_CODE,
    location_ci
