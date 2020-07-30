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
    EVENT_DATE
FROM
(
    -- Détail users unique par date, par formule et par service
    SELECT
        f.EVENT_DATE,
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
        END AS BUNDLE
    FROM
    (
        -- Dépivotage en date, formule, service_destination, nbre_users
        SELECT
            b.EVENT_DATE,
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
            END AS USERS_COUNT
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
               EVENT_DATE,
               FORMULE,
               OPERATOR_CODE,
               ci location_ci,
               SUM(CASE WHEN SMS_COUNT > 0 THEN 1 ELSE 0 END) AS SMS_ANY_DESTINATION,
               SUM(CASE WHEN TEL_COUNT > 0 THEN 1 ELSE 0 END) AS TEL_ANY_DESTINATION,
               SUM(CASE WHEN NATIONAL_SMS_COUNT > 0 THEN 1 ELSE 0 END) AS SMS_NATIONAL,
               SUM(CASE WHEN NATIONAL_DURATION > 0 THEN 1 ELSE 0 END) AS TEL_NATIONAL,
               SUM(CASE WHEN MTN_SMS_CONSO > 0 THEN 1 ELSE 0 END) AS SMS_MTN,
               SUM(CASE WHEN MTN_DURATION > 0 THEN 1 ELSE 0 END) AS TEL_MTN,
               SUM(CASE WHEN CAMTEL_SMS_CONSO > 0 THEN 1 ELSE 0 END) AS SMS_CAMTEL,
               SUM(CASE WHEN CAMTEL_DURATION > 0 THEN 1 ELSE 0 END) AS TEL_CAMTEL,
               SUM(CASE WHEN INTERNATIONAL_SMS_CONSO > 0 THEN 1 ELSE 0 END) AS SMS_INTERNATIONAL,
               SUM(CASE WHEN INTERNATIONAL_DURATION > 0 THEN 1 ELSE 0 END) AS TEL_INTERNATIONAL,
               SUM(CASE WHEN ONNET_SMS_CONSO > 0 THEN 1 ELSE 0 END) AS SMS_ONNET,
               SUM(CASE WHEN ONNET_MAIN_TEL_CONSO + ONNET_PROMO_TEL_CONSO > 0 THEN 1 ELSE 0 END) AS TEL_ONNET,
               SUM(CASE WHEN SET_SMS_CONSO > 0 THEN 1 ELSE 0 END) AS SMS_SET,
               SUM(CASE WHEN SET_DURATION > 0 THEN 1 ELSE 0 END) AS TEL_SET,
               SUM(CASE WHEN ROAM_SMS_CONSO > 0 THEN 1 ELSE 0 END) AS SMS_ROAM,
               SUM(CASE WHEN ROAM_DURATION > 0 THEN 1 ELSE 0 END) AS TEL_ROAM,
               SUM(CASE WHEN INROAM_SMS_CONSO > 0 THEN 1 ELSE 0 END) AS SMS_INROAM,
               SUM(CASE WHEN INROAM_DURATION> 0 THEN 1 ELSE 0 END) AS TEL_INROAM,
               SUM(CASE WHEN NEXTTEL_SMS_CONSO > 0 THEN 1 ELSE 0 END) AS SMS_NEXTTEL,
               SUM(CASE WHEN NEXTTEL_DURATION > 0 THEN 1 ELSE 0 END) AS TEL_NEXTTEL,
               SUM(CASE WHEN BUNDLE_SMS_COUNT > 0 THEN 1 ELSE 0 END) AS SMS_BUNDLE,
               SUM(CASE WHEN BUNDLE_TEL_DURATION > 0 THEN 1 ELSE 0 END) AS TEL_BUNDLE
            FROM MON.SPARK_FT_CONSO_MSISDN_DAY a
            left join (
                select
                    a.msisdn,
                    max(a.site_name) site_a,
                    max(b.site_name) site_b
                 from (select * from mon.spark_ft_client_last_site_day where event_date in (select max (event_date) from  mon.spark_ft_client_last_site_day where event_date between date_sub('###SLICE_VALUE###',7) and '###SLICE_VALUE###' ) )a
                left join (
                      select * from mon.spark_ft_client_site_traffic_day where event_date in (select max (event_date) from  mon.spark_ft_client_site_traffic_day where event_date between date_sub('###SLICE_VALUE###',7) and '###SLICE_VALUE###' )
                ) b on a.msisdn = b.msisdn
                where a.event_date=date_sub('###SLICE_VALUE###',1)
                group by a.msisdn
            ) site on a.msisdn = site.msisdn
            left join (
            select  max(ci) ci,  upper(site_name) site_name from dim.dt_gsm_cell_code
            group by upper(site_name)
            ) CELL on upper(nvl(site.site_b,site.site_a))=upper(CELL.site_name)
            WHERE EVENT_DATE ='###SLICE_VALUE###'
            GROUP BY EVENT_DATE, FORMULE, OPERATOR_CODE,ci
        ) b
    ) f
)T
GROUP BY EVENT_DATE,
         FORMULE,
         SERVICE,
         OPERATOR_CODE,
         location_ci
