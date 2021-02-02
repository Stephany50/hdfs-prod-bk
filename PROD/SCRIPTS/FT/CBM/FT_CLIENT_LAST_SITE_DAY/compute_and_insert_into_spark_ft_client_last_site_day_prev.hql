INSERT INTO TMP.SPARK_TT_CLIENT_LAST_SITE_DAY
SELECT
    fn_format_msisdn_to_9digits(MSISDN) MSISDN,
    SITE_NAME,
    TOWNNAME,
    ADMINISTRATIVE_REGION,
    COMMERCIAL_REGION,
    LAST_LOCATION_DAY,
    OPERATOR_CODE,
    current_timestamp AS INSERT_DATE,
    '###SLICE_VALUE###' event_date,
    LOCATION_CI,
    location_lac
FROM
    (
        select * from MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE = date_sub('###SLICE_VALUE###', 1) and location_ci is not null
        UNION ALL
        ------- WORK ARROUND POUR TRAITER LES SITENAME AVEC LOCATION_CI NULL
        ------- A SUPPRIMER APRES L'INITIALISATION
        select
            a.msisdn,
            a.site_name,
            a.townname,
            a.administrative_region,
            a.commercial_region,
            a.last_location_day,
            a.operator_code,
            a.insert_date,
            cast(b.ci as string) location_ci,
            cast(b.lac as string) location_lac,
            event_date
         from (select * from MON.SPARK_FT_CLIENT_LAST_SITE_DAY  WHERE EVENT_DATE = date_sub('###SLICE_VALUE###', 1) and location_ci is null ) a left join (select site_name , max(ci ) ci , max(lac) lac from DIM.spark_dt_gsm_cell_code group by site_name ) b on upper(trim(nvl(a.site_name,'ND'))) = upper(trim(nvl(b.site_name,'ND')))
) T
WHERE
    OPERATOR_CODE <> 'UNKNOWN_OPERATOR'
    OR  (OPERATOR_CODE = 'UNKNOWN_OPERATOR' AND LAST_LOCATION_DAY >  date_sub('###SLICE_VALUE###', 179))