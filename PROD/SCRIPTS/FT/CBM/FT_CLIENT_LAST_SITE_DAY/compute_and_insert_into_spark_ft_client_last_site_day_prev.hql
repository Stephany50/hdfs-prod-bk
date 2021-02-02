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
        select
            a.msisdn,
            max(a.site_name)site_name,
            max(a.townname)townname,
            max(a.administrative_region)administrative_region,
            max(a.commercial_region)commercial_region,
            max(a.last_location_day) last_location_day,
            max(a.operator_code)operator_code,
            max(a.insert_date) insert_date,
            max(a.location_ci) location_ci,
            max(a.location_lac) location_lac,
            event_date
        from MON.SPARK_FT_CLIENT_LAST_SITE_DAY a
        WHERE EVENT_DATE = date_sub('###SLICE_VALUE###', 1) and location_ci is not null
        group by msisdn, event_date

        UNION ALL
        ------- WORK ARROUND POUR TRAITER LES SITENAME AVEC LOCATION_CI NULL
        ------- A SUPPRIMER APRES L'INITIALISATION
        select
            a.msisdn,
            max(a.site_name)site_name,
            max(a.townname)townname,
            max(a.administrative_region)administrative_region,
            max(a.commercial_region)commercial_region,
            max(a.last_location_day) last_location_day,
            max(a.operator_code)operator_code,
            max(a.insert_date) insert_date,
            max(cast(b.ci as string)) location_ci,
            max(cast(b.lac as string)) location_lac,
            event_date
         from (select * from MON.SPARK_FT_CLIENT_LAST_SITE_DAY  WHERE EVENT_DATE = date_sub('###SLICE_VALUE###', 1) and location_ci is null ) a left join (select site_name , max(ci ) ci , max(lac) lac from DIM.spark_dt_gsm_cell_code group by site_name ) b on upper(trim(nvl(a.site_name,'ND'))) = upper(trim(nvl(b.site_name,'ND')))
         group by msisdn, event_date
) T
WHERE
    OPERATOR_CODE <> 'UNKNOWN_OPERATOR'
    OR  (OPERATOR_CODE = 'UNKNOWN_OPERATOR' AND LAST_LOCATION_DAY >  date_sub('###SLICE_VALUE###', 179))
