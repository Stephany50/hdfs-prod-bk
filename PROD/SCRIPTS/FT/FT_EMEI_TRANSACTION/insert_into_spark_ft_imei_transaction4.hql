INSERT INTO MON.FT_IMEI_TRANSACTION

SELECT

    nvl(a.IMEI, substr (b.IMEI,1,14)) IMEI,
    nvl(a.MSISDN, b.MSISDN) MSISDN,
   nvl(a.SITE_VOIX, b.SITE_VOIX) SITE_VOIX,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.SITE_DATA, b.SITE_DATA) SITE_DATA,
   nvl(a.TECHNOLOGIE, b.TECHNOLOGIE) TECHNOLOGIE,
    nvl(a.TERMINAL_TYPE, b.TERMINAL_TYPE) TERMINAL_TYPE,
    nvl(a.MSISDN_COUNT, b.MSISDN_COUNT) MSISDN_COUNT,
    nvl(a.NOMBRE_TRANSACTIONS_ENTRANT, b.NOMBRE_TRANSACTIONS_ENTRANT) NOMBRE_TRANSACTIONS_ENTRANT,
    nvl(a.NOMBRE_TRANSACTIONS_SORTANT, b.NOMBRE_TRANSACTIONS_SORTANT) NOMBRE_TRANSACTIONS_SORTANT,
    nvl(a.DUREE_ENTRANT, b.DUREE_ENTRANT) DUREE_ENTRANT,
    nvl(a.DUREE_SORTANT, b.DUREE_SORTANT) DUREE_SORTANT,
     IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.VOLUME_DATA_GPRS, nvl(a.VOLUME_DATA_GPRS, 0) + nvl(b.VOLUME_DATA_GPRS, 0)) VOLUME_DATA_GPRS,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.VOLUME_DATA_GPRS_2G, nvl(a.VOLUME_DATA_GPRS_2G, 0) + nvl(b.VOLUME_DATA_GPRS_2G, 0)) VOLUME_DATA_GPRS_2G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.VOLUME_DATA_GPRS_3G, nvl(a.VOLUME_DATA_GPRS_3G, 0) + nvl(b.VOLUME_DATA_GPRS_3G, 0)) VOLUME_DATA_GPRS_3G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.VOLUME_DATA_GPRS_4G, nvl(a.VOLUME_DATA_GPRS_4G, 0) + nvl(b.VOLUME_DATA_GPRS_4G, 0)) VOLUME_DATA_GPRS_4G,
    nvl(a.VOLUME_DATA_OTARIE, b.VOLUME_DATA_OTARIE) VOLUME_DATA_OTARIE,
    nvl(a.VOLUME_DATA_OTARIE_2G, b.VOLUME_DATA_OTARIE_2G) VOLUME_DATA_OTARIE_2G,
    nvl(a.VOLUME_DATA_OTARIE_3G, b.VOLUME_DATA_OTARIE_3G) VOLUME_DATA_OTARIE_3G,
    nvl(a.VOLUME_DATA_OTARIE_4G, b.VOLUME_DATA_OTARIE_4G) VOLUME_DATA_OTARIE_4G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR b.substr(a.imei, 1, 14) IS NULL, 'GPRS|', a.src_table||'GPRS|') src_table,
    CURRENT_TIMESTAMP INSERT_DATE,
    '2020-01-25' EVENT_DATE



FROM
TMP.SPARK_FT_IMEI_TRANSACTION_3  A
FULL OUTER JOIN

(select DISTINCT
    EVENT_DATE
    ,IMEI
    ,first_value(SITE_DATA) over (partition by imei order by VOLUME_DATA desc) SITE_DATA
    ,sum(VOLUME_DATA_GPRS_2G)over (partition by imei) VOLUME_DATA_GPRS_2G
    ,sum(VOLUME_DATA_GPRS_3G)over (partition by imei) VOLUME_DATA_GPRS_3G
    ,sum(VOLUME_DATA_GPRS_4G)over (partition by imei) VOLUME_DATA_GPRS_4G
    , sum(VOLUME_DATA)over (partition by imei) VOLUME_DATA_GPRS
    from
    (
    select
    SESSION_DATE EVENT_DATE, IMEI ,MSISDN ,SITE_DATA
    ,sum(case when technologie='2G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_2G
    ,sum(case when technologie='3G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_3G
    ,sum(case when technologie='4G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_4G
    ,sum (Volume_Total) VOLUME_DATA
    from
    (
    select
    SESSION_DATE, SUBSTR(A.IMEI, 1, 14), a.msisdn, b.technologie, b.site_name SITE_DATA, sum(Volume_Total) Volume_Total
    from
    (select SESSION_DATE, SERVED_PARTY_MSISDN msisdn, substr(SERVED_PARTY_IMEI, 1, 14) imei, LOCATION_CI ci, BYTES_SENT + BYTES_RECEIVED Volume_Total
    from FT_CRA_GPRS
    where session_date = '###SLICE_VALUE###'
    ) a
    ,
    (
    select (Case when length(CI) =3 then concat('00',CI)
    when length(CI) =4 then concat('0',CI)
    else to_char(CI) end) CI, max(TECHNOLOGIE) TECHNOLOGIE, max(site_name) site_name
    from dim.dt_gsm_cell_code
    group by Case when length(CI) =3 then concat('00',CI)
    when length(CI) =4 then concat('0',CI)
    else to_char(CI) end
    ) b
    WHERE a.CI = b.CI(+)
    group by session_date, imei,msisdn, technologie, SITE_NAME
    )
    group by SESSION_DATE, IMEI, MSISDN ,SITE_DATA
    )
) B

ON (SUBSTR(A.IMEI, 1, 14) = SUBSTR(B.IMEI, 1, 14), A.EVENT_DATE = B.EVENT_DATE )
