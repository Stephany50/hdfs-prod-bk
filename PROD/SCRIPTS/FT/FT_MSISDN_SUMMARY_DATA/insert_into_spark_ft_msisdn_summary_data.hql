INSERT INTO MON.SPARK_FT_IMEI_TRANSACTION

SELECT

    nvl(a.IMEI, b.IMEI) IMEI,
    a.MSISDN MSISDN,
    a.SITE_VOIX SITE_VOIX,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.SITE_DATA, b.SITE_DATA) SITE_DATA,
    a.TECHNOLOGIE TECHNOLOGIE,
    a.TERMINAL_TYPE TERMINAL_TYPE,
    a.MSISDN_COUNT MSISDN_COUNT,
    a.NOMBRE_TRANSACTIONS_ENTRANT NOMBRE_TRANSACTIONS_ENTRANT,
    a.NOMBRE_TRANSACTIONS_SORTANT NOMBRE_TRANSACTIONS_SORTANT,
    a.DUREE_ENTRANT DUREE_ENTRANT,
    a.DUREE_SORTANT DUREE_SORTANT,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.VOLUME_DATA_GPRS, nvl(a.VOLUME_DATA_GPRS, 0) + nvl(b.VOLUME_DATA_GPRS, 0)) VOLUME_DATA_GPRS,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.VOLUME_DATA_GPRS_2G, nvl(a.VOLUME_DATA_GPRS_2G, 0) + nvl(b.VOLUME_DATA_GPRS_2G, 0)) VOLUME_DATA_GPRS_2G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.VOLUME_DATA_GPRS_3G, nvl(a.VOLUME_DATA_GPRS_3G, 0) + nvl(b.VOLUME_DATA_GPRS_3G, 0)) VOLUME_DATA_GPRS_3G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, a.VOLUME_DATA_GPRS_4G, nvl(a.VOLUME_DATA_GPRS_4G, 0) + nvl(b.VOLUME_DATA_GPRS_4G, 0)) VOLUME_DATA_GPRS_4G,
    a.VOLUME_DATA_OTARIE VOLUME_DATA_OTARIE,
    a.VOLUME_DATA_OTARIE_2G VOLUME_DATA_OTARIE_2G,
    a.VOLUME_DATA_OTARIE_3G VOLUME_DATA_OTARIE_3G,
    a.VOLUME_DATA_OTARIE_4G VOLUME_DATA_OTARIE_4G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR substr(b.imei, 1, 14) IS NULL, 'GPRS|', a.src_table||'GPRS|') src_table,
    CURRENT_TIMESTAMP INSERT_DATE,
    nvl(a.EVENT_DATE, b.EVENT_DATE) EVENT_DATE




FROM
TMP.SPARK_FT_IMEI_TRANSACTION_4  A

FULL OUTER JOIN

(
select DISTINCT
    EVENT_DATE,
    IMEI,
    first_value(SITE_DATA) over (partition by imei order by VOLUME_DATA desc) SITE_DATA,
    sum(VOLUME_DATA_GPRS_2G)over (partition by imei) VOLUME_DATA_GPRS_2G,
    sum(VOLUME_DATA_GPRS_3G)over (partition by imei) VOLUME_DATA_GPRS_3G,
    sum(VOLUME_DATA_GPRS_4G)over (partition by imei) VOLUME_DATA_GPRS_4G,
    sum(VOLUME_DATA)over (partition by imei) VOLUME_DATA_GPRS
    from
    (
    select
    SESSION_DATE EVENT_DATE, IMEI ,MSISDN ,SITE_DATA,
    sum(case when technologie='2G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_2G,
    sum(case when technologie='3G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_3G,
    sum(case when technologie='4G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_4G,
    sum (Volume_Total) VOLUME_DATA
    from
    (
    select
    SESSION_DATE, a.imei, a.msisdn, b.technologie, b.site_name SITE_DATA, sum(Volume_Total) Volume_Total
    from
    (select SESSION_DATE, SERVED_PARTY_MSISDN msisdn, substr(SERVED_PARTY_IMEI, 1, 14) imei, LOCATION_CI ci, BYTES_SENT + BYTES_RECEIVED Volume_Total
    from MON.SPARK_FT_CRA_GPRS
    where session_date = '###SLICE_VALUE###'
    ) a
    LEFT JOIN
    (
    select (Case when length(String(CI)) =3 then concat('00',CI) when length(String(CI)) =4 then concat('0',CI) else String(CI) end) CI,
    max(TECHNOLOGIE) TECHNOLOGIE,
    max(site_name) site_name
    from dim.dt_gsm_cell_code
    group by (Case when length(String(CI)) =3 then concat('00',CI) when length(String(CI)) =4 then concat('0',CI) else String(CI) end)
    ) b
    ON a.CI = b.CI
    group by session_date, imei,msisdn, technologie, SITE_NAME
    ) DD
    group by SESSION_DATE, IMEI, MSISDN ,SITE_DATA
    ) DDD

) B

ON (SUBSTR(A.IMEI, 1, 14) = SUBSTR(B.IMEI, 1, 14) AND A.EVENT_DATE = B.EVENT_DATE )














