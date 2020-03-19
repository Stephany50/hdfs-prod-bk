
INSERT INTO TMP.SPARK_FT_IMEI_TRANSACTION_2

SELECT

    nvl(a.IMEI, b.IMEI) IMEI,
    nvl(a.MSISDN, b.MSISDN) MSISDN,
    a.SITE_VOIX,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, nvl(a.SITE_DATA, b.SITE_DATA), b.SITE_DATA) SITE_DATA,
    a.TECHNOLOGIE,
    a.TERMINAL_TYPE,
    a.MSISDN_COUNT,
    a.NOMBRE_TRANSACTIONS_ENTRANT,
    a.NOMBRE_TRANSACTIONS_SORTANT,
    a.DUREE_ENTRANT,
    a.DUREE_SORTANT,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, nvl(a.VOLUME_DATA_GPRS, b.VOLUME_DATA_GPRS), nvl(a.VOLUME_DATA_GPRS, 0) + nvl(b.VOLUME_DATA_GPRS, 0)) VOLUME_DATA_GPRS,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, nvl(a.VOLUME_DATA_GPRS_2G, b.VOLUME_DATA_GPRS_2G), nvl(a.VOLUME_DATA_GPRS_2G, 0) + nvl(b.VOLUME_DATA_GPRS_2G, 0)) VOLUME_DATA_GPRS_2G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, nvl(a.VOLUME_DATA_GPRS_3G, b.VOLUME_DATA_GPRS_3G), nvl(a.VOLUME_DATA_GPRS_3G, 0) + nvl(b.VOLUME_DATA_GPRS_3G, 0)) VOLUME_DATA_GPRS_3G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 14) IS NULL OR substr(b.imei, 1, 14) IS NULL, nvl(a.VOLUME_DATA_GPRS_4G, b.VOLUME_DATA_GPRS_4G), nvl(a.VOLUME_DATA_GPRS_4G, 0) + nvl(b.VOLUME_DATA_GPRS_4G, 0)) VOLUME_DATA_GPRS_4G,
    a.VOLUME_DATA_OTARIE,
    a.VOLUME_DATA_OTARIE_2G,
    a.VOLUME_DATA_OTARIE_3G,
    a.VOLUME_DATA_OTARIE_4G,
    a.src_table,
    CURRENT_TIMESTAMP INSERT_DATE,
    nvl(a.EVENT_DATE, b.EVENT_DATE) EVENT_DATE


FROM
    TMP.SPARK_FT_IMEI_TRANSACTION_1  A

FULL OUTER JOIN

    (SELECT DISTINCT
    EVENT_DATE,
    IMEI,
    MSISDN,
    first_value(SITE_DATA) over (partition by imei order by VOLUME_DATA desc) SITE_DATA,
    sum(VOLUME_DATA_GPRS_2G) over (partition by imei) VOLUME_DATA_GPRS_2G,
    sum(VOLUME_DATA_GPRS_3G) over (partition by imei) VOLUME_DATA_GPRS_3G,
    sum(VOLUME_DATA_GPRS_4G) over (partition by imei) VOLUME_DATA_GPRS_4G,
    sum(VOLUME_DATA) over (partition by imei) VOLUME_DATA_GPRS
    from
    (
    select
    SESSION_DATE EVENT_DATE, IMEI, MSISDN, SITE_DATA
    ,sum(case when technologie='2G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_2G
    ,sum(case when technologie='3G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_3G
    ,sum(case when technologie='4G' then Volume_Total else 0 end) VOLUME_DATA_GPRS_4G
    ,sum (Volume_Total) VOLUME_DATA
    from
    (
    select
    SESSION_DATE, e.imei, e.msisdn, f.technologie, f.site_name SITE_DATA, sum(Volume_Total) Volume_Total
    from
    (select SESSION_DATE, SERVED_PARTY_MSISDN msisdn, substr(SERVED_PARTY_IMEI, 1, 14) imei, LOCATION_CI ci, BYTES_SENT + BYTES_RECEIVED Volume_Total
    from MON.SPARK_FT_CRA_GPRS_POST
    where session_date = '2020-02-05'
    ) e

    LEFT JOIN

    (
    select (Case when length(String(CI)) =3 then concat('00',CI) when length(String(CI)) =4 then concat('0',CI) else String(CI) end) CI,
    max(TECHNOLOGIE) TECHNOLOGIE, max(site_name) site_name
    from dim.dt_gsm_cell_code
    group by (Case when length(String(CI)) =3 then concat('00',CI) when length(String(CI)) =4 then concat('0',CI) else String(CI) end)
    ) f

    ON e.CI = f.CI

    group by session_date, imei,msisdn, technologie, SITE_NAME
    ) DD
    group by SESSION_DATE, IMEI, MSISDN ,SITE_DATA
    ) DDD

    ) B

ON (A.EVENT_DATE = B.EVENT_DATE ) AND SUBSTR(A.IMEI, 1, 14) = SUBSTR(B.IMEI, 1, 14)

