
INSERT INTO TMP.SPARK_FT_IMEI_TRANSACTION_1

SELECT

    nvl(a.IMEI, b.IMEI) IMEI,
    nvl(a.MSISDN, b.MSISDN) MSISDN,
    a.SITE_VOIX,
    a.SITE_DATA,
    a.TECHNOLOGIE,
    a.TERMINAL_TYPE,
    a.MSISDN_COUNT,
    a.NOMBRE_TRANSACTIONS_ENTRANT,
    a.NOMBRE_TRANSACTIONS_SORTANT,
    a.DUREE_ENTRANT,
    a.DUREE_SORTANT,
    a.VOLUME_DATA_GPRS,
    a.VOLUME_DATA_GPRS_2G,
    a.VOLUME_DATA_GPRS_3G,
    a.VOLUME_DATA_GPRS_4G,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, nvl(a.VOLUME_DATA_OTARIE_2G, b.VOLUME_DATA_OTARIE_2G), b.VOLUME_DATA_OTARIE_2G) VOLUME_DATA_OTARIE_2G,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, nvl(a.VOLUME_DATA_OTARIE_3G, b.VOLUME_DATA_OTARIE_3G), b.VOLUME_DATA_OTARIE_3G) VOLUME_DATA_OTARIE_3G,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, nvl(a.VOLUME_DATA_OTARIE_4G, b.VOLUME_DATA_OTARIE_4G), b.VOLUME_DATA_OTARIE_4G) VOLUME_DATA_OTARIE_4G,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, nvl(a.VOLUME_DATA_OTARIE, b.VOLUME_DATA), b.VOLUME_DATA) VOLUME_DATA_OTARIE,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, 'OTARIE|', a.src_table||'OTARIE|') src_table,
    CURRENT_TIMESTAMP INSERT_DATE,
    '2020-01-25' EVENT_DATE

FROM
MON.SPARK_FT_IMEI_TRANSACTION  A
FULL OUTER JOIN
(
    select transaction_date EVENT_DATE ,MSISDN ,IMEI
    ,sum(case when RADIO_ACCESS_TECHNO in ('2G', 'Unknown') then NBYTEST else 0 end) VOLUME_DATA_OTARIE_2G
    ,sum(case when RADIO_ACCESS_TECHNO in ('3G', 'HSPA') then NBYTEST else 0 end) VOLUME_DATA_OTARIE_3G
    ,sum(case when RADIO_ACCESS_TECHNO in ('4G', 'LTE') then NBYTEST else 0 end) VOLUME_DATA_OTARIE_4G
    ,sum(NBYTEST) VOLUME_DATA
    from MON.FT_OTARIE_DATA_TRAFFIC_DAY
    where transaction_date = '2020-01-25'
    group by transaction_date, MSISDN, IMEI
) B

ON (A.MSISDN = B.MSISDN, A.EVENT_DATE = B.EVENT_DATE, SUBSTR(A.IMEI, 1, 8) = SUBSTR(B.IMEI, 1, 8) )