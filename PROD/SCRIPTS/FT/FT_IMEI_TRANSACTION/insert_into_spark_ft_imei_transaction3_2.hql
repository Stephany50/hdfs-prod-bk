INSERT INTO TMP.SPARK_FT_IMEI_TRANSACTION_4

SELECT

    nvl(a.IMEI, substr (b.IMEI,1,14)) IMEI,
    nvl(a.MSISDN, b.MSISDN) MSISDN,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR substr(b.imei, 1, 14) IS NULL, a.SITE_VOIX, b.SITE_VOIX) SITE_VOIX,
    a.SITE_DATA SITE_DATA,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR substr(b.imei, 1, 14) IS NULL, a.TECHNOLOGIE, b.TECHNOLOGIE) TECHNOLOGIE,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR substr(b.imei, 1, 14) IS NULL, a.TERMINAL_TYPE, b.TERMINAL_TYPE) TERMINAL_TYPE,
    nvl(a.MSISDN_COUNT, b.MSISDN_COUNT) MSISDN_COUNT,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR substr(b.imei, 1, 14) IS NULL, a.NOMBRE_TRANSACTIONS_ENTRANT, b.NOMBRE_TRANSACTIONS_ENTRANT) NOMBRE_TRANSACTIONS_ENTRANT,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR substr(b.imei, 1, 14) IS NULL, a.NOMBRE_TRANSACTIONS_SORTANT, b.NOMBRE_TRANSACTIONS_SORTANT) NOMBRE_TRANSACTIONS_SORTANT,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR substr(b.imei, 1, 14) IS NULL, a.DUREE_ENTRANT, b.DUREE_ENTRANT) DUREE_ENTRANT,
    nvl(a.DUREE_SORTANT, b.DUREE_SORTANT) DUREE_SORTANT,
    NULL VOLUME_DATA_GPRS,
    NULL VOLUME_DATA_GPRS_2G,
    NULL VOLUME_DATA_GPRS_3G,
    NULL VOLUME_DATA_GPRS_4G,
    NULL VOLUME_DATA_OTARIE,
    NULL VOLUME_DATA_OTARIE_2G,
    NULL VOLUME_DATA_OTARIE_3G,
    NULL VOLUME_DATA_OTARIE_4G,
    IF(a.event_date IS NULL OR b.event_date IS NULL OR a.imei IS NULL OR substr(b.imei, 1, 14) IS NULL, 'MSC|', a.src_table||'MSC|') src_table,
    CURRENT_TIMESTAMP INSERT_DATE,
    nvl(a.EVENT_DATE, b.EVENT_DATE)  EVENT_DATE

FROM
TMP.SPARK_FT_IMEI_TRANSACTION_2 A

FULL OUTER JOIN

(
SELECT
    X.EVENT_DATE,
    X.IMEI,
    X.TERMINAL_TYPE,
    X.MSISDN,
    X.SITE_VOIX,
    Y.NOMBRE_TRANSACTIONS_ENTRANT,
    Y.NOMBRE_TRANSACTIONS_SORTANT,
    Y.DUREE_ENTRANT,
    Y.DUREE_SORTANT,
    Y.MSISDN_COUNT ,
    Y.TECHNOLOGIE
FROM
    (SELECT
        EVENT_DATE,
        IMEI,
        TERMINAL_TYPE,
        MSISDN,
        SITE_VOIX
    FROM
    (SELECT
        EVENT_DATE,
        IMEI,
        TERMINAL_TYPE,
        FIRST_VALUE(MSISDN) OVER (PARTITION BY IMEI ORDER BY DUREE_SORTANT +  DUREE_ENTRANT DESC) MSISDN,
        FIRST_VALUE(SITE_VOIX) OVER (PARTITION BY IMEI ORDER BY DUREE_SORTANT +  DUREE_ENTRANT DESC) SITE_VOIX
    FROM TMP.SPARK_FT_IMEI_TRANSACTION_3 ) DD
    GROUP BY EVENT_DATE, IMEI,TERMINAL_TYPE, MSISDN, SITE_VOIX) X

    INNER JOIN

    (select
        EVENT_DATE,
        IMEI,
        MAX(TECHNOLOGIE) TECHNOLOGIE,
        sum(NOMBRE_TRANSACTIONS_ENTRANT) NOMBRE_TRANSACTIONS_ENTRANT,
        sum(NOMBRE_TRANSACTIONS_SORTANT) NOMBRE_TRANSACTIONS_SORTANT,
        sum(DUREE_ENTRANT) DUREE_ENTRANT,
        sum(DUREE_SORTANT) DUREE_SORTANT,
        count(distinct msisdn)  MSISDN_COUNT
    FROM TMP.SPARK_FT_IMEI_TRANSACTION_3
    GROUP BY EVENT_DATE, IMEI) Y

    ON (X.IMEI = Y.IMEI and X.EVENT_DATE = Y.EVENT_DATE)

) B

ON (A.IMEI = SUBSTR(B.IMEI, 1, 14) AND A.EVENT_DATE = B.EVENT_DATE)





