-- @TRAITEMENT :: INSERTION DES DONNÉES
-- EVENT_DATE, COMMERCIAL_OFFER, IN_BUNDLE_COUNT, OUT_BUNDLE_COUNT, RATED_COUNT, INSERT_DATE, OPERATOR_CODE_DAY
INSERT INTO MON.SPARK_FT_USERS_DATA_DAY
SELECT
    COMMERCIAL_OFFER,
    SUM(
        CASE
        WHEN event_date = '###SLICE_VALUE###' and BYTES_USED_IN_BUNDLE > 0 THEN 1
        ELSE 0
        END
    ) AS IN_BUNDLE_COUNT,
    SUM(
        CASE
        WHEN event_date = '###SLICE_VALUE###' and BYTES_USED_OUT_BUNDLE > 0 THEN 1
        ELSE 0
        END
    ) AS OUT_BUNDLE_COUNT,
    SUM(
        CASE
        WHEN event_date = '###SLICE_VALUE###' and (BYTES_USED_OUT_BUNDLE > 0 OR BYTES_USED_IN_BUNDLE > 0) THEN 1
        ELSE 0
        END
    ) AS RATED_COUNT,
    CURRENT_TIMESTAMP AS INSERT_DATE,
    OPERATOR_CODE,
    ci location_ci,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and BYTES_USED_IN_BUNDLE > 0 THEN a.msisdn
        END
    ) AS IN_BUNDLE_COUNT_7_DAYS,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and BYTES_USED_OUT_BUNDLE > 0 THEN a.msisdn
        END
    ) AS OUT_BUNDLE_COUNT_7_DAYS,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and (BYTES_USED_OUT_BUNDLE > 0 OR BYTES_USED_IN_BUNDLE > 0) THEN a.msisdn
        END
    ) AS RATED_COUNT_7_DAYS,
    COUNT(
        DISTINCT CASE
        WHEN BYTES_USED_IN_BUNDLE > 0 THEN a.msisdn
        END
    ) AS IN_BUNDLE_COUNT_30_DAYS,
    COUNT(
        DISTINCT CASE
        WHEN BYTES_USED_OUT_BUNDLE > 0 THEN a.msisdn
        END
    ) AS OUT_BUNDLE_COUNT_30_DAYS,
    COUNT(
        DISTINCT CASE
        WHEN BYTES_USED_OUT_BUNDLE > 0 OR BYTES_USED_IN_BUNDLE > 0 THEN a.msisdn
        END
    ) AS RATED_COUNT_30_DAYS,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    select
        msisdn,
        commercial_offer,
        BYTES_USED_IN_BUNDLE,
        BYTES_USED_OUT_BUNDLE,
        OPERATOR_CODE,
        event_date
    from MON.SPARK_FT_DATA_CONSO_MSISDN_DAY
    WHERE EVENT_DATE between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###'
) a
left join
(
    select
        a.msisdn,
        max(a.site_name) site_a,
        max(b.site_name) site_b
    from
    (
        select *
        from mon.spark_ft_client_last_site_day
        where event_date in (select max(event_date) from mon.spark_ft_client_last_site_day where event_date between date_sub('###SLICE_VALUE###',7) and '###SLICE_VALUE###')
    ) a
    left join
    (
        select *
        from mon.spark_ft_client_site_traffic_day
        where event_date in (select max(event_date) from mon.spark_ft_client_site_traffic_day where event_date between date_sub('###SLICE_VALUE###',7) and '###SLICE_VALUE###')
    ) b on a.msisdn = b.msisdn
    group by a.msisdn
) site on a.msisdn = site.msisdn
left join
(
    select
        max(ci) ci,
        upper(site_name) site_name
    from dim.dt_gsm_cell_code
    group by upper(site_name)
) CELL on upper(nvl(site.site_b, site.site_a))=upper(CELL.site_name)
GROUP BY COMMERCIAL_OFFER,
    OPERATOR_CODE,
    ci
