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
        WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and BYTES_USED_IN_BUNDLE > 0 THEN a.msisdn
        END
    ) AS IN_BUNDLE_COUNT_30_DAYS,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and BYTES_USED_OUT_BUNDLE > 0 THEN a.msisdn
        END
    ) AS OUT_BUNDLE_COUNT_30_DAYS,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and (BYTES_USED_OUT_BUNDLE > 0 OR BYTES_USED_IN_BUNDLE > 0) THEN a.msisdn
        END
    ) AS RATED_COUNT_30_DAYS,
    SUM(
        CASE
        WHEN event_date = '###SLICE_VALUE###' and BYTES_USED_IN_BUNDLE >= 1 THEN 1
        ELSE 0
        END
    ) AS IN_BUNDLE_COUNT_1MO,
    SUM(
        CASE
        WHEN event_date = '###SLICE_VALUE###' and BYTES_USED_OUT_BUNDLE >= 1 THEN 1
        ELSE 0
        END
    ) AS OUT_BUNDLE_COUNT_1MO,
    SUM(
        CASE
        WHEN event_date = '###SLICE_VALUE###' and (BYTES_USED_OUT_BUNDLE >= 1 OR BYTES_USED_IN_BUNDLE >= 1) THEN 1
        ELSE 0
        END
    ) AS RATED_COUNT_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and BYTES_USED_IN_BUNDLE >= 1 THEN a.msisdn
        END
    ) AS IN_BUNDLE_COUNT_7_DAYS_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and BYTES_USED_OUT_BUNDLE >= 1 THEN a.msisdn
        END
    ) AS OUT_BUNDLE_COUNT_7_DAYS_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and (BYTES_USED_OUT_BUNDLE >= 1 OR BYTES_USED_IN_BUNDLE >= 1) THEN a.msisdn
        END
    ) AS RATED_COUNT_7_DAYS_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and BYTES_USED_IN_BUNDLE >= 1 THEN a.msisdn
        END
    ) AS IN_BUNDLE_COUNT_30_DAYS_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and BYTES_USED_OUT_BUNDLE >= 1 THEN a.msisdn
        END
    ) AS OUT_BUNDLE_COUNT_30_DAYS_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between date_sub('###SLICE_VALUE###', 29) and '###SLICE_VALUE###' and (BYTES_USED_OUT_BUNDLE >= 1 OR BYTES_USED_IN_BUNDLE >= 1) THEN a.msisdn
        END
    ) AS RATED_COUNT_30_DAYS_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between substring('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and BYTES_USED_IN_BUNDLE > 0 THEN a.msisdn
        END
    ) AS IN_BUNDLE_COUNT_MTD,
    COUNT(
        DISTINCT CASE
        WHEN event_date between substring('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and BYTES_USED_OUT_BUNDLE > 0 THEN a.msisdn
        END
    ) AS OUT_BUNDLE_COUNT_MTD,
    COUNT(
        DISTINCT CASE
        WHEN event_date between substring('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and (BYTES_USED_OUT_BUNDLE > 0 OR BYTES_USED_IN_BUNDLE > 0) THEN a.msisdn
        END
    ) AS RATED_COUNT_MTD,
    COUNT(
        DISTINCT CASE
        WHEN event_date between substring('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and BYTES_USED_IN_BUNDLE >= 1 THEN a.msisdn
        END
    ) AS IN_BUNDLE_COUNT_MTD_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between substring('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and BYTES_USED_OUT_BUNDLE >= 1 THEN a.msisdn
        END
    ) AS OUT_BUNDLE_COUNT_MTD_1MO,
    COUNT(
        DISTINCT CASE
        WHEN event_date between substring('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and (BYTES_USED_OUT_BUNDLE >= 1 OR BYTES_USED_IN_BUNDLE >= 1) THEN a.msisdn
        END
    ) AS RATED_COUNT_MTD_1MO,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    select
        msisdn,
        commercial_offer,
        BYTES_USED_IN_BUNDLE/(1024*1024) BYTES_USED_IN_BUNDLE,
        BYTES_USED_OUT_BUNDLE/(1024*1024) BYTES_USED_OUT_BUNDLE,
        OPERATOR_CODE,
        event_date
    from MON.SPARK_FT_DATA_CONSO_MSISDN_DAY
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
) CELL on upper(nvl(site.site_b, site.site_a))=upper(CELL.site_name)
GROUP BY COMMERCIAL_OFFER,
    OPERATOR_CODE,
    ci