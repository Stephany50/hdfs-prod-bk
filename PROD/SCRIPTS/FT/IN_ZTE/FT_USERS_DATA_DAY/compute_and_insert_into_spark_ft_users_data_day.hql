-- @TRAITEMENT :: INSERTION DES DONNÉES
-- EVENT_DATE, COMMERCIAL_OFFER, IN_BUNDLE_COUNT, OUT_BUNDLE_COUNT, RATED_COUNT, INSERT_DATE, OPERATOR_CODE_DAY
INSERT INTO MON.SPARK_FT_USERS_DATA_DAY
SELECT
       COMMERCIAL_OFFER,
       SUM(CASE WHEN BYTES_USED_IN_BUNDLE > 0 THEN 1 ELSE 0 END) AS IN_BUNDLE_COUNT,
       SUM(CASE WHEN BYTES_USED_OUT_BUNDLE > 0 THEN 1 ELSE 0 END) AS OUT_BUNDLE_COUNT,
       SUM(CASE WHEN BYTES_USED_OUT_BUNDLE > 0 OR BYTES_USED_IN_BUNDLE > 0 THEN 1 ELSE 0 END) AS RATED_COUNT,
       CURRENT_TIMESTAMP AS INSERT_DATE,
       OPERATOR_CODE,
       ci location_ci,
       EVENT_DATE
FROM mon.SPARK_FT_DATA_CONSO_MSISDN_DAY a
left join (
    select
        a.msisdn,
        max(a.site_name) site_a,
        max(b.site_name) site_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date=date_sub('2020-04-29',1)
    ) b on a.msisdn = b.msisdn
    where a.event_date=date_sub('2020-04-29',1)
    group by a.msisdn
) site on a.msisdn = site.msisdn
left join (
    select  max(ci) ci,  upper(site_name) site_name from dim.dt_gsm_cell_code
    group by upper(site_name)
) CELL on upper(nvl(site.site_b,site.site_a))=upper(CELL.site_name)
WHERE EVENT_DATE = '2020-04-29'
GROUP BY EVENT_DATE,
       COMMERCIAL_OFFER,
       OPERATOR_CODE,
       ci