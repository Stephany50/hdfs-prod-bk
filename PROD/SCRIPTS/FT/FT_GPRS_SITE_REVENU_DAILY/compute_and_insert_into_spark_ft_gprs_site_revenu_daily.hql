INSERT INTO MON.SPARK_FT_GPRS_SITE_REVENU_DAILY

select count(*) from
(
SELECT
    nvl(c.SITE_NAME, LOCATION_CI) SITE_NAME,
    c.townname TOWNNAME,
    c.administrative_region ADMINISTRATIVE_REGION,
    c.commercial_region COMMERCIAL_REGION,
    AB.contract_type CONTRACT_TYPE,
    AB.profile_name PROFILE_NAME,
    SUM (main_cost) MAIN_COST,
    SUM (promo_cost) PROMO_COST, 
    SUM (total_cost) TOTAL_COST, 
    SUM (bytes_sent) BYTES_SENT,
    SUM (bytes_received) BYTES_RECEIVED, 
    SUM (session_duration) SESSION_DURATION, 
    SUM (total_hits) TOTAL_HITS,
    SUM (total_unit) TOTAL_UNIT, 
    SUM (bundle_bytes_used_volume) BUNDLE_BYTES_USED_VOLUME,
    SUM (total_count) TOTAL_COUNT, 
    SUM (rated_cout) RATED_COUNT,
    AB.OPERATOR_CODE OPERATOR_CODE,
    c.technologie TECHNOLOGIE,
    AB.SERVED_PARTY_OFFER PROFILE_CODE,
    SITE_CODE,
    CURRENT_TIMESTAMP AS INSERT_DATE,
    SESSION_DATE AS EVENT_DATE

FROM

(
select * from

(
    select
        location_ci new_ci_tech, --||'_'||ci_tech new_ci_tech, (ci_tech n'existe pas dans la table AAB.SPARK_FT_A_GPRS_LOCATION
        a.*
    FROM AGG.SPARK_FT_A_GPRS_LOCATION a
    where session_date = '2020-04-10'
) a
x
LEFT JOIN

(
    select
        ci||'_'||new_tech ci_tech,
        technologie, max(site_code) site_code,
        max(upper(site_name)) site_name,
        max(upper(townname)) townname,
        max(upper(quartier)) quartier,
        max(upper(region)) administrative_region,
        max(upper(departement)) departement,
        max(upper(commercial_region)) commercial_region
    from
    (
        select
            (case when technologie in ('2G', '3G') then '2G|3G'
            when technologie = '4G' then '4G'
            else technologie end
            ) new_tech,
            a.*
        from dim.dt_gsm_cell_code a
        )bbb
    group by ci||'_'||new_tech, technologie
)c

ON AB.new_ci_tech = c.CI_TECH


GROUP BY
    nvl(c.SITE_NAME, LOCATION_CI),
    c.townname,
    c.administrative_region,
    c.commercial_region,
    AB.contract_type,
    AB.profile_name,
    AB.OPERATOR_CODE,
    c.technologie,
    AB.SERVED_PARTY_OFFER,
    SITE_CODE,
    SESSION_DATE

)ABC











select count(*) from

(
SELECT
    nvl(c.SITE_NAME, LOCATION_CI) SITE_NAME,
    c.townname TOWNNAME,
    c.administrative_region ADMINISTRATIVE_REGION,
    c.commercial_region COMMERCIAL_REGION,
    AB.contract_type CONTRACT_TYPE,
    AB.profile_name PROFILE_NAME,
    SUM (main_cost) MAIN_COST,
    SUM (promo_cost) PROMO_COST,
    SUM (total_cost) TOTAL_COST,
    SUM (bytes_sent) BYTES_SENT,
    SUM (bytes_received) BYTES_RECEIVED,
    SUM (session_duration) SESSION_DURATION,
    SUM (total_hits) TOTAL_HITS,
    SUM (total_unit) TOTAL_UNIT,
    SUM (bundle_bytes_used_volume) BUNDLE_BYTES_USED_VOLUME,
    SUM (total_count) TOTAL_COUNT,
    SUM (rated_cout) RATED_COUNT,
    AB.OPERATOR_CODE OPERATOR_CODE,
    c.technologie TECHNOLOGIE,
    AB.SERVED_PARTY_OFFER PROFILE_CODE,
    SITE_CODE,
    CURRENT_TIMESTAMP AS INSERT_DATE,
    SESSION_DATE AS EVENT_DATE

FROM

(
SELECT * FROM
(
    select
            upper(profile_code) profile_code,
            contract_type,
            profile_name
    from dim.dt_offer_profiles
) b

CROSS JOIN

(
    select
        location_ci new_ci_tech, --||'_'||ci_tech new_ci_tech, (ci_tech n'existe pas dans la table AAB.SPARK_FT_A_GPRS_LOCATION
        a.*
    FROM AGG.SPARK_FT_A_GPRS_LOCATION a
    where session_date = '2020-04-10'
) a

WHERE b.profile_code = a.SERVED_PARTY_OFFER

)AB

CROSS JOIN

(
    select
        ci||'_'||new_tech ci_tech,
        technologie, max(site_code) site_code,
        max(upper(site_name)) site_name,
        max(upper(townname)) townname,
        max(upper(quartier)) quartier,
        max(upper(region)) administrative_region,
        max(upper(departement)) departement,
        max(upper(commercial_region)) commercial_region
    from
    (
        select
            (case when technologie in ('2G', '3G') then '2G|3G'
            when technologie = '4G' then '4G'
            else technologie end
            ) new_tech,
            a.*
        from dim.dt_gsm_cell_code a
        )bbb
    group by ci||'_'||new_tech, technologie
)c

-- WHERE AB.new_ci_tech = c.CI_TECH

GROUP BY
    nvl(c.SITE_NAME, LOCATION_CI),
    c.townname,
    c.administrative_region,
    c.commercial_region,
    AB.contract_type,
    AB.profile_name,
    AB.OPERATOR_CODE,
    c.technologie,
    AB.SERVED_PARTY_OFFER,
    SITE_CODE,
    SESSION_DATE

)ABC


