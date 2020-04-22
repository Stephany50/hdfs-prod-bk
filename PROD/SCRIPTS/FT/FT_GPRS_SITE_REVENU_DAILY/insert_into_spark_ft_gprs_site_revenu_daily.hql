INSERT INTO MON.SPARK_FT_GPRS_SITE_REVENU_DAILY


SELECT
    nvl(vdci.SITE_NAME, LOCATION_CI) SITE_NAME,
    vdci.townname TOWNNAME,
    vdci.administrative_region ADMINISTRATIVE_REGION,
    vdci.commercial_region COMMERCIAL_REGION,
    b.contract_type CONTRACT_TYPE,
    b.profile_name PROFILE_NAME,
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
    a.OPERATOR_CODE OPERATOR_CODE,
    vdci.technologie TECHNOLOGIE,
    a.SERVED_PARTY_OFFER PROFILE_CODE,
    SITE_CODE,
    CURRENT_TIMESTAMP AS INSERT_DATE,
    SESSION_DATE AS EVENT_DATE

FROM
(
    select
        location_ci new_ci_tech, --||'_'||ci_tech new_ci_tech, (ci_tech n'existe pas dans la table AGG.SPARK_FT_A_GPRS_LOCATION
        a.*
    FROM AGG.SPARK_FT_A_GPRS_LOCATION a
    where session_date = '2020-04-10'
) a

RIGHT JOIN

(
    select
            upper(profile_code) profile_code,
            contract_type,
            profile_name
    from dim.dt_offer_profiles
) b

ON  b.profile_code = a.SERVED_PARTY_OFFER

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
)vdci

ON a.new_ci_tech = vdci.CI_TECH

GROUP BY
    nvl(vdci.SITE_NAME, LOCATION_CI),
    vdci.townname,
    vdci.administrative_region,
    vdci.commercial_region,
    b.contract_type,
    b.profile_name,
    a.OPERATOR_CODE,
    vdci.technologie,
    a.SERVED_PARTY_OFFER,
    SITE_CODE,
    SESSION_DATE










Select count(*) from

(
SELECT
    nvl(vdci.SITE_NAME, LOCATION_CI) SITE_NAME,
    vdci.townname TOWNNAME,
    vdci.administrative_region ADMINISTRATIVE_REGION,
    vdci.commercial_region COMMERCIAL_REGION,
    b.contract_type CONTRACT_TYPE,
    b.profile_name PROFILE_NAME,
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
    a.OPERATOR_CODE OPERATOR_CODE,
    vdci.technologie TECHNOLOGIE,
    a.SERVED_PARTY_OFFER PROFILE_CODE,
    SITE_CODE,
    CURRENT_TIMESTAMP AS INSERT_DATE,
    SESSION_DATE AS EVENT_DATE

FROM
(

(
(
    select
            upper(profile_code) profile_code,
            contract_type,
            profile_name
    from dim.dt_offer_profiles
) b
,

(
    select
        location_ci new_ci_tech, --||'_'||ci_tech new_ci_tech, (ci_tech n'existe pas dans la table AGG.SPARK_FT_A_GPRS_LOCATION
        a.*
    FROM AGG.SPARK_FT_A_GPRS_LOCATION a
    where session_date = '2020-04-10'
) a

WHERE  b.profile_code = a.SERVED_PARTY_OFFER

)GGG
,

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
)vdci

WHERE a.new_ci_tech = vdci.CI_TECH

GROUP BY
    nvl(vdci.SITE_NAME, LOCATION_CI),
    vdci.townname,
    vdci.administrative_region,
    vdci.commercial_region,
    b.contract_type,
    b.profile_name,
    a.OPERATOR_CODE,
    vdci.technologie,
    a.SERVED_PARTY_OFFER,
    SITE_CODE,
    SESSION_DATE

) GGGGG

) GGGGGGG