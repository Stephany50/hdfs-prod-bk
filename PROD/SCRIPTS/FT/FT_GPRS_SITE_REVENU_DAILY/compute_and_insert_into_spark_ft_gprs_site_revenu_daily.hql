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
        from DIM.SPARK_DT_GSM_CELL_CODE a
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
        from DIM.SPARK_DT_GSM_CELL_CODE a
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












CREATE TABLE MON.SPARK_FT_GPRS_SITE_REVENU_DAILY(
  SITE_NAME                 VARCHAR(50),
  TOWNNAME                  VARCHAR(50),
  ADMINISTRATIVE_REGION     VARCHAR(50),
  COMMERCIAL_REGION         VARCHAR(40),
  CONTRACT_TYPE             VARCHAR(25),
  PROFILE_NAME              VARCHAR(50),
  MAIN_COST                 DECIMAL(20,4),
  PROMO_COST                DECIMAL(20,4),
  TOTAL_COST                DECIMAL(20,4),
  BYTES_SENT                DECIMAL(20,4),
  BYTES_RECEIVED            DECIMAL(20,4),
  SESSION_DURATION          DECIMAL(20,4),
  TOTAL_HITS                DECIMAL(20,4),
  TOTAL_UNIT                DECIMAL(20,4),
  BUNDLE_BYTES_USED_VOLUME  DECIMAL(20,4),
  TOTAL_COUNT               DECIMAL(20,4),
  RATED_COUNT               DECIMAL(20,4),
  OPERATOR_CODE             VARCHAR(25),
  TECHNOLOGIE               VARCHAR(50),
  PROFILE_CODE              VARCHAR(100),
  SITE_CODE                 VARCHAR(50),
  INSERT_DATE               TIMESTAMP
)COMMENT 'SPARK_FT_GPRS_SITE_REVENU_DAILY - FT'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


INSERT INTO MON.SPARK_FT_GPRS_SITE_REVENU_DAILY
SELECT                 
    nvl(vdci.SITE_NAME, a.LOCATION_CI) SITE_NAME,
    vdci.townname,vdci.administrative_region,
    vdci.commercial_region,b.contract_type,b.profile_name,
    SUM (nvl(main_cost,0)) main_cost,SUM (nvl(promo_cost,0)) promo_cost, 
    SUM(nvl(total_cost,0)) total_cost, SUM(nvl(bytes_sent,0)) bytes_sent,
    SUM (nvl(bytes_received,0)) bytes_received, SUM(nvl(session_duration,0)) session_duration, 
    SUM(nvl(total_hits,0)) total_hits,SUM (nvl(total_unit,0)) total_unit, 
    SUM(nvl(bundle_bytes_used_volume,0)) bundle_bytes_used_volume,
    SUM (nvl(total_count,0)) total_count, SUM(nvl(rated_cout,0)) rated_cout,
    a.OPERATOR_CODE OPERATOR_CODE ,vdci.technologie TECHNOLOGIE,
    a.SERVED_PARTY_OFFER PROFILE_CODE,SITE_CODE,
    CURRENT_TIMESTAMP INSERT_DATE,a.SESSION_DATE EVENT_DATE
FROM
(SELECT location_ci new_ci_tech, R.* FROM AGG.SPARK_FT_A_GPRS_LOCATION R WHERE session_date = '2021-04-21') a
LEFT JOIN (
 SELECT upper(profile_code) profile_code, 
 contract_type,profile_name 
 FROM dim.dt_offer_profiles
) b ON b.profile_code = a.SERVED_PARTY_OFFER
LEFT JOIN (
    SELECT ci||'_'||new_tech ci_tech, technologie, max(site_code) site_code, 
    max(upper(site_name)) site_name, max(upper(townname)) townname, 
    max(upper(quartier)) quartier, max(upper(region)) administrative_region, 
    max(upper(departement)) departement, max(upper(commercial_region)) commercial_region 
    FROM (
        SELECT 
            (CASE WHEN technologie IN ('2G', '3G') THEN '2G|3G'
                  WHEN technologie = '4G' THEN '4G'
                  ELSE technologie END
            ) new_tech
            , a.*
        FROM DIM.SPARK_DT_GSM_CELL_CODE a
    ) R GROUP BY ci||'_'||new_tech, technologie
)vdci ON a.location_ci = vdci.CI_TECH
GROUP BY a.SESSION_DATE,
 SITE_CODE,nvl(vdci.SITE_NAME, a.LOCATION_CI),
 vdci.townname,vdci.administrative_region,
 vdci.commercial_region,b.contract_type,
 b.profile_name,a.OPERATOR_CODE,
 vdci.technologie,a.SERVED_PARTY_OFFER;

