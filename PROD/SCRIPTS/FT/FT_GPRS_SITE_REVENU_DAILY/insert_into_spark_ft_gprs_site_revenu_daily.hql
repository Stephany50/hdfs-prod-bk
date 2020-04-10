INSERT into FT_GPRS_SITE_REVENU_DAILY
SELECT
SESSION_DATE AS EVENT_DATE,
nvl(vdci.SITE_NAME, LOCATION_CI) SITE_NAME,
vdci.townname,
vdci.administrative_region,
vdci.commercial_region,
b.contract_type,
b.profile_name,
SUM (main_cost) main_cost,
SUM (promo_cost) promo_cost, SUM (total_cost) total_cost, SUM (bytes_sent) bytes_sent,
SUM (bytes_received) bytes_received, SUM (session_duration) session_duration, SUM (total_hits) total_hits,
SUM (total_unit) total_unit, SUM (bundle_bytes_used_volume) bundle_bytes_used_volume,
SUM (total_count) total_count, SUM (rated_cout) rated_cout,
a.OPERATOR_CODE,
SYSDATE AS INSERT_DATE,
vdci.technologie,
a.SERVED_PARTY_OFFER Profile_code,
SITE_CODE
FROM
(
select location_ci||'_'||ci_tech new_ci_tech, a.*
FROM MON.FT_A_GPRS_LOCATION a
where session_date = to_date(date_char, 'yyyymmdd') --'20/10/2019'
) a,
(select upper(profile_code) profile_code, contract_type,profile_name
from dim.dt_offer_profiles
) b,
(select ci||'_'||new_tech ci_tech, technologie, max(site_code) site_code, max(upper(site_name)) site_name, max(upper(townname)) townname, max(upper(quartier)) quartier, max(upper(region)) administrative_region
, max(upper(département)) departement, max(upper(commercial_region)) commercial_region
from
(
select
(case when technologie in ('2G', '3G') then '2G|3G'
when technologie = '4G' then '4G'
else technologie end
) new_tech
, a.*
from dim.dt_gsm_cell_code a
)
group by ci||'_'||new_tech, technologie
)vdci --@G2d: Localisation adaptée pour tenir compte du fait que nous avons des Ci 2G /3G qui coincident avec des CI 4G d'ou la necessisté de distinction.
WHERE --a.SESSION_DATE = ''TO_DATE(p_date_deb,'yyyymmdd')
a.new_ci_tech = vdci.CI_TECH(+)
AND b.profile_code(+) = a.SERVED_PARTY_OFFER
GROUP BY SESSION_DATE,
SITE_CODE,
nvl(vdci.SITE_NAME, a.LOCATION_CI),
vdci.townname,
vdci.administrative_region,
vdci.commercial_region,
b.contract_type,
b.profile_name,
a.OPERATOR_CODE,
vdci.technologie,
a.SERVED_PARTY_OFFER;