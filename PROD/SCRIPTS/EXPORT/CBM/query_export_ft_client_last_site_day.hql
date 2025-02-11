SELECT 
   msisdn,
   a.site_name,
   b.townname,
   b.administrative_region,
   b.commercial_region,
   b.zone_pmo zone_pmo,
   b.category_site category_site,
   b.region_business region_business,
   b.typedezone typedezone,
   a.event_date
FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
left join
(
    select
        upper(trim(site_name)) site_name
        , max(townname) townname
        , max(region) administrative_region
        , max(commercial_region) commercial_region
        , max(zonepmo) zone_pmo
        , max(technosite) category_site
        , max(typedezone) typedezone
        , max(region_bus) region_business
    from dim.spark_dt_gsm_cell_code
    group by upper(trim(site_name))
) b on upper(a.site_name) = b.site_name
WHERE EVENT_DATE= "###SLICE_VALUE###" AND length(msisdn) = 9 and a.site_name is not null