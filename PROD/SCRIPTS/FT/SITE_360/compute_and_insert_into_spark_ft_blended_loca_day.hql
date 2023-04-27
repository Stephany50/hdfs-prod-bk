insert into mon.spark_ft_blended_loca_day
select distinct 
    msisdn
    , a.site_name
    , e.townname townname
    , e.administrative_region administrative_region
    ,(case 
         when UPPER(trim(townname)) = 'YAOUNDE' then 'YAOUNDE' 
         when upper(trim(townname)) = 'DOUALA' then 'DOUALA'
         else upper(trim(administrative_region)) 
      end) administrative_region12
    , e.commercial_region commercial_region
    , e.zone_pmo zone_pmo
    , e.category_site category_site
    , e.region_business region_business
    ,(case 
         when UPPER(trim(townname)) = 'YAOUNDE' then 'YAOUNDE' 
         when upper(trim(townname)) = 'DOUALA' then 'DOUALA'
         else upper(trim(region_business)) 
      end) region_business12
    , e.typedezone typedezone
    , '###SLICE_VALUE###' event_date
from
(
    select
        nvl(a00.msisdn, a01.msisdn) msisdn
        , nvl(a00.site_name, a01.site_name) site_name
    from
    (
        Select distinct
            msisdn
            , upper(trim(site_name)) site_name
        from mon.spark_ft_client_site_traffic_day
        where event_date = '###SLICE_VALUE###'        
    ) a00
   full join
    (
        Select distinct
            msisdn
            , upper(trim(site_name)) site_name
        from mon.spark_ft_client_last_site_day
        where event_date = '###SLICE_VALUE###' and length(msisdn) = 9 and site_name is not null
        group by msisdn, upper(trim(site_name)) 
    ) a01 on a00.msisdn=a01.msisdn
) a
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
) e on a.site_name = e.site_name
