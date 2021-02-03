 select  b.administrative_region region,sum(rated_amount) from (

  SELECT subs.* , d.LOCATION_CI,d.administrative_region
        FROM MON.SPARK_FT_SUBSCRIPTION  subs
        LEFT JOIN (
            select msisdn, max(location_ci) location_ci, max(administrative_region) administrative_region from (
                select
                     msisdn,
                     max(site_name) site_name, max(administrative_region) administrative_region, max(location_ci) location_ci
                 from mon.spark_ft_client_last_site_day where event_date IN (select max(event_date) from mon.spark_ft_client_last_site_day where event_date between date_sub('2021-01-01',7) and '2021-01-01')
                 group by msisdn
            )t
            group by msisdn
        ) D on d.msisdn=subs.SERVED_PARTY_MSISDN
        WHERE subs.TRANSACTION_DATE = '2021-01-01'
 ) a left join ( select location_ci,max(administrative_region) administrative_region from  mon.spark_ft_client_last_site_day where event_date='2021-01-01' group by location_ci ) b on a.location_ci=b.location_ci
 group by b.administrative_region



  select administrative_region,location_ci, region from (

  SELECT subs.* , d.LOCATION_CI,d.administrative_region
        FROM MON.SPARK_FT_SUBSCRIPTION  subs
        LEFT JOIN (
            select msisdn, max(location_ci) location_ci, max(administrative_region) administrative_region from (
                select
                     msisdn,
                     max(site_name) site_name, max(administrative_region) administrative_region, max(location_ci) location_ci
                 from mon.spark_ft_client_last_site_day where event_date IN (select max(event_date) from mon.spark_ft_client_last_site_day where event_date between date_sub('2021-01-01',7) and '2021-01-01')
                 group by msisdn
            )t
            group by msisdn
        ) D on d.msisdn=subs.SERVED_PARTY_MSISDN
        WHERE subs.TRANSACTION_DATE = '2021-01-01'
 ) a left join (select ci, max(region) region from (select ci, region, row_number() over (partition by ci order by nbs desc ) rg from (select ci,region , sum(case when region is null then 0 else 1 end ) nbs from dim.spark_dt_gsm_cell_code group by ci , region ) a  ) a where rg=1 group by ci) b on cast (a.location_ci as int) = cast(b.ci as int)
 left join (select ci, max(region_territoriale) region_territoriale from dim.spark_dt_gsm_cell_code_mkt group by ci ) c on a.location_ci=c.ci
