
insert into TMP.TT_MSISDN_DATA_SITE
select distinct
    session_date,
    msisdn,
    first_value(imei)over (partition by msisdn order by bytes_used desc) imei,
    first_value(site_name)over(partition by msisdn order by bytes_used desc) site_name,
    sum(Bytes_used)over(partition by msisdn) Bytes_Used,
    sum(total_cost)over(partition by msisdn) total_cost,
    sum(bundle_volume_used)over(partition by msisdn) bundle_volume_used,
    CURRENT_TIMESTAMP insert_date
from
(
    select
        session_date,
        served_party_msisdn msisdn,
        substr(served_party_imei, 1, 14) imei,
        site_name,
        sum(bytes_sent+bytes_Received) Bytes_Used,
        sum(total_cost)total_cost,
        sum(main_cost) Main_cost,
        sum(promo_cost) Promo_cost,
        sum(bundle_bytes_used_volume)bundle_Volume_used,
        sum(total_count) transaction_count,
        sum(rated_count) rated_count
    from (
        select * from  MON.SPARK_FT_MSISDN_IMEI_DATA_LOCATION  where session_date='###SLICE_VALUE###'
    ) imei
    left join (
        select cast(ci as STRING) as ci, site_name from DEFAULT.VW_SDT_CI_INFO_NEW
    )  site  on lpad(imei.location_ci,5,0) = lpad(site.ci,5,0)
    group by
        session_date,
        served_party_msisdn,
        substr(served_party_imei, 1, 14),
        site_name
)T

