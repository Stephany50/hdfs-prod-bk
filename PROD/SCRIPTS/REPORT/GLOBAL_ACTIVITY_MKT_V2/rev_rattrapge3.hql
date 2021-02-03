select
    if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region) administrative_region,
    sum(amount_data) amount,
    sum(amount_data)*100/(max(total_subs_amount)) poids
from (
    select a.*, sum(amount_data) over(partition by transaction_date) total_subs_amount
    from agg.spark_ft_a_subscription a
    where transaction_date='2021-01-03'
) a
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (a.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
group by  if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region) ;