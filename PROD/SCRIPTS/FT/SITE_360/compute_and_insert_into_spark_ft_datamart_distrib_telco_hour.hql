insert into MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR
select
    a.msisdn
    , a.hour_period event_hour
    , refill_amount
    , category_domain
    , site_name
    , town
    , region
    , commercial_region
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select
        sender_msisdn msisdn,
        substr(refill_time, 1, 2) hour_period,
        (
            case
                when sender_category in ('PS','PT','ODSA','ODS','POS','ODSA') and refill_type = 'RC' then 'PARTNER'
                when sender_category in ('INHSM','INSM','NPOS','ORNGPTNR','PPOS') and refill_type = 'RC' then 'NEW_DOMAIN'
                when sender_category in ('TN','TNT') and refill_type = 'RC' then 'OM'
                when sender_category in ('SCRATCH') and refill_type = 'RC' then 'CAG'
                when sender_category in ('NPOS','PPOS', 'INSM', 'INHSM') and refill_type = 'PVAS' then 'VAS'
                when sender_category in ('DATA_VIA_OM') then 'DATA_VIA_OM'
            end
        ) category_domain,
        sum(refill_amount) refill_amount
    from mon.spark_ft_refill
    where refill_date = '###SLICE_VALUE###'
        and termination_ind='200'
        and refill_mean ='C2S'
    group by sender_msisdn,
        substr(refill_time, 1, 2),
        (
            case when sender_category in('PS','PT','ODSA','ODS','POS','ODSA') and refill_type  ='RC' then 'PARTNER'
            when sender_category in('INHSM','INSM','NPOS','ORNGPTNR','PPOS') and refill_type  ='RC' then 'NEW_DOMAIN'
            when sender_category in('TN','TNT') and refill_type  ='RC' then 'OM'
            when sender_category in('SCRATCH') and refill_type  ='RC' then 'CAG'
            when sender_category in('NPOS','PPOS', 'INSM', 'INHSM') and refill_type = 'PVAS' then 'VAS'
            when sender_category in('DATA_VIA_OM')  then 'DATA_VIA_OM'
            end
        )
) a
left join
(
    select
        msisdn,
        hour_period,
        site_name,
        town,
        region,
        commercial_region
    from mon.spark_ft_client_site_traffic_hour
    where event_date = '###SLICE_VALUE###'
) b
on a.msisdn = b.msisdn and a.hour_period = b.hour_period
