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
                when refill_mean in ('SCRATCH') and refill_type = 'RC' then 'CAG'
                when sender_category in ('NPOS','PPOS', 'INSM', 'INHSM') and refill_type = 'PVAS' then 'VAS'
                else 'NEW_DOMAIN'
            end
        ) category_domain,
        sum(refill_amount) refill_amount
    from mon.spark_ft_refill
    where refill_date = '###SLICE_VALUE###'
        and termination_ind='200'
        and refill_mean in ('C2S', 'SCRATCH')
        and sender_category not in ('TN', 'TNT', 'WHA')
    group by sender_msisdn,
        substr(refill_time, 1, 2),
        (
            case
                when sender_category in ('PS','PT','ODSA','ODS','POS','ODSA') and refill_type = 'RC' then 'PARTNER'
                when refill_mean in ('SCRATCH') and refill_type = 'RC' then 'CAG'
                when sender_category in ('NPOS','PPOS', 'INSM', 'INHSM') and refill_type = 'PVAS' then 'VAS'
                else 'NEW_DOMAIN'
            end
        )
    union all
    select
        receiver_msisdn msisdn,
        substr(refill_time, 1, 2) hour_period,
        (
            case
                when sender_category in ('TN','TNT') and refill_type = 'RC' then 'OM'
                when SENDER_CATEGORY IN ('WHA') and refill_type = 'RC' then 'WHA'
            end
        ) category_domain,
        sum(refill_amount) refill_amount
    from mon.spark_ft_refill
    where refill_date = '###SLICE_VALUE###'
        and termination_ind='200'
        and refill_mean ='C2S'
        and sender_category in ('TN', 'TNT', 'WHA')
    group by receiver_msisdn,
        substr(refill_time, 1, 2),
        (
            case
                when sender_category in ('TN','TNT') and refill_type = 'RC' then 'OM'
                when SENDER_CATEGORY IN ('WHA') and refill_type = 'RC' then 'WHA'
            end
        )
    union ALL
    select
        served_party_msisdn msisdn,
        substr(transaction_time, 1, 2) hour_period,
        'DATA_VIA_OM' category_domain,
        SUM(rated_amount) refill_amount
    from MON.SPARK_FT_SUBSCRIPTION
    where transaction_date = '###SLICE_VALUE###'
        and subscription_channel = '32'
    group by served_party_msisdn,
        substr(transaction_time, 1, 2)
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
