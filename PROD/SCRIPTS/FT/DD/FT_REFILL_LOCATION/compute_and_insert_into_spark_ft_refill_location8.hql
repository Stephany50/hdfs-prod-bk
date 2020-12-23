insert into mon.spark_ft_refill_location
select
    refill_time
    , sender_msisdn
    , receiver_msisdn
    , sender_category
    , refill_mean
    , refill_type
    , refill_amount
    , site_name_sender
    , site_appreciation_sender
    , nvl(a.site_name_receiver, b.site_name) site_name_receiver
    , nvl(a.site_appreciation_receiver, b.site_appreciation) Site_Appreciation_receiver
    , current_timestamp() insert_date
    , '###SLICE_VALUE###' refill_date
from
(
    select *
    from TMP.TT_REFILL_LOCATION_61
) a
left join
(
    select 
        served_msisdn
        , first_value(site_name) over(partition by served_msisdn order by nbre desc) site_name
        , 'MOST NOTIFICATION SENDER SITE DAY' Site_appreciation
    from
    (
        select served_msisdn, site_name, count(*) nbre 
        from TMP.TMP_RECEIVED_NOTIFICATION_SMS
        group by served_msisdn, site_name 
    ) a     
) b
on (receiver_msisdn = served_msisdn)
