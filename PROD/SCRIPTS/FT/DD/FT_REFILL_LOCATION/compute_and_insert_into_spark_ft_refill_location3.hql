insert into TMP.TT_REFILL_LOCATION_2
select
    refill_time
    , sender_msisdn
    , receiver_msisdn
    , sender_category
    , refill_mean
    , refill_type
    , refill_amount
    , nvl(a.site_name, b.site_name) site_name
    , nvl(a.site_appreciation, b.site_appreciation)  Site_Appreciation
from
(
    select *
    from TMP.TT_REFILL_LOCATION_1
) a
left outer join
(
    select hour_min, served_msisdn, site_name, 'EXACTLY MINUTES RECEIVER' Site_appreciation 
    from TMP.TMP_RECEIVED_NOTIFICATION_SMS
) b
on (a.receiver_msisdn = b.served_msisdn and substr(refill_time, 1, 4) = Hour_min)