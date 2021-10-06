insert into TMP.TT_REFILL_LOCATION_21
select
      refill_date
    , refill_time
    , sender_msisdn
    , receiver_msisdn
    , sender_category
    , refill_mean
    , refill_type
    , refill_amount
    , site_name_sender
    , site_appreciation_sender
    , site_name site_name_receiver
    , site_appreciation  Site_appreciation_receiver
from
(
    select *
    from TMP.TT_REFILL_LOCATION_11
) a
left outer join
(
    select transaction_date, hour_min, served_msisdn, site_name, 'EXACTLY MINUTES RECEIVER' Site_appreciation
    from TMP.TMP_RECEIVED_NOTIFICATION_SMS11 where site_name is not null
) b
on (refill_date = transaction_date and a.receiver_msisdn = b.served_msisdn and substr(refill_time, 1, 4) = Hour_min)