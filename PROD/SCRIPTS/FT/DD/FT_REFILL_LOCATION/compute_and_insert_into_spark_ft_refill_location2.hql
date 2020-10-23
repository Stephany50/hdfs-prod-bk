insert into TMP.TT_REFILL_LOCATION_1
select
    a.*
    , site_name
    , site_appreciation
from
(
    select
        REfill_time
        , SENDER_MSISDN
        , RECEIVER_MSISDN
        , SENDER_CATEGORY
        , Refill_mean
        , refill_type
        , Refill_amount
    from Mon.Spark_ft_refill
    where refill_date = '###SLICE_VALUE###'
        AND REFILL_MEAN ='C2S'
        AND REFILL_TYPE  in ('RC', 'PVAS')
        AND SENDER_CATEGORY not in ('TN', 'TNT', 'WHA')
        AND TERMINATION_IND = 200
) a
left join
(
    select hour_min, served_msisdn, site_name, 'EXACTLY MINUTES SENDER' Site_appreciation 
    from TMP.TMP_RECEIVED_NOTIFICATION_SMS
) b
on (a.sender_msisdn = b.served_msisdn and substr(refill_time, 1, 4) = Hour_min)
