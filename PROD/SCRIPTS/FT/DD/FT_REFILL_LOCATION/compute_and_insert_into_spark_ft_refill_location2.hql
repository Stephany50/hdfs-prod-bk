insert into TMP.TT_REFILL_LOCATION_11
select
    a.*
    , site_name as site_name_sender
    , site_appreciation as site_appreciation_sender
from
(
    select
        refill_date
        , REfill_time
        , SENDER_MSISDN
        , RECEIVER_MSISDN
        , SENDER_CATEGORY
        , Refill_mean
        , refill_type
        , Refill_amount
    from Mon.Spark_ft_refill
    where refill_date = '###SLICE_VALUE###'	--'2020-11-24'
        AND REFILL_MEAN ='C2S'
        --AND REFILL_TYPE  in ('RC', 'PVAS')
        --AND SENDER_CATEGORY not in ('TN', 'TNT', 'WHA')
        AND TERMINATION_IND = 200
) a
left join
(
    select transaction_date, hour_min, served_msisdn, site_name, 'EXACTLY MINUTES SENDER' Site_appreciation
    from TMP.TMP_RECEIVED_NOTIFICATION_SMS11 where site_name is not null
) b
on (refill_date = transaction_date and a.sender_msisdn = b.served_msisdn and substr(refill_time, 1, 4) = Hour_min)


