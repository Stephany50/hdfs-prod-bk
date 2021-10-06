insert into TMP.TT_REFILL_LOCATION_41
select refill_date
    , refill_time
    , sender_msisdn
    , receiver_msisdn
    , sender_category
    , refill_mean
    , refill_type
    , refill_amount
    , site_name_sender
    , site_appreciation_sender
    , nvl(a.site_name_receiver, b.site_name) site_name_receiver
    , nvl(a.site_appreciation_receiver, b.site_appreciation)  site_appreciation_receiver
from
(
    select *
    from TMP.TT_REFILL_LOCATION_31
) a
left outer join
(
    select distinct transaction_date
        , hour
        , served_msisdn
        , first_value(site_name) over(partition by transaction_date, hour, served_msisdn order by nbre desc) site_name
        , 'EXACTLY HOUR RECEIVER' Site_appreciation
    from
    (
        select transaction_date
            , substr(hour_min, 1, 2) hour
            , served_msisdn
            , site_name
            , count(*) nbre
        from TMP.TMP_RECEIVED_NOTIFICATION_SMS11 where site_name is not null
        group by transaction_date, substr(hour_min, 1, 2), served_msisdn, site_name
    ) a
) b
on (refill_date = transaction_date and a.receiver_msisdn = b.served_msisdn and substr(refill_time, 1, 2) = Hour)