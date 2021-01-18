insert into TMP.TT_REFILL_LOCATION_31
select
      refill_date
    , refill_time
    , sender_msisdn
    , receiver_msisdn
    , sender_category
    , refill_mean
    , refill_type
    , refill_amount
    , nvl(a.site_name_sender, b.site_name) site_name_sender
    , nvl(a.site_appreciation_sender, b.site_appreciation) site_appreciation_sender
    , site_name_receiver
    , site_appreciation_receiver
from
(
    select *
    from TMP.TT_REFILL_LOCATION_21
) a
left outer join
(
   select
          distinct transaction_date
        , hour
        , served_msisdn
        , first_value(site_name) over(partition by hour, served_msisdn order by nbre desc) site_name
        , 'EXACTLY HOUR SENDER' Site_appreciation
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
on (refill_date = transaction_date and a.sender_msisdn = b.served_msisdn and substr(refill_time, 1, 2) = Hour)