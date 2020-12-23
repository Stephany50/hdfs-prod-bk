insert into TMP.TT_REFILL_LOCATION_51
select a.refill_date
    , refill_time
    , a.sender_msisdn
    , receiver_msisdn
    , sender_category
    , refill_mean
    , refill_type
    , refill_amount
    , nvl(a.site_name_sender, b.site_name_sender) site_name_sender
    , nvl(a.site_appreciation_sender, b.site_appreciation_sender) site_appreciation_sender
    , site_name_receiver
    , site_appreciation_receiver
from
(
    select *
    from TMP.TT_REFILL_LOCATION_41
) a
left outer join
(
    select distinct refill_date
        , sender_msisdn
        , first_value(site_name_sender) over(partition by sender_msisdn order by nbre desc) site_name_sender
        , 'MOST NOTIFICATION SENDER SITE FROM RECHARGE' site_appreciation_sender
    from
    (
        select refill_date
            , sender_msisdn
            , site_name_sender
            , count(*) nbre
        from TMP.TT_REFILL_LOCATION_41 where site_name_sender is not null
        group by refill_date, sender_msisdn, site_name_sender
    ) a
) b
on (a.refill_date = b.refill_date and a.sender_msisdn = b.sender_msisdn)