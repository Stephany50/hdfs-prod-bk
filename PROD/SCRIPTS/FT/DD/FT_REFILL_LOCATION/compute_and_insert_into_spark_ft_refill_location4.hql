insert into TMP.TT_REFILL_LOCATION_3
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
    from TMP.TT_REFILL_LOCATION_2
) a
left outer join
(
   select 
        hour
        , served_msisdn
        , first_value(site_name) over(partition by hour, served_msisdn order by nbre desc) site_name
        , 'EXACTLY HOUR SENDER' Site_appreciation
    from
    (
        select 
            substr(hour_min, 1, 2) hour
            , served_msisdn
            , site_name
            , count(*) nbre 
        from TMP.TMP_RECEIVED_NOTIFICATION_SMS
        group by substr(hour_min, 1, 2), served_msisdn, site_name 
    ) a     
) b
on (a.sender_msisdn = b.served_msisdn and substr(refill_time, 1, 2) = Hour)
