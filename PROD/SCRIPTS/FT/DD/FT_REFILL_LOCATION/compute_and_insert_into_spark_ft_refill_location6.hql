insert into TMP.TT_REFILL_LOCATION_5
select
    refill_time
    , a.sender_msisdn
    , receiver_msisdn
    , sender_category
    , refill_mean
    , refill_type
    , refill_amount
    , nvl(a.site_name, b.site_name) site_name
    , nvl(a.site_appreciation, b.site_appreciation) Site_Appreciation
from
(
    select *
    from TMP.TT_REFILL_LOCATION_4
) a
left outer join
(
    select
        sender_msisdn
        , first_value(site_name) over(partition by sender_msisdn order by nbre desc) site_name
        , 'MOST NOTIFICATION SENDER SITE FROM RECHARGE' Site_appreciation
    from
    (
        select
            sender_msisdn
            , site_name
            , count(*) nbre 
        from TMP.TT_REFILL_LOCATION_4
        group by sender_msisdn, site_name 
    ) a     
) b
on (a.sender_msisdn = b.sender_msisdn)