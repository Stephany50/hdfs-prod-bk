insert  into MON.SPARK_FT_LOCATION_RETAILLER_TMP
select site_name, 'DATA_VIA_OM' Category_name, SUM(rated_amount) CA, count(*) nbre, 'SUBS' Source_type,
CURRENT_TIMESTAMP insert_date
, transaction_date as refill_date
from
(
select *
from MON.SPARK_FT_SUBSCRIPTION
where transaction_date = '###SLICE_VALUE###' --'01/11/2019' and transaction_date <= '17/11/2019'
and subscription_channel = '32'
) a
LEFT JOIN
( SELECT MSISDN, SITE_NAME  FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY   --MON.FT_CLIENT_SITE_TRAFFIC_DAY
WHERE  to_date(EVENT_DATE)='###SLICE_VALUE###'
) b
ON a.SERVED_PARTY_MSISDN=b.MSISDN
group by transaction_date, site_name