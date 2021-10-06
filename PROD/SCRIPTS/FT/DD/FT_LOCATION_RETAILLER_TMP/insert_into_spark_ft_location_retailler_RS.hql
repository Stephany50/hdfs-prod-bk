insert into MON.SPARK_FT_LOCATION_RETAILLER_TMP
select site_name
, case when ( SENDER_CATEGORY IN ('TN','TNT') and refill_type = 'RC')
or ( SENDER_CATEGORY IN ('WHA')  and refill_type = 'RC') then 'NOT_USED_RECEIVER'
else SENDER_CATEGORY||'_'||refill_type
end CATEGORY
, sum(refill_amount) refill_amount
, count(distinct sender_msisdn) msisdn_count    --sum(msisdn_count) msisdn_count
, 'RS' Source_type
, CURRENT_TIMESTAMP insert_date
, refill_date
from
(
select SENDER_MSISDN, refill_date, SENDER_CATEGORY, refill_type, sum(refill_amount) refill_amount
from MON.SPARK_FT_REFILL
where refill_date = '###SLICE_VALUE###'   --'01/11/2019'-- and refill_date <= '31/05/2018'
AND REFILL_MEAN ='C2S'
AND REFILL_TYPE  in ('RC', 'PVAS')
--AND SENDER_CATEGORY IN ('INHSM','INSM','NPOS','ORNGPTNR','PPOS')
and termination_ind = '200'
group by SENDER_MSISDN, refill_date, SENDER_CATEGORY, refill_type
)a
LEFT JOIN
(
select * from MON.SPARK_FT_CLIENT_LAST_SITE_DAY   --FT_CLIENT_SITE_TRAFFIC_DAY
where to_date(event_date) = '###SLICE_VALUE###' --'01/11/2019'
) b
ON a.SENDER_MSISDN = b.msisdn
group by refill_date, site_name
, case when ( SENDER_CATEGORY IN ('TN','TNT') and refill_type = 'RC')
or ( SENDER_CATEGORY IN ('WHA')  and refill_type = 'RC') then 'NOT_USED_RECEIVER'
else SENDER_CATEGORY||'_'||refill_type
end

