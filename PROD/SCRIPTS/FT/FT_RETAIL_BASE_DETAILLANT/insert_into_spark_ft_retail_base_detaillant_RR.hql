insert into MON.SPARK_FT_RETAIL_BASE_DETAILLANT
select
'INCONNU' msisdn,
site_name,
case when ( SENDER_CATEGORY IN ('INHSM','INSM','NPOS','ORNGPTNR','PPOS', 'PS','PT','ODSA','ODS','POS') and refill_type = 'RC')
or ( SENDER_CATEGORY IN ('NPOS','PPOS', 'INHSM')  and refill_type = 'PVAS') then 'NOT_USED_SENDER'
when  REFILL_MEAN = 'SCRATCH' then 'SCRATCH'
else SENDER_CATEGORY||'_'||refill_type
end CATEGORY,
sum(refill_amount) refill_amount,
count(*) transaction_count,
'RR' Source_type,
CURRENT_TIMESTAMP insert_date,
refill_date
from
(
select RECEIVER_MSISDN, refill_date, SENDER_CATEGORY, refill_type, REFILL_MEAN, sum(refill_amount) refill_amount
from MON.SPARK_FT_REFILL
where REFILL_DATE = '###SLICE_VALUE###' --'01/11/2019'-- and refill_date <= '31/05/2018'
AND REFILL_AMOUNT > 0
AND REFILL_MEAN in ('C2S', 'SCRATCH')
--AND SENDER_CATEGORY IN ('INHSM','INSM','NPOS','ORNGPTNR','PPOS')
and termination_ind = '200'
group by RECEIVER_MSISDN, refill_date, SENDER_CATEGORY, refill_type, REFILL_MEAN
) a
LEFT JOIN
(
select * from MON.SPARK_FT_CLIENT_LAST_SITE_DAY   --FT_CLIENT_SITE_TRAFFIC_DAY
where to_date(event_date) = '###SLICE_VALUE###' --'01/11/2019'
) b
ON a.RECEIVER_MSISDN = b.msisdn
group by refill_date, site_name
, case when ( SENDER_CATEGORY IN ('INHSM','INSM','NPOS','ORNGPTNR','PPOS', 'PS','PT','ODSA','ODS','POS') and refill_type = 'RC')
or ( SENDER_CATEGORY IN ('NPOS','PPOS', 'INHSM')  and refill_type = 'PVAS') then 'NOT_USED_SENDER'
when  REFILL_MEAN = 'SCRATCH' then 'SCRATCH'
else SENDER_CATEGORY||'_'||refill_type
end