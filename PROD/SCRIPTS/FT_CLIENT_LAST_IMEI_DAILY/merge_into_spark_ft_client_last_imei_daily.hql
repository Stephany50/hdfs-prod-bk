insert into MON.SPARK_FT_CLIENT_LAST_IMEI_DAILY
select 
nvl(a.msisdn,b.msisdn) as msisdn,
case when a.msisdn is null or b.msisdn is null then 
nvl(a.imei,b.imei) else trim(b.imei) end as imei,
current_timestamp() as refresh_date,
'###SLICE_VALUE###' as event_date
from (select *
from MON.SPARK_FT_CLIENT_LAST_IMEI_DAILY
where event_date = date_sub('###SLICE_VALUE###',1)) a
full outer join ( select msisdn, imei from (select msisdn, imei, transaction_count,
row_number() over(partition by msisdn order by transaction_count desc nulls last) as rang
from MON.SPARK_FT_IMEI_ONLINE
where sdate = '###SLICE_VALUE###') t
where rang = 1) b
on trim(a.msisdn) = trim(b.msisdn)