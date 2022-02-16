SELECT 
event_date,
site_name,
duration duration_seconds,
sms_count,
insert_date
FROM mon.spark_ft_nuran_local_traffic_day
WHERE EVENT_DATE BETWEEN concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))
order by event_date

