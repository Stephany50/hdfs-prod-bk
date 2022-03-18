SELECT 
concat(year(date_sub('###SLICE_VALUE###', 1)), '-S', case when length(WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1))) = 1 then concat('0', WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1))) else WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1)) end) EVENT_WEEK,
site_name,
duration duration_seconds,
sms_count,
insert_date
FROM mon.spark_ft_nuran_local_traffic_day
WHERE EVENT_DATE BETWEEN date_sub('###SLICE_VALUE###', 7) and date_sub('###SLICE_VALUE###', 1)

