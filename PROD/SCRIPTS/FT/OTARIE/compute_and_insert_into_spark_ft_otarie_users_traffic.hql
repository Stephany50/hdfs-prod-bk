INSERT INTO MON.SPARK_FT_OTARIE_USERS_TRAFFIC
select
    msisdn,
    sum(case when transaction_date='###SLICE_VALUE###' then (nbytesup) else 0 end ) traffic_daily_up ,
    sum(case when transaction_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###' then (nbytesup) else 0 end ) traffic_7_days_up ,
    sum(case when transaction_date between substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' then (nbytesup) else 0 end ) traffic_mtd_up ,
    sum(nbytesup) traffic_30_days_up,
    sum(case when transaction_date='###SLICE_VALUE###' then (nbytesdn) else 0 end ) traffic_daily_down ,
    sum(case when transaction_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###' then (nbytesdn) else 0 end ) traffic_7_days_down ,
    sum(case when transaction_date between substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' then (nbytesdn) else 0 end ) traffic_mtd_down ,
    sum(nbytesdn) traffic_30_days_down,
    sum(case when transaction_date='###SLICE_VALUE###' then (nbytest) else 0 end ) traffic_daily_test ,
    sum(case when transaction_date between date_sub('###SLICE_VALUE###',6) and '###SLICE_VALUE###' then (nbytest) else 0 end ) traffic_7_days_test ,
    sum(case when transaction_date between substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###' then (nbytest) else 0 end ) traffic_mtd_test ,
    sum(nbytest) traffic_30_days_test,
    current_timestamp insert_date,
    '###SLICE_VALUE###' transaction_date
from mon.spark_ft_otarie_data_traffic_day
where transaction_date between date_sub('###SLICE_VALUE###',29)  and '###SLICE_VALUE###'
group by msisdn



