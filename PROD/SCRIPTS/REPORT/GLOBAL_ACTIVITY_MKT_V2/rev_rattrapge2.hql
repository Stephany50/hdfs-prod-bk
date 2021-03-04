insert into junk.otarie_users_day
select
    msisdn, 
    sum(case when transaction_date='2021-01-01' then (nbytesup+nbytesdn) else 0 end ) traffic_daily ,  
    sum(case when transaction_date between date_sub('2021-01-01',6) and '2021-01-01' then (nbytesup+nbytesdn) else 0 end ) traffic_7_days ,  
    sum(case when transaction_date between substring('2021-01-01', 1, 7)||'-01' AND '2021-01-01' then (nbytesup+nbytesdn) else 0 end ) traffic_mtd ,  
    sum(nbytesup+nbytesdn) traffic_30_days,
    '2021-01-01' transaction_date
from mon.spark_ft_otarie_data_traffic_day where transaction_date between date_sub('2021-01-01',29)  and '2021-01-01' group by msisdn  -- limit 10;