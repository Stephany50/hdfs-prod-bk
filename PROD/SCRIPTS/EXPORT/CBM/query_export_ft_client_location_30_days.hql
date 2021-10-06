select 
* 
from mon.spark_ft_client_location_30_days 
where event_date='###SLICE_VALUE###' and length(msisdn)=9