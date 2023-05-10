SELECT distinct 
access_key,
main_imsi 
from MON.SPARK_FT_CONTRACT_SNAPSHOT 
where event_date = concat('###SLICE_VALUE###', '-01')