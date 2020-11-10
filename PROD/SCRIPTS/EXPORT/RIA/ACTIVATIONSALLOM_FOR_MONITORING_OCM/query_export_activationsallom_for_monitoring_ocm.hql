SELECT 
event_date,
user_id,
account_number,
msisdn,
registered_on,
id_number
from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
where event_date = "###SLICE_VALUE###"