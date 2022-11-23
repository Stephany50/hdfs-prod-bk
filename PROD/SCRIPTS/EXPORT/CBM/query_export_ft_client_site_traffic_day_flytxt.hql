select 
    msisdn, 
    townname, 
    administrative_region, 
    event_date 
from MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY 
where event_date = '###SLICE_VALUE###'