select 
    msisdn, 
    townname, 
    administrative_region, 
    event_date 
from MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
where event_date = '###SLICE_VALUE###'