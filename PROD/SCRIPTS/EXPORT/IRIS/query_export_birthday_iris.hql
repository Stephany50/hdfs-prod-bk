select 
msisdn,
date_naissance,
event_date
from MON.SPARK_FT_KYC_BDI_PP
where event_date = '###SLICE_VALUE###'