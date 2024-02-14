select 
nom,
prenom,
msisdn,
date_naissance,
nvl(DATEDIFF(date_expiration,current_date), 0) as expire_delay,
event_date
from MON.SPARK_FT_KYC_BDI_PP
where event_date = '###SLICE_VALUE###'