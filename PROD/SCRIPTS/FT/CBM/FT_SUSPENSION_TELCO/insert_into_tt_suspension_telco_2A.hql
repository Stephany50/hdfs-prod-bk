insert into TMP.TT_SUSPENSION_TELCO_2A
select
date_suspension,
msisdn,
valide_bo_telco,
date_activation,
site_name,
townname,
administrative_region,
commercial_region,
last_location_day,
statut_zsmart,
raison_statut,
insert_date,
event_date
from (
select a.*,
row_number() over(partition by trim(msisdn) order by date_activation desc nulls last) as rang
from TMP.TT_SUSPENSION_TELCO a) b where rang = 1