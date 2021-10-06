select
nvl(date_suspension,'') as date_suspension,
nvl(msisdn,'') as msisdn,
nvl(valide_bo_telco,'') as valide_bo_telco,
nvl(date_activation,'') as date_activation,
nvl(site_name,'') as site_name,
nvl(townname,'') as townname,
nvl(administrative_region,'') as administrative_region,
nvl(commercial_region,'') as commercial_region,
nvl(last_location_day,'') as last_location_day,
nvl(statut_zsmart,'') as statut_zsmart,
nvl(raison_statut,'') as raison_statut,
nvl(insert_date,'') as insert_date,
nvl(event_date,'') as event_date
from MON.SPARK_FT_SUSPENSIONS_TELCO
where event_date='###SLICE_VALUE###'