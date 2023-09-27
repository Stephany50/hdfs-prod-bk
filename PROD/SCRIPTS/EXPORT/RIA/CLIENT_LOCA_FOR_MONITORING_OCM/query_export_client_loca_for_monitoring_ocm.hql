SELECT 
A.msisdn,
vendeur_om,
vendeur_telco,
vendeur_sim,
site_name,
townname ,
administrative_region,
last_location_day,
location_ci,
location_lac,
quartier, 
longitude,
latitude
from 
(SELECT * FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date='###SLICE_VALUE###') A
LEFT JOIN 
(SELECT distinct MSISDN, 'oui' vendeur_om from dim.ref_marchand_om) C on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(C.MSISDN)
LEFT JOIN 
(SELECT distinct MSISDN, 'oui' vendeur_telco from dim.ref_marchand_telco) D on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(D.MSISDN)
LEFT JOIN 
(SELECT distinct MSISDN, 'oui' vendeur_sim from dim.ref_marchand_SIM) E on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(E.MSISDN)
LEFT JOIN 
(select ci, lac, max(quartier) quartier, max(longitude) longitude, max(latitude) latitude from dim.spark_dt_gsm_cell_code  group by ci, lac) B
ON UPPER(TRIM(A.location_ci)) = UPPER(TRIM(B.ci)) AND UPPER(TRIM(A.location_lac))= UPPER(TRIM(B.lac))
where vendeur_telco is not null or vendeur_om is not null or vendeur_sim is not null