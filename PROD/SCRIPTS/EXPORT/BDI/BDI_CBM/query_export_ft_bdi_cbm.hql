select
nvl(trim(replace(msisdn,';',' ')),'') as msisdn,
nvl(trim(replace(nom,';',' ')),'') as nom,
nvl(trim(replace(prenom,';',' ')),'') as prenom,
nvl(date_naissance,'') as date_naissance
from MON.SPARK_FT_BDI_CRM_B2C
where event_date='###SLICE_VALUE###'
and trim(msisdn) rlike '^\\d+$'
and length(trim(msisdn)) = 9