select
nvl(trim(replace(a.msisdn,';',' ')),'') as msisdn,
nvl(trim(replace(a.nom,';',' ')),'') as nom,
nvl(trim(replace(a.prenom,';',' ')),'') as prenom,
nvl(a.date_naissance,'') as date_naissance,
nvl(b.genre,'INCONNU') as sexe
from MON.SPARK_FT_BDI_CRM_B2C a
left join DIM.SPARK_DT_BASE_IDENTIFICATION b on a.msisdn = b.msisdn
where event_date='###SLICE_VALUE###'
and trim(a.msisdn) rlike '^\\d+$'
and length(trim(a.msisdn)) = 9