select
nvl(trim(replace(msisdn,';',' ')),'') as msisdn,
nvl(trim(replace(nom,';',' ')),'') as nom,
nvl(trim(replace(prenom,';',' ')),'') as prenom,
nvl(date_naissance,'') as date_naissance
from (
select FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn,nom,prenom,date_naissance
from Mon.spark_ft_bdi_1A
where event_date='###SLICE_VALUE###'
union all
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn,nom,prenom,
(CASE
WHEN trim(DATE_NAISSANCE) IS NULL OR trim(DATE_NAISSANCE) = '' THEN NULL
WHEN trim(DATE_NAISSANCE) LIKE '%-%'
THEN cast(substr(trim(DATE_NAISSANCE),1,10) as DATE)
WHEN trim(DATE_NAISSANCE) LIKE '%/%'
THEN cast(translate(substr(trim(DATE_NAISSANCE),1,10),'/','-') as DATE)
ELSE NULL
END) DATE_NAISSANCE
from Mon.spark_ft_bdi_b2b
where event_date='###SLICE_VALUE###'
) a
where trim(msisdn) rlike '^\\d+$'
and length(trim(msisdn)) = 9