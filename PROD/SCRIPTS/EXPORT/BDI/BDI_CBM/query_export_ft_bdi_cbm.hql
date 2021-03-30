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
DATE_NAISSANCE
from Mon.spark_ft_bdi_b2b
where event_date='###SLICE_VALUE###'
) a
where trim(msisdn) rlike '^\\d+$'
and length(trim(msisdn)) = 9