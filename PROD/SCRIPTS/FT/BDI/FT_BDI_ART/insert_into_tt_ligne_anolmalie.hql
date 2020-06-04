insert into TMP.TT_LIGNE_ANOMALIE
insert into TMP.TT_LIGNE_ANOMALIE
select distinct msisdn
from Mon.spark_ft_bdi_art
where event_date=to_date('###SLICE_VALUE###')
and est_suspendu = 'NON'
and  est_conforme_maj_kyc = 'NON'
AND type_personne IN ('MAJEUR','PP')
union
select distinct msisdn
from Mon.spark_ft_bdi_art
where event_date=to_date('###SLICE_VALUE###')
and est_suspendu = 'NON'
and  est_conforme_min_kyc = 'NON'
AND type_personne IN ('MINEUR')