insert into TMP.TT_LIGNE_ANOMALIE
select distinct msisdn
from Mon.spark_ft_bdi_art
where event_date=to_date('###SLICE_VALUE###')
and est_suspendu = 'OUI'
and (est_conforme_maj_kyc = 'NON'
or est_conforme_min_kyc = 'NON')
