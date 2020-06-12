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
union
select distinct  A1.MSISDN from
(select *
from Mon.spark_ft_bdi_art
WHERE EVENT_DATE = to_date('###SLICE_VALUE###')
and not(NUMERO_PIECE is  null or trim(NUMERO_PIECE) = '')
and est_suspendu = 'NON'
and TYPE_PERSONNE IN ('MAJEUR','PP', 'MINEUR')
and statut_derogation = 'NON') A1
JOIN
(SELECT NUMERO_PIECE, count(*)
from Mon.spark_ft_bdi_art
WHERE EVENT_DATE = to_date('###SLICE_VALUE###')
and not(NUMERO_PIECE is  null or trim(NUMERO_PIECE) = '')
and est_suspendu = 'NON'
and TYPE_PERSONNE IN ('MAJEUR','PP', 'MINEUR')
and statut_derogation = 'NON'
GROUP BY NUMERO_PIECE
HAVING COUNT(*) > 3) A2
ON trim(A1.NUMERO_PIECE) = trim(A2.NUMERO_PIECE)