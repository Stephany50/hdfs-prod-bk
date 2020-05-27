insert into TMP.TT_MULTISIMS_ART
select A1.NUMERO_PIECE, A1.NOM_PRENOM, A1.MSISDN, A1.TYPE_PERSONNE from
(select *
from MON.SPARK_FT_BDI_ART
WHERE EVENT_DATE = to_date('###SLICE_VALUE###')
and not(NUMERO_PIECE is  null or trim(NUMERO_PIECE) = '')
and est_suspendu = 'NON'
and TYPE_PERSONNE IN ('MAJEUR','PP', 'MINEUR')
and statut_derogation = 'NON') A1
JOIN
(SELECT NUMERO_PIECE, count(*)
from MON.SPARK_FT_BDI_ART
WHERE EVENT_DATE = to_date('###SLICE_VALUE###')
and not(NUMERO_PIECE is  null or trim(NUMERO_PIECE) = '')
and est_suspendu = 'NON'
and TYPE_PERSONNE IN ('MAJEUR','PP', 'MINEUR')
and statut_derogation = 'NON'
GROUP BY NUMERO_PIECE
HAVING COUNT(*) > 3) A2
ON trim(A1.NUMERO_PIECE) = trim(A2.NUMERO_PIECE)
ORDER BY A1.numero_piece