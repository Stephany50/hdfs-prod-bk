insert into TMP.TT_MULTISIMS_FIN
select A1.NUMERO_PIECE as NUMERO_PIECE,
A1.MSISDN as MSISDN,A1.date_activation as date_activation from
(select *
from TMP.TT_IT_BDI_ART2
where not(NUMERO_PIECE is  null or trim(NUMERO_PIECE) = '')
and ODBOUTGOINGCALLS = '0'
and statut_derogation = 'NON') A1
JOIN
(SELECT NUMERO_PIECE, count(*)
from TMP.TT_IT_BDI_ART2
where not(NUMERO_PIECE is  null or trim(NUMERO_PIECE) = '')
and ODBOUTGOINGCALLS = '0'
and statut_derogation = 'NON'
GROUP BY NUMERO_PIECE
HAVING COUNT(*) > 3) A2
ON trim(A1.NUMERO_PIECE) = trim(A2.NUMERO_PIECE)
ORDER BY A1.numero_piece