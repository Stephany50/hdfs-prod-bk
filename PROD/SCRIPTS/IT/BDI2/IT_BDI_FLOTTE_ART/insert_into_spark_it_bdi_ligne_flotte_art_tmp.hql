insert into TMP.TT_LIGNE_ANOMALIE
select distinct msisdn
from Mon.spark_ft_bdi_art
where event_date=to_date('###SLICE_VALUE###')
and est_suspendu = 'NON'
and (DATE_ACTIVATION IS NULL
    OR NOM_PRENOM_ABSENT = 'OUI'
    OR NOM_PRENOM_DOUTEUX = 'OUI'
    OR NUMERO_PIECE_ABSENT = 'OUI'
    OR NUMERO_PIECE_INF_4 = 'OUI'
    OR NUMERO_PIECE_NON_AUTHORISE = 'OUI'
    OR NUMERO_PIECE_EGALE_MSISDN = 'OUI'
    OR NUMERO_PIECE_A_CARACT_NON_AUTH = 'OUI'
    OR NUMERO_PIECE_UNIQUEMENT_LETTRE = 'OUI'
    OR DATE_EXPIRATION_DOUTEUSE = 'OUI'
    OR DATE_EXPIRATION is null
    OR DATE_NAISSANCE is null
    OR DATE_NAISSANCE_DOUTEUX = 'OUI'
    OR MULTI_SIM = 'OUI'
    OR ADRESSE_ABSENT = 'OUI'
    OR ADRESSE_DOUTEUSE = 'OUI'
    OR IMEI is null OR trim(IMEI) = ''
    OR TYPE_PIECE IS NULL OR trim(TYPE_PIECE) = ''
    OR MSISDN IS NULL OR trim(MSISDN) = '')
AND type_personne IN ('MAJEUR','PP')
union
select distinct msisdn
from Mon.spark_ft_bdi_art
where event_date=to_date('###SLICE_VALUE###')
and est_suspendu = 'NON'
and (DATE_ACTIVATION IS NULL
    OR NOM_PRENOM_ABSENT = 'OUI'
    OR NOM_PRENOM_DOUTEUX = 'OUI'
    OR NUMERO_PIECE_ABSENT = 'OUI'
    OR NUMERO_PIECE_INF_4 = 'OUI'
    OR NUMERO_PIECE_NON_AUTHORISE = 'OUI'
    OR NUMERO_PIECE_EGALE_MSISDN = 'OUI'
    OR NUMERO_PIECE_A_CARACT_NON_AUTH = 'OUI'
    OR NUMERO_PIECE_UNIQUEMENT_LETTRE = 'OUI'
    OR DATE_EXPIRATION_DOUTEUSE = 'OUI'
    OR DATE_EXPIRATION is null
    OR TYPE_PIECE IS NULL OR trim(TYPE_PIECE) = ''
    OR DATE_NAISSANCE is null
    OR DATE_NAISSANCE_DOUTEUX = 'OUI'
    OR NOM_PARENT_ABSENT = 'OUI'
    OR NOM_PARENT_DOUTEUX = 'OUI'
    OR NUMERO_PIECE_TUT_ABSENT = 'OUI'
    OR NUMERO_PIECE_TUT_INF_4 = 'OUI'
    OR NUMERO_PIECE_TUT_NON_AUTH = 'OUI'
    OR NUMERO_PIECE_TUT_EGALE_MSISDN = 'OUI'
    OR NUMERO_PIECE_TUT_CARAC_NON_A = 'OUI'
    OR NUMERO_PIECE_TUT_UNIQ_LETTRE = 'OUI'
    OR date_naissance_tuteur is null
    OR DATE_NAISSANCE_TUT_DOUTEUX ='OUI'
    OR MULTI_SIM = 'OUI'
    OR ADRESSE_ABSENT = 'OUI'
    OR ADRESSE_DOUTEUSE = 'OUI'
    OR IMEI is null OR trim(IMEI) = ''
    OR MSISDN IS NULL OR trim(MSISDN) = ''
    )
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