insert into TMP.TT_MYOMID_CONFORMITE
select C.*,
(case when C.dateexpire is null then 'OUI' else 'NON' END) AS DATE_EXPIRATION_ABSENTE,
(case when C.dateexpire < '###SLICE_VALUE###' then 'OUI' ELSE 'NON' END) AS CNI_EXPIRE,
(CASE WHEN trim(C.NOM_PRENOM) = '' OR C.NOM_PRENOM IS NULL THEN 'OUI' ELSE 'NON' END) NOM_PRENOM_ABSENT,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(C.NOM_PRENOM)),'aeiou',' ')) = ''
                    OR TRIM(TRANSLATE(LOWER(trim(C.NOM_PRENOM)),'aeiou',' ')) is null)
            OR (TRIM(TRANSLATE(LOWER(trim(C.NOM_PRENOM)),'bcdfghjklmnpqrstvwxz',' '))  = ''
                OR TRIM(TRANSLATE(LOWER(trim(C.NOM_PRENOM)),'bcdfghjklmnpqrstvwxz',' ')) is null)
            OR (TRIM(TRANSLATE(trim(C.NOM_PRENOM),'1234567890.',' '))  = ''
                OR TRIM(TRANSLATE(trim(C.NOM_PRENOM),'1234567890.',' ')) is null)
            OR (LENGTH(trim(C.NOM_PRENOM)) <= 1 and trim(C.NOM_PRENOM) is not null and trim(C.NOM_PRENOM) <> '')
            OR LOWER(TRIM(C.NOM_PRENOM)) IN ('orange', 'vendeur', 'madame', 'monsieur', 'delta', 'phone', 'inconnu', 'inconnue', 'anonyme', 'unknown')
            OR (TRIM(TRANSLATE(LOWER(trim(C.NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüï\'öîôûâê-.',' ')) is not null
                AND TRIM(TRANSLATE(LOWER(trim(C.NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüïöîô\'ûâê-.',' ')) <> ''
                )
                THEN 'OUI' ELSE 'NON'
END) NOM_PRENOM_DOUTEUX,
(CASE WHEN trim(C.NUMERO_PIECE) = '' OR C.NUMERO_PIECE IS NULL THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_ABSENT,
(CASE WHEN not(trim(C.NUMERO_PIECE) is null or trim(C.NUMERO_PIECE) = '') AND LENGTH(trim(C.NUMERO_PIECE)) < 4
        THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_INF_4,
(
            CASE WHEN TRIM(C.NUMERO_PIECE) IN ('000000000', '111111111', '222222222', '333333333',
            '444444444', '555555555', '666666666', '777777777', '888888888', '999999999',
            '012345678','122222222', '123456789', '100010001', '1122334455') OR
            LENGTH(trim(C.NUMERO_PIECE)) > 21 OR TRIM(C.NUMERO_PIECE) LIKE '112233445%' THEN 'OUI' ELSE 'NON' END
) NUMERO_PIECE_NON_AUTHORISE,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(C.NUMERO_PIECE)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) is not null
              AND TRIM(TRANSLATE(LOWER(trim(C.NUMERO_PIECE)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) <> ''
            THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_A_CARACT_NON_AUTH,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(C.NUMERO_PIECE)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) = ''
            OR TRIM(TRANSLATE(LOWER(trim(C.NUMERO_PIECE)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) is null
            THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_UNIQUEMENT_LETTRE,
(CASE WHEN trim(C.MSISDN) = trim(C.NUMERO_PIECE) THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_EGALE_MSISDN,
(CASE WHEN  C.DATE_NAISSANCE IS NULL THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_ABSENT,
(CASE WHEN C.DATE_NAISSANCE > '###SLICE_VALUE###' THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_DOUTEUX,
(case when D.msisdn is null OR trim(D.msisdn) = '' then 'NON' else 'OUI' end) AS EST_NOMAD_OK
from (
select phone_tango AS msisdn,
trim(nom) AS nom,
trim(prenom) AS prenom,
if(nom is null or trim(nom)='','',trim(nom)) || ' ' || if(prenom is null or trim(prenom)='','',trim(prenom)) AS nom_prenom,
trim(piece) as  numero_piece,
naissance AS date_naissance,
dateexpire,
statut,
ajour,
case when statut = 1 and ajour = 1 then 'OUI' else 'NON' end AS STATUT_VALID_BO_MYOMID
from CDR.SPARK_IT_MYOMID where original_file_date = '2020-10-17'
) C
left join (
select distinct telephone as msisdn
from CDR.SPARK_IT_NOMAD_CLIENT_DIRECTORY
where ORIGINAL_FILE_DATE >= to_date('2019-10-15')
and upper(trim(typedecontrat)) like '%MONEY%'
union
select distinct telephone as msisdn
from CDR.spark_it_nomad_client_directory_dwh
where ORIGINAL_FILE_DATE >= to_date('2019-10-15')
and upper(trim(typedecontrat)) like '%MONEY%'
) D  ON substr(trim(C.msisdn),-9,9) = substr(trim(D.msisdn),-9,9)