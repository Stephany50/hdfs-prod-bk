insert into TMP.TT_OM_PHOTO_CONFORMITE
select
B.*,
(CASE WHEN trim(B.NOM_PRENOM) = '' OR B.NOM_PRENOM IS NULL THEN 'OUI' ELSE 'NON' END) NOM_PRENOM_ABSENT,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(B.NOM_PRENOM)),'aeiou',' ')) = ''
                    OR TRIM(TRANSLATE(LOWER(trim(B.NOM_PRENOM)),'aeiou',' ')) is null)
            OR (TRIM(TRANSLATE(LOWER(trim(B.NOM_PRENOM)),'bcdfghjklmnpqrstvwxz',' '))  = ''
                OR TRIM(TRANSLATE(LOWER(trim(B.NOM_PRENOM)),'bcdfghjklmnpqrstvwxz',' ')) is null)
            OR (TRIM(TRANSLATE(trim(B.NOM_PRENOM),'1234567890.',' '))  = ''
                OR TRIM(TRANSLATE(trim(B.NOM_PRENOM),'1234567890.',' ')) is null)
            OR (LENGTH(trim(B.NOM_PRENOM)) <= 1 and trim(B.NOM_PRENOM) is not null and trim(B.NOM_PRENOM) <> '')
            OR LOWER(TRIM(B.NOM_PRENOM)) IN ('orange', 'vendeur', 'madame', 'monsieur', 'delta', 'phone', 'inconnu', 'inconnue', 'anonyme', 'unknown')
            OR (TRIM(TRANSLATE(LOWER(trim(B.NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüïöî\'ôûâê-.',' ')) is not null
                AND TRIM(TRANSLATE(LOWER(trim(B.NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüï\'öîôûâê-.',' ')) <> ''
                )
                THEN 'OUI' ELSE 'NON'
END) NOM_PRENOM_DOUTEUX,
(CASE WHEN trim(B.NUMERO_PIECE) = '' OR B.NUMERO_PIECE IS NULL THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_ABSENT,
(CASE WHEN not(trim(B.NUMERO_PIECE) is null or trim(B.NUMERO_PIECE) = '') AND LENGTH(trim(B.NUMERO_PIECE)) < 4
        THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_INF_4,
(
            CASE WHEN TRIM(B.NUMERO_PIECE) IN ('000000000', '111111111', '222222222', '333333333',
            '444444444', '555555555', '666666666', '777777777', '888888888', '999999999',
            '012345678','122222222', '123456789', '100010001', '1122334455') OR
            LENGTH(trim(B.NUMERO_PIECE)) > 21 OR TRIM(B.NUMERO_PIECE) LIKE '112233445%' THEN 'OUI' ELSE 'NON' END
) NUMERO_PIECE_NON_AUTHORISE,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(B.NUMERO_PIECE)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) is not null
              AND TRIM(TRANSLATE(LOWER(trim(B.NUMERO_PIECE)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) <> ''
            THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_A_CARACT_NON_AUTH,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(B.NUMERO_PIECE)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) = ''
            OR TRIM(TRANSLATE(LOWER(trim(B.NUMERO_PIECE)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) is null
            THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_UNIQUEMENT_LETTRE,
(CASE WHEN trim(B.MSISDN) = trim(B.NUMERO_PIECE) THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_EGALE_MSISDN,
(CASE WHEN  B.DATE_NAISSANCE IS NULL THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_ABSENT,
(CASE WHEN B.DATE_NAISSANCE > current_date() THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_DOUTEUX
from (
select
trim(b.msisdn) AS msisdn,
trim(b.user_first_name) AS prenom,
trim(b.user_last_name) AS nom,
b.registered_on AS registered_on,
trim(b.id_number) AS numero_piece,
b.user_type As user_type,
b.modified_on AS modified_on,
b.birth_date as date_naissance,
nvl(trim(b.user_last_name),'') || ' ' || nvl(trim(b.user_first_name),'') as nom_prenom
from (select a.*,
row_number() over(partition by a.msisdn order by a.modified_on desc NULLS LAST) as RANG
from MON.spark_ft_omny_account_snapshot a
where event_date = (select max(event_date) from  MON.spark_ft_omny_account_snapshot)
) b where RANG = 1
) B