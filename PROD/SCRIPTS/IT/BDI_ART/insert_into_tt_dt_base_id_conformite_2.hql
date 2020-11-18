insert into TMP.TT_DT_BASE_ID_CONFORMITE_2
select
msisdn,
CNI as numero_piece,
(CASE WHEN trim(CNI) = '' OR CNI IS NULL THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_ABSENT,
(CASE WHEN not(trim(CNI) is null or trim(CNI) = '') AND LENGTH(trim(CNI)) < 4
THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_INF_4,
(
CASE WHEN TRIM(CNI) IN ('000000000', '111111111', '222222222', '333333333',
'444444444', '555555555', '666666666', '777777777', '888888888', '999999999',
'012345678','122222222', '123456789', '100010001', '1122334455') OR
LENGTH(trim(CNI)) > 21 OR TRIM(CNI) LIKE '112233445%' THEN 'OUI' ELSE 'NON' END
) NUMERO_PIECE_NON_AUTHORISE,
(CASE WHEN trim(MSISDN) = trim(CNI) THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_EGALE_MSISDN,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(CNI)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(CNI)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) <> ''
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_A_CARACT_NON_AUTH,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(CNI)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(CNI)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) is null
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_UNIQUEMENT_LETTRE,
nom_prenom,
(CASE WHEN trim(NOM_PRENOM) = '' OR NOM_PRENOM IS NULL THEN 'OUI' ELSE 'NON' END) NOM_PRENOM_ABSENT,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(NOM_PRENOM)),'aeiou',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(NOM_PRENOM)),'aeiou',' ')) is null)
OR (TRIM(TRANSLATE(LOWER(trim(NOM_PRENOM)),'bcdfghjklmnpqrstvwxz',' '))  = ''
OR TRIM(TRANSLATE(LOWER(trim(NOM_PRENOM)),'bcdfghjklmnpqrstvwxz',' ')) is null)
OR (TRIM(TRANSLATE(trim(NOM_PRENOM),'1234567890.',' '))  = ''
OR TRIM(TRANSLATE(trim(NOM_PRENOM),'1234567890.',' ')) is null)
OR (LENGTH(trim(NOM_PRENOM)) <= 1 and trim(NOM_PRENOM) is not null and trim(NOM_PRENOM) <> '')
OR LOWER(TRIM(NOM_PRENOM)) IN ('orange', 'vendeur', 'madame', 'monsieur', 'delta', 'phone', 'inconnu', 'inconnue', 'anonyme', 'unknown')
OR (TRIM(TRANSLATE(LOWER(trim(NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüïöî\'ôûâê-.',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüïöîôû\'âê-.',' ')) <> ''
)
THEN 'OUI' ELSE 'NON'
END) NOM_PRENOM_DOUTEUX,
date_naissance,
(CASE WHEN DATE_NAISSANCE IS NULL THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_ABSENT,
(CASE WHEN DATE_NAISSANCE > '###SLICE_VALUE###' THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_DOUTEUX,
nom,
prenom,
nee_a,
profession,
quartier_residence,
ville_village,
date_identification,
type_document,
fichier_chargement,
date_insertion,
est_snappe,
identificateur,
date_mise_a_jour,
date_table_mis_a_jour,
genre,
civilite,
type_piece_identification,
profession_identificateur,
motif_rejet
from TMP.TT_DT_BASE_ID_CONFORMITE