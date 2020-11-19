insert into TMP.TT_ZSMART_CONFORMITE_2
select
msisdn,
id_type_piece,
numero_piece,
(CASE WHEN trim(NUMERO_PIECE) = '' OR NUMERO_PIECE IS NULL THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_ABSENT,
(CASE WHEN not(trim(NUMERO_PIECE) is null or trim(NUMERO_PIECE) = '') AND LENGTH(trim(NUMERO_PIECE)) < 4
THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_INF_4,
(
CASE WHEN TRIM(NUMERO_PIECE) IN ('000000000', '111111111', '222222222', '333333333',
'444444444', '555555555', '666666666', '777777777', '888888888', '999999999',
'012345678','122222222', '123456789', '100010001', '1122334455') OR
LENGTH(trim(NUMERO_PIECE)) > 21 OR TRIM(NUMERO_PIECE) LIKE '112233445%' THEN 'OUI' ELSE 'NON' END
) NUMERO_PIECE_NON_AUTHORISE,
(CASE WHEN trim(MSISDN) = trim(NUMERO_PIECE) THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_EGALE_MSISDN,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(NUMERO_PIECE)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(NUMERO_PIECE)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) <> ''
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_A_CARACT_NON_AUTH,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(NUMERO_PIECE)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(NUMERO_PIECE)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) is null
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_UNIQUEMENT_LETTRE,
numero_piece_tuteur,
(CASE WHEN trim(NUMERO_PIECE_TUTEUR) = '' OR NUMERO_PIECE_TUTEUR IS NULL THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_TUT_ABSENT,
(CASE WHEN not(trim(NUMERO_PIECE_TUTEUR) is null or trim(NUMERO_PIECE_TUTEUR) = '') AND
LENGTH(trim(NUMERO_PIECE_TUTEUR)) < 4 THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_TUT_INF_4,
(
CASE WHEN TRIM(NUMERO_PIECE_TUTEUR) IN ('000000000', '111111111', '222222222', '333333333',
'444444444', '555555555', '666666666', '777777777', '888888888', '999999999',
'012345678','122222222', '123456789', '100010001', '1122334455') OR LENGTH(TRIM(NUMERO_PIECE_TUTEUR)) > 21 OR
trim(NUMERO_PIECE) = trim(NUMERO_PIECE_TUTEUR) OR TRIM(NUMERO_PIECE_TUTEUR) LIKE '112233445%' THEN 'OUI' ELSE 'NON' END
) NUMERO_PIECE_TUT_NON_AUTH,
(CASE WHEN trim(MSISDN) = trim(NUMERO_PIECE_TUTEUR) THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_TUT_EGALE_MSISDN,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(NUMERO_PIECE_TUTEUR)), 'abcdefghijklmnopMCMLXXXIVqrstuvwxyz1234567890-/',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(NUMERO_PIECE_TUTEUR)), 'abcdefghijklmnopMCMLXXXIVqrstuvwxyz1234567890-/',' ')) <> ''
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_TUT_CARAC_NON_A,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(NUMERO_PIECE_TUTEUR)), 'abcdefghijklMCMLXXXIVmnopqrstuvwxyz-',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(NUMERO_PIECE_TUTEUR)), 'abcdefghijklMCMLXXXIVmnopqrstuvwxyz-',' ')) is null
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_TUT_UNIQ_LETTRE,
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
date_naissance_tuteur,
(CASE WHEN DATE_NAISSANCE_TUTEUR IS NULL THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_TUT_ABSENT,
(CASE WHEN DATE_NAISSANCE_TUTEUR > '###SLICE_VALUE###'  OR  cast((date_format('###SLICE_VALUE###','yyyy') - date_format(DATE_NAISSANCE_TUTEUR,'yyyy')) as int)  < 21
THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_TUT_DOUTEUX,
nom_parent,
(CASE WHEN trim(NOM_PARENT) = '' OR NOM_PARENT IS NULL THEN 'OUI' ELSE 'NON' END) NOM_PARENT_ABSENT,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(NOM_PARENT)),'aeiou',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(NOM_PARENT)),'aeiou',' ')) is null)
OR (TRIM(TRANSLATE(LOWER(trim(NOM_PARENT)),'bcdfghjklmnpqrstvwxz',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(NOM_PARENT)),'bcdfghjklmnpqrstvwxz',' ')) is null
)
OR (TRIM(TRANSLATE(trim(NOM_PARENT),'1234567890.',' ')) = ''
OR TRIM(TRANSLATE(trim(NOM_PARENT),'1234567890.',' ')) is null)
OR (LENGTH(trim(NOM_PARENT)) <= 1 and trim(NOM_PARENT) is not null and trim(NOM_PARENT) <> '')
OR LOWER(TRIM(NOM_PARENT)) IN ('orange', 'vendeur', 'madame', 'monsieur', 'delta', 'phone', 'inconnu', 'inconnue', 'anonyme', 'unknown')
OR (TRIM(TRANSLATE(LOWER(trim(NOM_PARENT)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxMCMLXXXIVcvbnm1234567890¿çéèàäëüïöîôû\'âê-.',' ')) <> ''
AND TRIM(TRANSLATE(LOWER(trim(NOM_PARENT)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxMCMLXXXIVcvbnm1234567890¿çéèàäëüïöîôû\'âê-.',' ')) is not null
)
THEN 'OUI' ELSE 'NON'
END) NOM_PARENT_DOUTEUX,
date_expiration,
(CASE WHEN DATE_EXPIRATION IS NULL THEN 'OUI' ELSE 'NON' END) DATE_EXPIRATION_ABSENT,
(CASE WHEN  DATE_EXPIRATION >= add_months('###SLICE_VALUE###',120) THEN 'OUI' ELSE 'NON' END) DATE_EXPIRATION_DOUTEUSE,
date_activation,
(case when date_activation is null then 'OUI' else 'NON' end) as date_activation_absent,
adresse,
(CASE WHEN trim(ADRESSE ) = '' OR ADRESSE IS NULL THEN 'OUI' ELSE 'NON' END) ADRESSE_ABSENT,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(ADRESSE)), 'abcdefghijklmnopqrstuvwxyz123456MCMLXXXIV()[]_7890çéèàäëüï\'öî¿ôûâê,-./:',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(ADRESSE)), 'abcdefghijklmnopqrstuvwxyz123456MCMLXXXIV()[]_7890çéèàäëüïöî¿ôû\'âê,-./:',' ')) <> ''
)
OR (TRIM(TRANSLATE(trim(ADRESSE), '12345678\'90-./:',' ')) =  ''
OR TRIM(TRANSLATE(trim(ADRESSE), '12345678\'90-./:',' ')) is null
)
OR (TRIM(TRANSLATE(LOWER(trim(ADRESSE)), 'bcdfghjklmnpqrstv\'wxzç-./,:',' '))  =  ''
OR TRIM(TRANSLATE(LOWER(trim(ADRESSE)), 'bcdfghjklmnpqrstvw\'xzç-./,:',' ')) is null
)
OR (TRIM(TRANSLATE(LOWER(trim(ADRESSE)), 'aeiouéèàäëüïöîôû\'âê-./:',' '))   =  ''
OR TRIM(TRANSLATE(LOWER(trim(ADRESSE)), 'aeiouéèàäëüïöî\'ôûâê-./:',' ')) is null
)
OR (LENGTH(TRIM(ADRESSE)) < 2 AND not(TRIM(ADRESSE) is null or TRIM(ADRESSE) = ''))
OR TRIM(LOWER(trim(ADRESSE))) IN ('n/a', 'nan', 'unknown', 'inconnue', 'sans adresse','sans','non renseignée') THEN 'OUI' ELSE 'NON'
END) ADRESSE_DOUTEUSE,
quartier,
ville,
statut,
statut_validation_bo,
motif_rejet_bo,
date_validation_bo,
login_validateur_bo,
canal_validateur_bo,
type_abonnement,
csmoddate,
ccmoddate,
compte_client_structure,
nom_structure,
numero_registre_commerce,
numero_piece_representant_legal,
date_changement_statut,
ville_structure,
quartier_structure,
raison_statut,
prenom,
nom,
customer_id,
contract_id,
compte_client,
plan_localisation,
contrat_soucription,
acceptation_cgv,
disponibilite_scan,
nom_tuteur,
prenom_tuteur,
date_expiration_tuteur,
id_type_piece_tuteur,
adresse_tuteur,
identificateur,
localisation_identificateur,
profession
from TMP.TT_ZSMART_CONFORMITE