insert into TMP.TT_ZSMART_CONFORMITE
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
            OR (TRIM(TRANSLATE(LOWER(trim(B.NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüïöîôûâê-.''',' ')) is not null
                AND TRIM(TRANSLATE(LOWER(trim(B.NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüïöîôûâê-.''',' ')) <> ''
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
(CASE WHEN B.DATE_NAISSANCE > current_date() THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_DOUTEUX,
(case when B.DATE_EXPIRATION is null then 'OUI' else 'NON' END) AS DATE_EXPIRATION_ABSENTE,
(case when B.DATE_EXPIRATION < current_date() then 'OUI' ELSE 'NON' END) AS CNI_EXPIRE,
IF(B.date_activation is null or B.date_activation < B.DATE_SOUSCRIPTION, B.DATE_SOUSCRIPTION,B.date_activation) as DATE_ACTIVATION,
IF(B.CONTRAT_SOUCRIPTION is null or trim(B.CONTRAT_SOUCRIPTION) = '','OUI','NON') as CONTRAT_SOUCRIPTION_zsmart_abs,
IF(B.nom_parent is null or trim(B.nom_parent) = '','OUI','NON') as nom_parent_z_abs,
if(B.DATE_NAISSANCE_TUTEUR is null,'OUI','NON') as DATE_NAISSANCE_TUT_absent,
(CASE WHEN B.DATE_NAISSANCE_TUTEUR > B.original_file_date OR  cast((date_format(B.original_file_date,'yyyy') - date_format(B
.DATE_NAISSANCE_TUTEUR,'yyyy')) as int)  < 21
THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_TUT_DOUTEUX,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(B.NOM_PARENT)),'aeiou',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(B.NOM_PARENT)),'aeiou',' ')) is null)
OR (TRIM(TRANSLATE(LOWER(trim(B.NOM_PARENT)),'bcdfghjklmnpqrstvwxz',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(B.NOM_PARENT)),'bcdfghjklmnpqrstvwxz',' ')) is null
)
OR (TRIM(TRANSLATE(trim(B.NOM_PARENT),'1234567890.',' ')) = ''
OR TRIM(TRANSLATE(trim(B.NOM_PARENT),'1234567890.',' ')) is null)
OR (LENGTH(trim(B.NOM_PARENT)) <= 1 and trim(B.NOM_PARENT) is not null and trim(B.NOM_PARENT) <> '')
OR LOWER(TRIM(B.NOM_PARENT)) IN ('orange', 'vendeur', 'madame', 'monsieur', 'delta', 'phone', 'inconnu', 'inconnue', 'anonyme', 'unknown')
OR (TRIM(TRANSLATE(LOWER(trim(B.NOM_PARENT)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxMCMLXXXIVcvbnm1234567890¿çéèàäëüïöîôûâê-.''',' ')) <> ''
AND TRIM(TRANSLATE(LOWER(trim(B.NOM_PARENT)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxMCMLXXXIVcvbnm1234567890¿çéèàäëüïöîôûâê-.''',' ')) is not null
)
THEN 'OUI' ELSE 'NON'
END) NOM_PARENT_DOUTEUX
from (
select
trim(b.msisdn) AS msisdn,
trim(b.prenom) AS prenom,
trim(b.nom) AS nom,
trim(b.NUMERO_PIECE) as NUMERO_PIECE,
(CASE
WHEN trim(b.DATE_EXPIRATION) IS NULL OR trim(b.DATE_EXPIRATION) = '' THEN NULL
WHEN trim(b.DATE_EXPIRATION) LIKE '%-%'
THEN cast(substr(trim(b.DATE_EXPIRATION),1,10) AS DATE)
WHEN trim(b.DATE_EXPIRATION) LIKE '%/%'
THEN cast(translate(substr(trim(b.DATE_EXPIRATION),1,10),'/','-')  AS DATE)
ELSE NULL
END) DATE_EXPIRATION,
(CASE
WHEN trim(b.date_naissance) IS NULL OR trim(b.date_naissance) = '' THEN NULL
WHEN trim(b.date_naissance) LIKE '%-%'
THEN cast(substr(trim(b.date_naissance),1,10) as DATE)
WHEN trim(b.date_naissance) LIKE '%/%'
THEN cast(translate(substr(trim(b.date_naissance),1,10),'/','-') as DATE)
ELSE NULL
END) DATE_NAISSANCE,
(CASE
WHEN trim(b.date_activation) IS NULL OR trim(b.date_activation) = '' THEN NULL
WHEN trim(b.date_activation) like '%/%'
THEN  cast(translate(SUBSTR(trim(b.date_activation), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(b.date_activation) like '%-%' THEN  cast(SUBSTR(trim(b.date_activation), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION,
trim(b.COMPTE_CLIENT_STRUCTURE) as COMPTE_CLIENT_STRUCTURE,
trim(b.NOM_STRUCTURE) as NOM_STRUCTURE,
trim(b.NUMERO_REGISTRE_COMMERCE) as NUMERO_REGISTRE_COMMERCE,
trim(b.NUMERO_PIECE_REPRESENTANT_LEGAL) as NUMERO_PIECE_REPRESENTANT_LEGAL,
(CASE
WHEN trim(b.DATE_SOUSCRIPTION) IS NULL OR trim(b.DATE_SOUSCRIPTION) = '' THEN NULL
WHEN trim(b.DATE_SOUSCRIPTION) like '%/%'
THEN  cast(translate(SUBSTR(trim(b.DATE_SOUSCRIPTION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(b.DATE_SOUSCRIPTION) like '%-%' THEN  cast(SUBSTR(trim(b.DATE_SOUSCRIPTION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_SOUSCRIPTION,
trim(b.COMPTE_CLIENT) as COMPTE_CLIENT,
trim(b.CONTRAT_SOUCRIPTION) as CONTRAT_SOUCRIPTION,
trim(b.NOM_TUTEUR) as NOM_TUTEUR,
trim(b.PRENOM_TUTEUR) as PRENOM_TUTEUR,
(CASE
WHEN trim(b.DATE_NAISSANCE_TUTEUR) IS NULL OR trim(b.DATE_NAISSANCE_TUTEUR) = '' THEN NULL
WHEN trim(b.DATE_NAISSANCE_TUTEUR) LIKE '%-%'
THEN cast(substr(trim(b.DATE_NAISSANCE_TUTEUR),1,10)  AS DATE)
WHEN trim(b.DATE_NAISSANCE_TUTEUR) LIKE '%/%'
THEN cast(translate(substr(trim(b.DATE_NAISSANCE_TUTEUR),1,10),'/','-')  AS DATE)
ELSE NULL
END) DATE_NAISSANCE_TUTEUR,
trim(NUMERO_PIECE_TUTEUR) as NUMERO_PIECE_TUTEUR,
trim(b.DATE_EXPIRATION_TUTEUR) as DATE_EXPIRATION_TUTEUR,
trim(b.ADRESSE_TUTEUR) as ADRESSE_TUTEUR,
trim(b.NOM_TUTEUR) as NOM_TUTEUR,
trim(b.PRENOM_TUTEUR) as PRENOM_TUTEUR,
nvl(trim(b.nom),'') || ' ' || nvl(trim(b.prenom),'') as nom_prenom,
nvl(trim(b.NOM_TUTEUR),'') || ' ' || nvl(trim(b.PRENOM_TUTEUR),'') as nom_parent,
b.original_file_date
from (select a.*,
row_number() over(partition by a.msisdn order by a.date_activation desc NULLS LAST) as RANG
from CDR.SPARK_IT_BDI_ZSMART a
where a.original_file_date = to_date('###SLICE_VALUE###')
) b where RANG = 1
) B