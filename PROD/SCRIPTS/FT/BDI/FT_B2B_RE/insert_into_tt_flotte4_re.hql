insert into TMP.tt_flotte4_RE
select
NOM_STRUCTURE,
NUMERO_REGISTRE_COMMERCE,
num_piece_representant_legal,
DATE_SOUSCRIPTION,
ADRESSE_STRUCTURE,
MSISDN,
NOM_PRENOM,
NUMERO_PIECE,
IMEI,
ADRESSE,
STATUT,
DISPONIBILITE_SCAN,
ACCEPTATION_CGV,
CUSTOMER_ID,
CONTRACT_ID,
COMPTE_CLIENT,
TYPE_PERSONNE,
TYPE_PIECE,
ID_TYPE_PIECE,
NOM,
PRENOM,
DATE_NAISSANCE,
DATE_EXPIRATION,
VILLE,
QUARTIER,
STATUT_OLD,
RAISON_STATUT,
ODBIC,
ODBOC,
DATE_CHANGEMENT_STATUT,
PLAN_LOCALISATION,
CONTRAT_SOUCRIPTION,
TYPE_PIECE_TUTEUR,
NUMERO_PIECE_TUTEUR,
NOM_TUTEUR,
PRENOM_TUTEUR,
DATE_NAISSANCE_TUTEUR,
DATE_EXPIRATION_TUTEUR,
ADRESSE_TUTEUR,
COMPTE_CLIENT_STRUCTURE,
STATUT_DEROGATION,
REGION_ADMINISTRATIVE,
REGION_COMMERCIALE,
SITE_NAME,
VILLE_SITE,
OFFRE_COMMERCIALE,
TYPE_CONTRAT,
SEGMENTATION,
DEROGATION_IDENTIFICATION,
COMPTE_CLIENT_PARENT,
NOM_REPRESENTANT_LEGAL,
PRENOM_REPRESENTANT_LEGAL,
CONTACT_TELEPHONIQUE,
VILLE_STRUCTURE,
QUARTIER_STRUCTURE,
SMS_CONTACT,
DOC_PLAN_LOCALISATION,
DOC_FICHE_SOUSCRIPTION,
DOC_ATTESTATION_CNPS,
DOC_RCCM,
TYPE_CLIENT,
RANG,
-- conformités ---
(case
    when trim(nom_structure) = '' or nom_structure is null
    then 'OUI' else 'NON'
end) as NOM_STRUCTURE_ABSENT,
(case
    when not(trim(nom_structure) = '' or nom_structure is null) and (
        trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
    or    length(trim(nom_structure)) < 2)
    then 'OUI' else 'NON'
end) as NOM_STRUCTURE_DOUTEUX,
(case
    when trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null
    then 'OUI' else 'NON'
end) as RCCM_ABSENT,
(case
    when not(trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null) and (
        trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
    or    length(trim(NUMERO_REGISTRE_COMMERCE)) < 2)
    then 'OUI' else 'NON'
end) as  RCCM_DOUTEUX,
(case
    when trim(num_piece_representant_legal) = '' or num_piece_representant_legal is NULL
    then 'OUI' else 'NON'
end) as NUM_PIECE_RPSTANT_ABSENT,
(case
    when not(trim(num_piece_representant_legal) = '' or num_piece_representant_legal is NULL) and
    (
        trim(num_piece_representant_legal) rlike '^(\\d)\\1+$' or
        length(trim(num_piece_representant_legal)) > 21 or
        trim(num_piece_representant_legal) like '112233445%' or
        trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null or
        trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
    ) then 'OUI' else 'NON'
end) as NUM_PIECE_RPSTANT_DOUTEUX,
(case
    when DATE_SOUSCRIPTION is NULL
    then 'OUI' else 'NON'
end) as DATE_SOUSCRIPTION_ABSENT,
(case
    when trim(ADRESSE_STRUCTURE) = '' or ADRESSE_STRUCTURE is NULL
    then 'OUI' else 'NON'
end) as ADRESSE_STRUCTURE_ABSENT,
(case
    when not(trim(ADRESSE_STRUCTURE) = '' or ADRESSE_STRUCTURE is NULL) and (
    trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
    or length(trim(ADRESSE_STRUCTURE)) < 2
    )
    then 'OUI' else 'NON'
end) as ADRESSE_STRUCTURE_DOUTEUX,
(case
    when trim(msisdn) = '' or  msisdn is NULL
    then 'OUI' else 'NON'
end) as NUM_TEL_ABSENT,
(case
    when trim(NOM_PRENOM) = '' or NOM_PRENOM is NULL
    then 'OUI' else 'NON'
end) as NOM_PRENOM_ABSENT,
(case
    when not(trim(NOM_PRENOM) = '' or NOM_PRENOM is NULL) and (
        trim(translate(trim(NOM_PRENOM),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
    or    length(trim(NOM_PRENOM)) = 1)
    then 'OUI' else 'NON'
end) as NOM_PRENOM_DOUTEUX,
(case
    when trim(NUMERO_PIECE) = '' or NUMERO_PIECE is NULL
    then 'OUI' else 'NON'
end) as NUMERO_PIECE_ABSENT,
(case
    when not(trim(NUMERO_PIECE) = '' or NUMERO_PIECE is NULL) and
    (
        trim(NUMERO_PIECE) rlike '^(\\d)\\1+$' or
        length(trim(NUMERO_PIECE)) > 21 or
        trim(NUMERO_PIECE) like '112233445%' or
        trim(translate(lower(trim(NUMERO_PIECE)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null or
        trim(translate(lower(trim(NUMERO_PIECE)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
    ) and upper(trim(NUMERO_PIECE)) not in ('NON ASSUJETTI')
    then 'OUI' else 'NON'
end) as NUMERO_PIECE_DOUTEUX,
(case
    when trim(IMEI) = '' or IMEI is NULL
    then 'OUI' else 'NON'
end) as IMEI_ABSENT,
(case
    when not(trim(IMEI) = '' or IMEI is NULL) and (
        trim(IMEI) not rlike '^\\d{14,16}$'
    )
    then 'OUI' else 'NON'
end) as IMEI_DOUTEUX,
(case
    when trim(ADRESSE) = '' or ADRESSE  is NULL
    then 'OUI' else 'NON'
end) as ADRESSE_ABSENT,
(case
    when not(trim(ADRESSE) = '' or ADRESSE  is NULL) and (
    trim(translate(trim(ADRESSE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
    or length(trim(ADRESSE)) < 2
    )
    then 'OUI' else 'NON'
end) as ADRESSE_DOUTEUX,
(case
    when trim(STATUT) = '' or STATUT is NULL
    then 'OUI' else 'NON'
end) as STATUT_ABSENT,
(case
    when upper(trim(STATUT)) in ('ACTIF','SUSPENDU_ENTRANT','SUSPENDU_SORTANT','SUSPENDU')
    then 'NON' else 'OUI'
end) as STATUT_DOUTEUX
from TMP.tt_flotte3_RE