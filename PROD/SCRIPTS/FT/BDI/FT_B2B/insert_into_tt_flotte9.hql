insert into TMP.tt_flotte9
select
nom_structure,
numero_registre_commerce,
num_piece_representant_legal,
date_souscription,
adresse_structure,
msisdn,
nom_prenom,
numero_piece,
imei,
adresse,
statut,
disponibilite_scan,
acceptation_cgv,
customer_id,
contract_id,
compte_client,
type_personne,
type_piece,
id_type_piece,
nom,
prenom,
(CASE
WHEN trim(DATE_NAISSANCE) IS NULL OR trim(DATE_NAISSANCE) = '' THEN NULL
WHEN trim(DATE_NAISSANCE) LIKE '%-%'
THEN cast(substr(trim(DATE_NAISSANCE),1,10) as DATE)
WHEN trim(DATE_NAISSANCE) LIKE '%/%'
THEN cast(translate(substr(trim(DATE_NAISSANCE),1,10),'/','-') as DATE)
ELSE NULL
END) DATE_NAISSANCE,
(CASE
WHEN trim(DATE_EXPIRATION) IS NULL OR trim(DATE_EXPIRATION) = '' THEN NULL
WHEN trim(DATE_EXPIRATION) LIKE '%-%'
THEN cast(substr(trim(DATE_EXPIRATION),1,10) AS DATE)
WHEN trim(DATE_EXPIRATION) LIKE '%/%'
THEN cast(translate(substr(trim(DATE_EXPIRATION),1,10),'/','-')  AS DATE)
ELSE NULL
END) DATE_EXPIRATION,
ville,
quartier,
statut_old,
raison_statut,
odbic,
odboc,
(CASE
WHEN trim(DATE_CHANGEMENT_STATUT) IS NULL OR trim(DATE_CHANGEMENT_STATUT) = '' THEN NULL
WHEN trim(DATE_CHANGEMENT_STATUT) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_CHANGEMENT_STATUT), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_CHANGEMENT_STATUT) like '%-%'
THEN  cast(SUBSTR(trim(DATE_CHANGEMENT_STATUT), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_CHANGEMENT_STATUT,
plan_localisation,
contrat_soucription,
type_piece_tuteur,
numero_piece_tuteur,
nom_tuteur,
prenom_tuteur,
(CASE
WHEN trim(date_naissance_tuteur) IS NULL OR trim(date_naissance_tuteur) = '' THEN NULL
WHEN trim(date_naissance_tuteur) LIKE '%-%'
THEN cast(substr(trim(date_naissance_tuteur),1,10) AS DATE)
WHEN trim(date_naissance_tuteur) LIKE '%/%'
THEN cast(translate(substr(trim(date_naissance_tuteur),1,10),'/','-')  AS DATE)
ELSE NULL
END) date_naissance_tuteur,
(CASE
WHEN trim(date_expiration_tuteur) IS NULL OR trim(date_expiration_tuteur) = '' THEN NULL
WHEN trim(date_expiration_tuteur) LIKE '%-%'
THEN cast(substr(trim(date_expiration_tuteur),1,10) AS DATE)
WHEN trim(date_expiration_tuteur) LIKE '%/%'
THEN cast(translate(substr(trim(date_expiration_tuteur),1,10),'/','-')  AS DATE)
ELSE NULL
END) date_expiration_tuteur,
adresse_tuteur,
compte_client_structure,
statut_derogation,
region_administrative,
region_commerciale,
site_name,
ville_site,
offre_commerciale,
type_contrat,
segmentation,
derogation_identification,
compte_client_parent,
nom_representant_legal,
prenom_representant_legal,
contact_telephonique,
ville_structure,
quartier_structure,
sms_contact,
doc_plan_localisation,
doc_fiche_souscription,
doc_attestation_cnps,
doc_rccm,
type_client,
rang,
type_personne_morale,
(case when trim(nom_structure) = '' or nom_structure is null or
(trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(nom_structure)) < 2 then 'OUI' else 'NON'
end) as nom_structure_an,
(case
when trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null or
(trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(NUMERO_REGISTRE_COMMERCE)) < 2
then 'OUI' else 'NON'
end) as  RCCM_an,
(case
when trim(num_piece_representant_legal) = '' or num_piece_representant_legal is NULL or
trim(num_piece_representant_legal) rlike '^(\\d)\\1+$' or
length(trim(num_piece_representant_legal)) > 21 or
trim(num_piece_representant_legal) like '112233445%' or
(trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '')
then 'OUI' else 'NON'
end) as NUM_PIECE_RPSTANT_AN,
(case
when DATE_SOUSCRIPTION is NULL
then 'OUI' else 'NON'
end) as DATE_SOUSCRIPTION_AN,
(case
when trim(ADRESSE_STRUCTURE) = '' or ADRESSE_STRUCTURE is NULL or
(trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(ADRESSE_STRUCTURE)) < 2
then 'OUI' else 'NON'
end) as ADRESSE_STRUCTURE_AN,
(case
when trim(msisdn) = '' or msisdn is null or trim(msisdn) not rlike '^\\d+$'
then 'OUI' else 'NON'
end) as NUM_TEL_AN,
(case
when trim(NOM_PRENOM) = '' or NOM_PRENOM is NULL or
(trim(translate(trim(NOM_PRENOM),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(NOM_PRENOM),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(NOM_PRENOM)) = 1
then 'OUI' else 'NON'
end) as NOM_PRENOM_AN,
(case
when (trim(NUMERO_PIECE) = '' or NUMERO_PIECE is NULL or
trim(NUMERO_PIECE) rlike '^(\\d)\\1+$' or
length(trim(NUMERO_PIECE)) > 21 or
trim(NUMERO_PIECE) like '112233445%' or
(trim(translate(lower(trim(NUMERO_PIECE)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(NUMERO_PIECE)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(NUMERO_PIECE)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(NUMERO_PIECE)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '')
) and upper(trim(NUMERO_PIECE)) not in ('NON ASSUJETTI')
then 'OUI' else 'NON'
end) as NUMERO_PIECE_AN,
(case
when trim(IMEI) = '' or IMEI is NULL or
trim(IMEI) not rlike '^\\d{14,16}$'
then 'OUI' else 'NON'
end) as IMEI_AN,
(case
when trim(ADRESSE) = '' or ADRESSE  is NULL or
(trim(translate(trim(ADRESSE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(ADRESSE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(ADRESSE)) < 2
then 'OUI' else 'NON'
end) as ADRESSE_AN,
(case
when not(trim(STATUT) = '' or STATUT is NULL) and upper(trim(STATUT)) in ('ACTIF','SUSPENDU_ENTRANT','SUSPENDU_SORTANT','SUSPENDU')
then 'NON' else 'OUI'
end) as STATUT_AN
from TMP.tt_flotte8