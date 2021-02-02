insert into TMP.TT_ZSMART1B
select
msisdn,
id_type_piece,
numero_piece,
date_expiration,
date_naissance,
adresse,
DATE_ACTIVATION,
ville,
quartier,
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
nom_prenom,
customer_id,
contract_id,
compte_client,
plan_localisation,
contrat_soucription,
acceptation_cgv,
disponibilite_scan,
nom_tuteur,
prenom_tuteur,
date_naissance_tuteur,
numero_piece_tuteur,
date_expiration_tuteur,
id_type_piece_tuteur,
adresse_tuteur,
identificateur,
localisation_identificateur,
profession,
(case when trim(nom_structure) = '' or nom_structure is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
length(trim(nom_structure)) < 2 then 'OUI' else 'NON'
end) as nom_structure_an,
(case
when trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null or
trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
length(trim(NUMERO_REGISTRE_COMMERCE)) < 2
then 'OUI' else 'NON'
end) as  RCCM_an,
(case
when trim(  numero_piece_representant_legal) = '' or   numero_piece_representant_legal is NULL or
trim(  numero_piece_representant_legal) rlike '^(\\d)\\1+$' or
length(trim(  numero_piece_representant_legal)) > 21 or
trim(  numero_piece_representant_legal) like '112233445%' or
trim(translate(lower(trim(  numero_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null or
trim(translate(lower(trim(  numero_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
then 'OUI' else 'NON'
end) as NUM_PIECE_RPSTANT_AN,
(case
when  DATE_ACTIVATION is NULL
then 'OUI' else 'NON'
end) as  DATE_ACTIVATION_AN,
(case
when trim(msisdn) = '' or msisdn is null or trim(msisdn) not rlike '^\\d+$'
then 'OUI' else 'NON'
end) as NUM_TEL_AN,
(case
when trim(NOM_PRENOM) = '' or NOM_PRENOM is NULL or
trim(translate(trim(NOM_PRENOM),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
length(trim(NOM_PRENOM)) = 1
then 'OUI' else 'NON'
end) as NOM_PRENOM_AN,
(case
when (trim(NUMERO_PIECE) = '' or NUMERO_PIECE is NULL or
trim(NUMERO_PIECE) rlike '^(\\d)\\1+$' or
length(trim(NUMERO_PIECE)) > 21 or
trim(NUMERO_PIECE) like '112233445%' or
trim(translate(lower(trim(NUMERO_PIECE)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null or
trim(translate(lower(trim(NUMERO_PIECE)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null
) and upper(trim(NUMERO_PIECE)) not in ('NON ASSUJETTI')
then 'OUI' else 'NON'
end) as NUMERO_PIECE_AN,
(case
when trim(ADRESSE) = '' or ADRESSE  is NULL or
trim(translate(trim(ADRESSE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
length(trim(ADRESSE)) < 2
then 'OUI' else 'NON'
end) as ADRESSE_AN,
(case
when not(trim(STATUT) = '' or STATUT is NULL) and upper(trim(STATUT)) in ('ACTIF','SUSPENDU_ENTRANT','SUSPENDU_SORTANT','SUSPENDU')
then 'NON' else 'OUI'
end) as STATUT_AN
from TMP.TT_ZSMART1A