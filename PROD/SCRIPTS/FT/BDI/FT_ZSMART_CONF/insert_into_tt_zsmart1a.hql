insert into TMP.TT_ZSMART1A
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
profession
from (
select a.*,
row_number() over(partition by msisdn order by DATE_ACTIVATION desc nulls last) as rang
from (
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn,
id_type_piece,
numero_piece,
date_expiration,
date_naissance,
(case when trim(adresse) = '' or adresse is null then trim(nvl(quartier,'') ||' ' || nvl(ville,'')) else adresse end) as adresse,
nvl((CASE
WHEN trim(DATE_SOUSCRIPTION) IS NULL OR trim(DATE_SOUSCRIPTION) = '' THEN NULL
WHEN trim(DATE_SOUSCRIPTION) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_SOUSCRIPTION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_SOUSCRIPTION) like '%-%' THEN  cast(SUBSTR(trim(DATE_SOUSCRIPTION), 1, 19) AS TIMESTAMP)
ELSE NULL
END),
(CASE
WHEN trim(DATE_ACTIVATION) IS NULL OR trim(DATE_ACTIVATION) = '' THEN NULL
WHEN trim(DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END)) as DATE_ACTIVATION,
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
trim(concat_ws(' ',nvl(nom,''),nvl(prenom,''))) as nom_prenom,
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
profession
from cdr.spark_it_bdi_zsmart
where original_file_date=date_add('###SLICE_VALUE###',1)
) a ) b where rang = 1