insert into TMP.tt_crm_b2c_1A
select
msisdn,
type_personne,
nom_prenom,
id_type_piece,
type_piece,
numero_piece,
date_expiration,
date_naissance,
date_activation,
adresse,
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
imei,
statut_derogation,
region_administrative,
region_commerciale,
site_name,
ville_site,
offre_commerciale,
type_contrat,
segmentation,
date_souscription,
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
date_naissance_tuteur,
numero_piece_tuteur,
date_expiration_tuteur,
id_type_piece_tuteur,
type_piece_tuteur,
adresse_tuteur,
identificateur,
localisation_identificateur,
profession
from (
select a.*,
row_number() over(partition by msisdn order by ccmoddate desc nulls last) as rn
from (
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) msisdn,
type_personne,
nom_prenom,
id_type_piece,
type_piece,
numero_piece,
(CASE
WHEN trim(DATE_EXPIRATION) IS NULL OR trim(DATE_EXPIRATION) = '' THEN NULL
WHEN trim(DATE_EXPIRATION) LIKE '%-%'
THEN cast(substr(trim(DATE_EXPIRATION),1,10) AS DATE)
WHEN trim(DATE_EXPIRATION) LIKE '%/%'
THEN cast(translate(substr(trim(DATE_EXPIRATION),1,10),'/','-')  AS DATE)
ELSE NULL
END) DATE_EXPIRATION,
(CASE
WHEN trim(date_naissance) IS NULL OR trim(date_naissance) = '' THEN NULL
WHEN trim(date_naissance) LIKE '%-%'
THEN cast(substr(trim(date_naissance),1,10) AS DATE)
WHEN trim(date_naissance) LIKE '%/%'
THEN cast(translate(substr(trim(date_naissance),1,10),'/','-')  AS DATE)
ELSE NULL
END) date_naissance,
(CASE
WHEN trim(DATE_ACTIVATION) IS NULL OR trim(DATE_ACTIVATION) = '' THEN NULL
WHEN trim(DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION,
adresse,
quartier,
ville,
statut,
statut_validation_bo,
motif_rejet_bo,
(CASE
WHEN trim(date_validation_bo) IS NULL OR trim(date_validation_bo) = '' THEN NULL
WHEN trim(date_validation_bo) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_validation_bo), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_validation_bo) like '%-%' THEN  cast(SUBSTR(trim(date_validation_bo), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_validation_bo,
login_validateur_bo,
canal_validateur_bo,
type_abonnement,
(CASE
WHEN trim(csmoddate) IS NULL OR trim(csmoddate) = '' THEN NULL
WHEN trim(csmoddate) like '%/%'
THEN  cast(translate(SUBSTR(trim(csmoddate), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(csmoddate) like '%-%' THEN  cast(SUBSTR(trim(csmoddate), 1, 19) AS TIMESTAMP)
ELSE NULL
END) csmoddate,
(CASE
WHEN trim(ccmoddate) IS NULL OR trim(ccmoddate) = '' THEN NULL
WHEN trim(ccmoddate) like '%/%'
THEN  cast(translate(SUBSTR(trim(ccmoddate), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(ccmoddate) like '%-%' THEN  cast(SUBSTR(trim(ccmoddate), 1, 19) AS TIMESTAMP)
ELSE NULL
END) ccmoddate,
compte_client_structure,
nom_structure,
numero_registre_commerce,
numero_piece_representant_legal,
imei,
statut_derogation,
region_administrative,
region_commerciale,
site_name,
ville_site,
offre_commerciale,
type_contrat,
segmentation,
(CASE
WHEN trim(date_souscription) IS NULL OR trim(date_souscription) = '' THEN NULL
WHEN trim(date_souscription) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_souscription), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_souscription) like '%-%' THEN  cast(SUBSTR(trim(date_souscription), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_souscription,
(CASE
WHEN trim(date_changement_statut) IS NULL OR trim(date_changement_statut) = '' THEN NULL
WHEN trim(date_changement_statut) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_changement_statut), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_changement_statut) like '%-%' THEN  cast(SUBSTR(trim(date_changement_statut), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_changement_statut,
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
(CASE
WHEN trim(date_naissance_tuteur) IS NULL OR trim(date_naissance_tuteur) = '' THEN NULL
WHEN trim(date_naissance_tuteur) LIKE '%-%'
THEN cast(substr(trim(date_naissance_tuteur),1,10) AS DATE)
WHEN trim(date_naissance_tuteur) LIKE '%/%'
THEN cast(translate(substr(trim(date_naissance_tuteur),1,10),'/','-')  AS DATE)
ELSE NULL
END) date_naissance_tuteur,
numero_piece_tuteur,
(CASE
WHEN trim(date_expiration_tuteur) IS NULL OR trim(date_expiration_tuteur) = '' THEN NULL
WHEN trim(date_expiration_tuteur) LIKE '%-%'
THEN cast(substr(trim(date_expiration_tuteur),1,10) AS DATE)
WHEN trim(date_expiration_tuteur) LIKE '%/%'
THEN cast(translate(substr(trim(date_expiration_tuteur),1,10),'/','-')  AS DATE)
ELSE NULL
END) date_expiration_tuteur,
id_type_piece_tuteur,
type_piece_tuteur,
adresse_tuteur,
identificateur,
localisation_identificateur,
profession
from cdr.spark_it_bdi_crm_b2c
where original_file_date = date_add('###SLICE_VALUE###',1)
) a) b where rn = 1