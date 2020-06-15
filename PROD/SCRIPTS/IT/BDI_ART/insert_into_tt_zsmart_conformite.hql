insert into TMP.TT_ZSMART_CONFORMITE
select
msisdn,
id_type_piece,
numero_piece,
date_expiration,
date_naissance,
case when date_activation is null or date_activation < date_souscription
     then date_souscription
     else date_activation
end as date_activation,
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
adresse_tuteur,
identificateur,
localisation_identificateur,
profession,
nvl(trim(nom),'') || ' ' || nvl(trim(prenom),'') as nom_prenom,
nvl(trim(NOM_TUTEUR),'') || ' ' || nvl(trim(PRENOM_TUTEUR),'') as nom_parent
from (
select 
a.*,
row_number() over(partition by msisdn order by date_activation desc nulls last) as rang
from (
select 
trim(msisdn) AS msisdn,
trim(id_type_piece) AS id_type_piece,
trim(numero_piece) AS numero_piece,
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
THEN cast(substr(trim(date_naissance),1,10) as DATE)
WHEN trim(date_naissance) LIKE '%/%'
THEN cast(translate(substr(trim(date_naissance),1,10),'/','-') as DATE)
ELSE NULL
END) DATE_NAISSANCE,
(CASE
WHEN trim(date_activation) IS NULL OR trim(date_activation) = '' THEN NULL
WHEN trim(date_activation) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_activation), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_activation) like '%-%' THEN  cast(SUBSTR(trim(date_activation), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION,
trim(adresse) AS adresse,
trim(quartier) AS quartier,
trim(ville) AS ville,
trim(statut) AS statut,
trim(statut_validation_bo) AS statut_validation_bo,
trim(motif_rejet_bo) AS motif_rejet_bo,
trim(date_validation_bo) AS date_validation_bo,
trim(login_validateur_bo) AS login_validateur_bo,
trim(canal_validateur_bo) AS canal_validateur_bo,
trim(type_abonnement) AS type_abonnement,
trim(csmoddate) AS csmoddate,
trim(ccmoddate) AS ccmoddate,
trim(compte_client_structure) AS compte_client_structure,
trim(nom_structure) AS nom_structure,
trim(numero_registre_commerce) AS numero_registre_commerce,
trim(numero_piece_representant_legal) AS numero_piece_representant_legal,
(CASE
WHEN trim(DATE_SOUSCRIPTION) IS NULL OR trim(DATE_SOUSCRIPTION) = '' THEN NULL
WHEN trim(DATE_SOUSCRIPTION) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_SOUSCRIPTION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_SOUSCRIPTION) like '%-%' THEN  cast(SUBSTR(trim(DATE_SOUSCRIPTION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_SOUSCRIPTION,
trim(date_changement_statut) AS date_changement_statut,
trim(ville_structure) AS ville_structure,
trim(quartier_structure) AS quartier_structure,
trim(raison_statut) AS raison_statut,
trim(prenom) AS prenom,
trim(nom) AS nom,
trim(customer_id) AS customer_id,
trim(contract_id) AS contract_id,
trim(compte_client) AS compte_client,
trim(plan_localisation) AS plan_localisation,
trim(contrat_soucription) AS contrat_soucription,
trim(acceptation_cgv) AS acceptation_cgv,
trim(disponibilite_scan) AS disponibilite_scan,
trim(nom_tuteur) AS nom_tuteur,
trim(prenom_tuteur) AS prenom_tuteur,
(CASE
WHEN trim(DATE_NAISSANCE_TUTEUR) IS NULL OR trim(DATE_NAISSANCE_TUTEUR) = '' THEN NULL
WHEN trim(DATE_NAISSANCE_TUTEUR) LIKE '%-%'
THEN cast(substr(trim(DATE_NAISSANCE_TUTEUR),1,10)  AS DATE)
WHEN trim(DATE_NAISSANCE_TUTEUR) LIKE '%/%'
THEN cast(translate(substr(trim(DATE_NAISSANCE_TUTEUR),1,10),'/','-')  AS DATE)
ELSE NULL
END) DATE_NAISSANCE_TUTEUR,
trim(numero_piece_tuteur) AS numero_piece_tuteur,
trim(date_expiration_tuteur) AS date_expiration_tuteur,
trim(id_type_piece_tuteur) AS id_type_piece_tuteur,
trim(adresse_tuteur) AS adresse_tuteur,
trim(identificateur) AS identificateur,
trim(localisation_identificateur) AS localisation_identificateur,
trim(profession) AS profession                            
from CDR.SPARK_IT_BDI_ZSMART a
where original_file_date = (select max(original_file_date) from CDR.SPARK_IT_BDI_ZSMART)
) a) b where rang = 1