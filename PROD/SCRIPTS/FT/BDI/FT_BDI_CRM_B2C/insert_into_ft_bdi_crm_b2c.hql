insert into MON.SPARK_FT_BDI_CRM_B2C
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
profession,
current_timestamp() as insert_date,
'###SLICE_VALUE###' event_date
from TMP.tt_crm_b2c_1B
where not(msisdn is null or trim(msisdn) = '')