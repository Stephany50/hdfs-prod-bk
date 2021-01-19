insert into Mon.spark_ft_bdi_b2b
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
date_naissance,
date_expiration,
ville,
quartier,
statut_old as statut_zsmart,
raison_statut as raison_statut_zsmart,
odbic,
odboc,
date_changement_statut,
plan_localisation,
contrat_soucription,
type_piece_tuteur,
numero_piece_tuteur,
nom_tuteur,
prenom_tuteur,
date_naissance_tuteur,
date_expiration_tuteur,
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
nom_structure_absent,
nom_structure_douteux,
rccm_absent,
rccm_douteux,
num_piece_rpstant_absent,
num_piece_rpstant_douteux,
date_souscription_absent,
adresse_structure_absent,
adresse_structure_douteux,
num_tel_absent,
nom_prenom_absent,
nom_prenom_douteux,
numero_piece_absent,
numero_piece_douteux,
imei_absent,
imei_douteux,
adresse_absent,
adresse_douteux,
statut_absent,
statut_douteux,
current_timestamp() as insert_date,
'###SLICE_VALUE###' event_date
from TMP.tt_flotte4