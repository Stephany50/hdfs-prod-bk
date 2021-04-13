insert into CDR.SPARK_IT_BDI_LIGNE_FLOTTE
select
msisdn,
customer_id,
contract_id,
compte_client,
type_personne,
type_piece,
numero_piece,
id_type_piece,
nom_prenom,
nom,
prenom,
date_naissance,
date_expiration,
adresse,
ville,
quartier,
date_souscription,
date_activation,
statut,
raison_statut,
date_changement_statut,
plan_localisation,
contrat_soucription,
disponibilite_scan,
acceptation_cgv,
type_piece_tuteur,
numero_piece_tuteur,
nom_tuteur,
prenom_tuteur,
date_naissance_tuteur,
date_expiration_tuteur,
adresse_tuteur,
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
odbincomingcalls,
odboutgoingcalls,
derogation_identification,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as original_file_date
from TMP.TT_BDI_LIGNE_FLOTTE2_1C