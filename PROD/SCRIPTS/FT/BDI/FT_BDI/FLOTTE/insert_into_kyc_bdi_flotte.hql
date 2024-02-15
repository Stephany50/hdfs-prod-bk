-- insertion dans la table finale
insert into MON.SPARK_FT_KYC_BDI_FLOTTE
select
cust_guid,
nom_structure,
numero_registre_commerce,
numero_piece_representant_legal,
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
raison_statut,
odbincomingcalls,
odboutgoingcalls,
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
type_personne_morale,
nom_structure_an,
rccm_an,
num_piece_rpstant_an,
date_souscription_an,
adresse_structure_an,
num_tel_an,
nom_prenom_an,
numero_piece_an,
imei_an,
adresse_an,
statut_an,
(case when trim(type_personne)='FLOTTE' and
(trim(nom_structure_an) = 'OUI' or
trim(rccm_an) = 'OUI' or
trim(num_piece_rpstant_an) = 'OUI' or
trim(date_souscription_an) = 'OUI' or
trim(adresse_structure_an) = 'OUI' or
trim(num_tel_an) = 'OUI' or
trim(nom_prenom_an) = 'OUI' or
trim(numero_piece_an) = 'OUI' or
trim(imei_an) = 'OUI' or
trim(adresse_an) = 'OUI' or
trim(statut_an) = 'OUI')
then 'NON' 
/*when trim(type_personne)='M2M' and 
(trim(nom_structure_an) = 'OUI' or
trim(rccm_an) = 'OUI' or
trim(num_piece_rpstant_an) = 'OUI' or
trim(adresse_structure_an) = 'OUI') 
then 'NON' else 'OUI' end) as est_conforme,*/
when trim(type_personne)='M2M' and 
(trim(nom_structure_an) = 'OUI')
then 'NON' else 'OUI' end) as est_conforme,
current_timestamp() AS insert_date,
'###SLICE_VALUE###' as event_date
from TMP.TT_KYC_BDI_FLOTTE_ST2