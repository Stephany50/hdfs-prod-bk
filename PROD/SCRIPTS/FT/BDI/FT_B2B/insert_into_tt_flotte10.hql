--etape 15 insertion dans B2B avec maj du champs est conforme
insert into TMP.tt_flotte10
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
statut_old,
raison_statut,
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
rang2,
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
(case when trim(nom_structure_an) = 'OUI' or
trim(rccm_an) = 'OUI' or
trim(num_piece_rpstant_an) = 'OUI' or
trim(date_souscription_an) = 'OUI' or
trim(adresse_structure_an) = 'OUI' or
trim(num_tel_an) = 'OUI' or
trim(nom_prenom_an) = 'OUI' or
trim(numero_piece_an) = 'OUI' or
trim(imei_an) = 'OUI' or
trim(adresse_an) = 'OUI' or
trim(statut_an) = 'OUI'
then 'NON' else 'OUI'
end) as est_conforme,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from (
select a.*,
row_number() over(partition by msisdn order by date_souscription desc nulls last) as rang2
from TMP.tt_flotte9 a ) b where rang2 = 1