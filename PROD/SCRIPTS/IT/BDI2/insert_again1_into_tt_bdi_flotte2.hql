insert into TMP.TT_BDI_LIGNE_FLOTTE2
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
current_timestamp() AS insert_date,
'###SLICE_VALUE###' AS original_file_date
from (
select
msisdn,
customer_id,
contract_id,
compte_client,
(case when upper(trim(OFFRE_COMMERCIALE)) like '%POSTPAID%DATALIVE%' OR
upper(trim(OFFRE_COMMERCIALE)) like '%POSTPAID%GPRSTRACKING%' OR
upper(trim(OFFRE_COMMERCIALE)) like '%POSTPAID%SMARTRACK%' OR
upper(trim(OFFRE_COMMERCIALE)) like '%PREPAID%DATALIVE%'
then 'M2M'
when upper(trim(compte_client)) like '4.%' then 'FLOTTE'
else 'PP'
end) as type_personne,
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
statut_derogation as derogation_identification
from  TMP.TT_BDI3_1
) a where upper(trim(type_personne)) in ('FLOTTE','M2M')