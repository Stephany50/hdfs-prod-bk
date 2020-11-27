insert into TMP.TT_IT_BDI_FULL_2A
select
msisdn,
(case when upper(trim(type_personne)) = 'PP' and upper(trim(compte_client)) like '4.%'
then (
case when upper(trim(OFFRE_COMMERCIALE)) like '%POSTPAID%DATALIVE%' OR
upper(trim(OFFRE_COMMERCIALE)) like '%POSTPAID%GPRSTRACKING%' OR
upper(trim(OFFRE_COMMERCIALE)) like '%POSTPAID%SMARTRACK%' OR
upper(trim(OFFRE_COMMERCIALE)) like '%PREPAID%DATALIVE%'
then 'M2M'
else 'FLOTTE'
end )
else type_personne
end) as type_personne,
nom_prenom,
id_type_piece,
type_piece,
numero_piece,
date_expiration,
date_naissance,
date_activation,
addresse,
quartier,
ville,
statut_bscs,
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
numero_piece_rep_legal,
imei,
statut_derogation,
region_administrative,
region_commerciale,
site_name,
ville_site,
offre_commerciale,
type_contrat,
segmentation,
score_vip,
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
nom_parent,
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
odbincomingcalls,
odboutgoingcalls,
derogation_identification,
current_timestamp() AS insert_date,
'###SLICE_VALUE###' AS original_file_date
from (
select b.*,
row_number() over(partition by msisdn order by DATE_ACTIVATION2 desc nulls last) as rang
from (
select a.*,
(CASE
WHEN trim(a.DATE_ACTIVATION) IS NULL OR trim(a.DATE_ACTIVATION) = '' THEN NULL
WHEN trim(a.DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(a.DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION2
from (
select A.*
from
(select
msisdn,
type_personne,
nom_prenom,
id_type_piece,
type_piece,
numero_piece,
date_expiration,
date_naissance,
date_activation,
adresse as addresse,
quartier,
ville,
statut as statut_bscs,
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
numero_piece_representant_legal as numero_piece_rep_legal,
imei,
statut_derogation,
region_administrative,
region_commerciale,
site_name,
ville_site,
offre_commerciale,
type_contrat,
segmentation,
score_vip,
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
nom_tuteur as nom_parent,
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
odbincomingcalls,
odboutgoingcalls,
null derogation_identification,
CURRENT_TIMESTAMP AS insert_date,
'###SLICE_VALUE###' AS original_file_date
from TMP.TT_BDI3_1) A
union all
select B.*
from
(select
msisdn,
type_personne,
nom_prenom,
id_type_piece,
type_piece,
numero_piece,
date_expiration,
date_naissance,
date_activation,
adresse as addresse,
quartier,
ville,
statut as statut_bscs,
null as statut_validation_bo,
null AS motif_rejet_bo,
null AS date_validation_bo,
null AS login_validateur_bo,
null AS canal_validateur_bo,
null AS type_abonnement,
null AS csmoddate,
null AS ccmoddate,
compte_client_structure,
nom_structure,
numero_registre_commerce,
numero_piece_representant_legal  as numero_piece_rep_legal,
imei,
statut_derogation,
region_administrative,
region_commerciale,
site_name,
ville_site,
offre_commerciale,
type_contrat,
segmentation,
null as score_vip,
date_souscription,
date_changement_statut,
null as ville_structure,
null as quartier_structure,
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
nom_tuteur as nom_parent,
prenom_tuteur,
date_naissance_tuteur,
numero_piece_tuteur,
date_expiration_tuteur,
null as id_type_piece_tuteur,
type_piece_tuteur,
adresse_tuteur,
null as identificateur,
null as localisation_identificateur,
null as profession,
odbincomingcalls,
odboutgoingcalls,
derogation_identification,
CURRENT_TIMESTAMP AS insert_date,
'###SLICE_VALUE###' AS original_file_date
from TMP.TT_BDI_LIGNE_FLOTTE2) B
) a
join (select *
from cdr.spark_it_bdi_zsmart
where original_file_date='###SLICE_VALUE###'
) zsm
on substr(upper(trim(a.msisdn)),-9,9) = substr(upper(trim(zsm.msisdn)),-9,9)
) b
) c
where rang = 1