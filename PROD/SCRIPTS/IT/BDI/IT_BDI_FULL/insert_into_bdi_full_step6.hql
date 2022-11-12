--- Etape 6 : Ettiquetage des lignes PDV.
insert into TMP.TT_KYC_BDI_FULL_ST6
select
a.guid,
a.cust_guid,
a.msisdn,
(case when b.msisdn is not null then 'PDV' else a.type_personne end) type_personne,
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
adresse_structure,
imei,
odbincomingcalls,
odboutgoingcalls,
statut_hlr,
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
type_piece_tuteur,select REGION,NOM_SITE,LATITUDE_DEC,LONGITUDE_DEC from TMP.KYC_CEM_INDICATEURS_L1_B2B_LOC where LATITUDE_DEC <LONGITUDE_DEC limit 10;
adresse_tuteur,
identificateur,
localisation_identificateur,
profession
from TMP.TT_KYC_BDI_FULL_ST5 a 
left join DIM.SPARK_KYC_PDV b on a.msisdn=b.msisdn