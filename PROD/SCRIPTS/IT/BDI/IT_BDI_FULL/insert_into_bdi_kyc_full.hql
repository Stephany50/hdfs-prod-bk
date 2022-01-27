--- Etapes 6 : Scoring + dedoublonnage + insertion dans la table finale
insert into CDR.SPARK_IT_KYC_BDI_FULL
select  
`(rang)?+.+`,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from (select
A.guid,
A.cust_guid,
A.msisdn,
type_personne,
nom_prenom,
id_type_piece,
type_piece,
numero_piece,
date_expiration,
date_naissance,
date_activation,
(case when nvl(trim(VILLE_SITE),'') = '' and nvl(trim(site_name),'') = '' then adresse
when nvl(trim(VILLE_SITE),'') = '' and nvl(trim(site_name),'') <> '' then trim(site_name)
when nvl(trim(site_name),'') = ''  and nvl(trim(VILLE_SITE),'') <> '' then trim(VILLE_SITE)
else concat_ws(',',nvl(trim(VILLE_SITE),''),nvl(trim(site_name),''))
end) as adresse,
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
B.classe as score_vip,
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
row_number() over (partition by A.msisdn order by to_date(A.date_activation) desc nulls last) rang
from (select * from TMP.TT_KYC_BDI_FULL_ST5) A
left join (select * from DIM.DT_VIP_SCORING_REF) B 
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.msisdn)))
where rang=1

