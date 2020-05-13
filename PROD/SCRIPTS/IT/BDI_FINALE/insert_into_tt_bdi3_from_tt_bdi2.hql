insert into TMP.tt_bdi3
select
trim(e.msisdn) AS msisdn,
trim(e.type_personne) AS type_personne,
trim(e.nom_prenom) AS nom_prenom,
trim(e.id_type_piece) AS id_type_piece,
trim(e.type_piece) AS type_piece,
IF(TRIM(e.NUMERO_PIECE) IN ('NI', 'N I', 'ni', '1122334455', '123456789'),'112233445',e.NUMERO_PIECE) NUMERO_PIECE,
trim(e.date_expiration) AS date_expiration,
trim(e.date_naissance) AS date_naissance,
trim(e.date_activation) AS date_activation,
trim(e.adresse) AS adresse,
trim(e.quartier) AS quartier,
trim(e.ville) AS ville,
trim(e.statut) AS statut,
trim(e.statut_validation_bo) AS statut_validation_bo,
trim(e.motif_rejet_bo) AS motif_rejet_bo,
trim(e.date_validation_bo) AS date_validation_bo,
trim(e.login_validateur_bo) AS login_validateur_bo,
trim(e.canal_validateur_bo) AS canal_validateur_bo,
trim(e.type_abonnement) AS type_abonnement,
trim(e.csmoddate) AS csmoddate,
trim(e.ccmoddate) AS ccmoddate,
trim(e.compte_client_structure) AS compte_client_structure,
trim(e.nom_structure) AS nom_structure,
trim(e.numero_registre_commerce) AS numero_registre_commerce,
trim(e.numero_piece_representant_legal) AS numero_piece_representant_legal,
trim(e.imei) AS imei,
trim(e.statut_derogation) AS statut_derogation,
trim(e.region_administrative) AS region_administrative,
trim(e.region_commerciale) AS region_commerciale,
trim(e.site_name) AS site_name,
trim(e.ville_site) AS ville_site,
trim(e.offre_commerciale) AS offre_commerciale,
trim(e.type_contrat) AS type_contrat,
trim(e.segmentation) AS segmentation,
trim(e.score_vip) AS score_vip,
trim(e.date_souscription) AS date_souscription,
trim(e.date_changement_statut) AS date_changement_statut,
trim(e.ville_structure) AS ville_structure,
trim(e.quartier_structure) AS quartier_structure,
trim(e.raison_statut) AS raison_statut,
trim(e.prenom) AS prenom,
trim(e.nom) AS nom,
trim(e.customer_id) AS customer_id,
trim(e.contract_id) AS contract_id,
trim(e.compte_client) AS compte_client,
case when e.PLAN_LOCALISATION  is null or trim(e.PLAN_LOCALISATION ) = '' 
    then 'data10/PL/MAKEPE.jpeg' else e.PLAN_LOCALISATION
end AS  PLAN_LOCALISATION,
case when e.CONTRAT_SOUCRIPTION  is null OR trim(e.CONTRAT_SOUCRIPTION) = ''
     then 'data11/' || IF(e.MSISDN is null or trim(e.MSISDN) = '','',SUBSTR(trim(e.MSISDN),7,3)) || '/' || nvl(trim(e.msisdn),'') || '.jpeg'
end AS CONTRAT_SOUCRIPTION,
trim(e.acceptation_cgv) AS acceptation_cgv,
trim(e.disponibilite_scan) AS disponibilite_scan,
trim(e.nom_tuteur) AS nom_tuteur,
trim(e.prenom_tuteur) AS prenom_tuteur,
trim(e.date_naissance_tuteur) AS date_naissance_tuteur,
trim(e.numero_piece_tuteur) AS numero_piece_tuteur,
trim(e.date_expiration_tuteur) AS date_expiration_tuteur,
trim(e.id_type_piece_tuteur) AS id_type_piece_tuteur,
trim(e.type_piece_tuteur) AS type_piece_tuteur,
trim(e.adresse_tuteur) AS adresse_tuteur,
trim(e.identificateur) AS identificateur,
trim(e.localisation_identificateur) AS localisation_identificateur,
trim(e.profession) AS profession,
trim(e.odbincomingcalls) AS odbincomingcalls,
trim(e.odboutgoingcalls) AS odboutgoingcalls
from
(
select
c.*
from
(
select
a.*
from (select * from  TMP.TT_BDI2) a
left join (select * from  DIM.SPARK_DT_MSISDN_FAMOCO ) b
on substr(trim(a.msisdn),-9,9) = substr(trim(b.msisdn),-9,9)
where b.msisdn is null
) c
left join(select * from  CDR.SPARK_IT_OM_PERS_MORALE where original_file_date='2020-03-13') d
on substr(trim(c.msisdn),-9,9) = substr(trim(d.msisdn),-9,9)
where d.msisdn is null
) e
left join (select * from CDR.SPARK_IT_OM_FLOTTE  where original_file_date='2020-03-13') f
on substr(trim(e.msisdn),-9,9) = substr(trim(f.msisdn),-9,9)
where f.msisdn is null