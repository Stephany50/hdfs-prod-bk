insert into cdr.spark_it_bdi
select
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.msisdn),'\n+','n'),'[|]+',' ')),'"+','') AS msisdn,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.type_personne),'\n+','n'),'[|]+',' ')),'"+','') AS type_personne,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.nom_prenom),'\n+','n'),'[|]+',' ')),'"+','') AS nom_prenom,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.id_type_piece),'\n+','n'),'[|]+',' ')),'"+','') AS id_type_piece,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.type_piece),'\n+','n'),'[|]+',' ')),'"+','') AS type_piece,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.numero_piece),'\n+','n'),'[|]+',' ')),'"+','') AS numero_piece,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.date_expiration),'\n+','n'),'[|]+',' ')),'"+','') AS date_expiration,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.date_naissance),'\n+','n'),'[|]+',' ')),'"+','') AS date_naissance,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.date_activation),'\n+','n'),'[|]+',' ')),'"+','') AS date_activation,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.addresse),'\n+','n'),'[|]+',' ')),'"+','') AS addresse,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.quartier),'\n+','n'),'[|]+',' ')),'"+','') AS quartier,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.ville),'\n+','n'),'[|]+',' ')),'"+','') AS ville,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.statut_bscs),'\n+','n'),'[|]+',' ')),'"+','') AS statut_bscs,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.statut_validation_bo),'\n+','n'),'[|]+',' ')),'"+','') AS statut_validation_bo,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.motif_rejet_bo),'\n+','n'),'[|]+',' ')),'"+','') AS motif_rejet_bo,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.date_validation_bo),'\n+','n'),'[|]+',' ')),'"+','') AS date_validation_bo,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.login_validateur_bo),'\n+','n'),'[|]+',' ')),'"+','') AS login_validateur_bo,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.canal_validateur_bo),'\n+','n'),'[|]+',' ')),'"+','') AS canal_validateur_bo,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.type_abonnement),'\n+','n'),'[|]+',' ')),'"+','') AS type_abonnement,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.csmoddate),'\n+','n'),'[|]+',' ')),'"+','') AS csmoddate,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.ccmoddate),'\n+','n'),'[|]+',' ')),'"+','') AS ccmoddate,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.compte_client_structure),'\n+','n'),'[|]+',' ')),'"+','') AS compte_client_structure,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.nom_structure),'\n+','n'),'[|]+',' ')),'"+','') AS nom_structure,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.numero_registre_commerce),'\n+','n'),'[|]+',' ')),'"+','') AS numero_registre_commerce,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.numero_piece_rep_legal),'\n+','n'),'[|]+',' ')),'"+','') AS numero_piece_rep_legal,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.imei),'\n+','n'),'[|]+',' ')),'"+','') AS imei,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.statut_derogation),'\n+','n'),'[|]+',' ')),'"+','') AS statut_derogation,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.region_administrative),'\n+','n'),'[|]+',' ')),'"+','') AS region_administrative,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.region_commerciale),'\n+','n'),'[|]+',' ')),'"+','') AS region_commerciale,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.site_name),'\n+','n'),'[|]+',' ')),'"+','') AS site_name,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.ville_site),'\n+','n'),'[|]+',' ')),'"+','') AS ville_site,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.offre_commerciale),'\n+','n'),'[|]+',' ')),'"+','') AS offre_commerciale,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.type_contrat),'\n+','n'),'[|]+',' ')),'"+','') AS type_contrat,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.segmentation),'\n+','n'),'[|]+',' ')),'"+','') AS segmentation,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.score_vip),'\n+','n'),'[|]+',' ')),'"+','') AS score_vip,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.date_souscription),'\n+','n'),'[|]+',' ')),'"+','') AS date_souscription,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.date_changement_statut),'\n+','n'),'[|]+',' ')),'"+','') AS date_changement_statut,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.ville_structure),'\n+','n'),'[|]+',' ')),'"+','') AS ville_structure,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.quartier_structure),'\n+','n'),'[|]+',' ')),'"+','') AS quartier_structure,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.raison_statut),'\n+','n'),'[|]+',' ')),'"+','') AS raison_statut,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.prenom),'\n+','n'),'[|]+',' ')),'"+','') AS prenom,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.nom),'\n+','n'),'[|]+',' ')),'"+','') AS nom,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.customer_id),'\n+','n'),'[|]+',' ')),'"+','') AS customer_id,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.contract_id),'\n+','n'),'[|]+',' ')),'"+','') AS contract_id,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.compte_client),'\n+','n'),'[|]+',' ')),'"+','') AS compte_client,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.plan_localisation),'\n+','n'),'[|]+',' ')),'"+','') AS plan_localisation,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.contrat_soucription),'\n+','n'),'[|]+',' ')),'"+','') AS contrat_soucription,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.acceptation_cgv),'\n+','n'),'[|]+',' ')),'"+','') AS acceptation_cgv,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.disponibilite_scan),'\n+','n'),'[|]+',' ')),'"+','') AS disponibilite_scan,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.nom_parent),'\n+','n'),'[|]+',' ')),'"+','') AS nom_parent,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.prenom_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS prenom_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.date_naissance_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS date_naissance_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.numero_piece_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS numero_piece_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.date_expiration_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS date_expiration_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.id_type_piece_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS id_type_piece_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.type_piece_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS type_piece_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.adresse_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS adresse_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.identificateur),'\n+','n'),'[|]+',' ')),'"+','') AS identificateur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.localisation_identificateur),'\n+','n'),'[|]+',' ')),'"+','') AS localisation_identificateur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.profession),'\n+','n'),'[|]+',' ')),'"+','') AS profession,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.odbincomingcalls),'\n+','n'),'[|]+',' ')),'"+','') AS odbincomingcalls,
regexp_replace(trim(regexp_replace(regexp_replace(trim(it.odboutgoingcalls),'\n+','n'),'[|]+',' ')),'"+','') AS odboutgoingcalls,
current_timestamp() insert_date,
'###SLICE_VALUE###' original_file_date
from (
select X.*,
row_number() over(partition by X.msisdn order by X.date_activation2 DESC NULLS LAST) AS RANG
from (
select it.*,
(CASE
WHEN trim(it.DATE_ACTIVATION) IS NULL OR trim(it.DATE_ACTIVATION) = '' THEN NULL
WHEN trim(it.DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(it.DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(it.DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(it.DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION2
from (
select
trim(e.msisdn) AS msisdn,
trim(type_personne) AS type_personne,
trim(nom_prenom) AS nom_prenom,
trim(id_type_piece) AS id_type_piece,
trim(type_piece) AS type_piece,
trim(translate(trim(numero_piece),'|','')) AS numero_piece,
date_expiration,
date_naissance,
date_activation,
upper(trim(translate(trim(translate(lower(trim(adresse)),'|éèêïîüûöôëàâäû','')),';',' '))) AS addresse,
trim(quartier) AS quartier,
trim(ville) AS ville,
trim(statut) AS statut_bscs,
trim(statut_validation_bo) AS statut_validation_bo,
trim(motif_rejet_bo) AS motif_rejet_bo,
date_validation_bo,
trim(login_validateur_bo) AS login_validateur_bo,
trim(canal_validateur_bo) AS canal_validateur_bo,
trim(type_abonnement) AS type_abonnement,
trim(csmoddate) AS csmoddate,
trim(ccmoddate) AS ccmoddate,
trim(compte_client_structure) AS compte_client_structure,
trim(translate(trim(nom_structure),'|éèêïîüûöôëàâäû','')) AS nom_structure,
trim(translate(trim(numero_registre_commerce),'|','') ) AS numero_registre_commerce,
translate(trim(numero_piece_representant_legal),'|','') AS numero_piece_rep_legal,
trim(imei) AS imei,
trim(statut_derogation) AS statut_derogation,
trim(region_administrative) AS region_administrative,
trim(region_commerciale) AS region_commerciale,
trim(site_name) AS site_name,
trim(ville_site) AS ville_site,
trim(offre_commerciale) AS offre_commerciale,
trim(type_contrat) AS type_contrat,
trim(segmentation) AS segmentation,
trim(score_vip) AS score_vip,
date_souscription,
date_changement_statut,
trim(ville_structure) AS ville_structure,
trim(quartier_structure) AS quartier_structure,
trim(raison_statut) AS raison_statut,
upper(trim(translate(lower(trim(prenom)),'|éèêïîüûöôëàâäû',''))) AS prenom,
upper(trim(translate(lower(trim(nom)),'|éèêïîüûöôëàâäû',''))) AS nom,
trim(customer_id) AS customer_id,
trim(contract_id) AS contract_id,
trim(compte_client) AS compte_client,
trim(plan_localisation) AS plan_localisation,
trim(contrat_soucription) AS contrat_soucription,
trim(acceptation_cgv) AS acceptation_cgv,
trim(disponibilite_scan) AS disponibilite_scan,
upper(trim(translate(trim(nom_tuteur),'|éèêïîüûöôëàâäû',''))) AS nom_parent,
trim(prenom_tuteur) AS prenom_tuteur,
date_naissance_tuteur,
trim(translate(trim(numero_piece_tuteur),'|',''))  AS numero_piece_tuteur,
trim(date_expiration_tuteur) AS date_expiration_tuteur,
trim(id_type_piece_tuteur) AS id_type_piece_tuteur,
trim(type_piece_tuteur) AS type_piece_tuteur,
trim(translate(trim(adresse_tuteur),'|éèêïîüûöôëàâäû','')) AS adresse_tuteur,
trim(identificateur) AS identificateur,
trim(localisation_identificateur) AS localisation_identificateur,
trim(profession) AS profession,
trim(odbincomingcalls) AS odbincomingcalls,
trim(odboutgoingcalls) AS odboutgoingcalls,
current_timestamp() AS insert_date,
'###SLICE_VALUE###' AS original_file_date
from (select * from TMP.TT_BDI3_1) e
left join
(select distinct trim(msisdn) msisdn
from CDR.SPARK_IT_BDI_LIGNE_FLOTTE
where original_file_date='###SLICE_VALUE###') f
on substr(upper(trim(e.msisdn)),-9,9) = substr(upper(trim(f.msisdn)),-9,9)
where f.msisdn is null
) it
join (select *
from cdr.spark_it_bdi_zsmart
where original_file_date='###SLICE_VALUE###'
) zsm
on substr(upper(trim(it.msisdn)),-9,9) = substr(upper(trim(zsm.msisdn)),-9,9)
) X
) it where RANG = 1