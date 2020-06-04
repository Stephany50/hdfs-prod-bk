insert into cdr.spark_it_bdi_ameliore
select
trim(a.msisdn) AS msisdn,
trim(a.type_personne) AS type_personne,
trim(a.nom_prenom) AS nom_prenom,
trim(a.id_type_piece) AS id_type_piece,
trim(a.type_piece) AS type_piece,
trim(a.numero_piece) AS numero_piece,
trim(a.date_expiration) AS date_expiration,
trim(a.date_naissance) AS date_naissance,
trim(a.date_activation) AS date_activation,
trim(a.addresse) AS addresse,
trim(a.quartier) AS quartier,
trim(a.ville) AS ville,
trim(a.statut_bscs) AS statut_bscs,
trim(a.statut_validation_bo) AS statut_validation_bo,
trim(a.motif_rejet_bo) AS motif_rejet_bo,
trim(a.date_validation_bo) AS date_validation_bo,
trim(a.login_validateur_bo) AS login_validateur_bo,
trim(a.canal_validateur_bo) AS canal_validateur_bo,
trim(a.type_abonnement) AS type_abonnement,
trim(a.csmoddate) AS csmoddate,
trim(a.ccmoddate) AS ccmoddate,
trim(a.compte_client_structure) AS compte_client_structure,
trim(a.nom_structure) AS nom_structure,
trim(a.numero_registre_commerce) AS numero_registre_commerce,
trim(a.numero_piece_rep_legal) AS numero_piece_rep_legal,
trim(a.imei) AS imei,
trim(a.statut_derogation) AS statut_derogation,
trim(a.region_administrative) AS region_administrative,
trim(a.region_commerciale) AS region_commerciale,
trim(a.site_name) AS site_name,
trim(a.ville_site) AS ville_site,
trim(a.offre_commerciale) AS offre_commerciale,
trim(a.type_contrat) AS type_contrat,
trim(a.segmentation) AS segmentation,
trim(a.score_vip) AS score_vip,
trim(a.date_souscription) AS date_souscription,
trim(a.date_changement_statut) AS date_changement_statut,
trim(a.ville_structure) AS ville_structure,
trim(a.quartier_structure) AS quartier_structure,
trim(a.raison_statut) AS raison_statut,
trim(a.prenom) AS prenom,
trim(a.nom) AS nom,
trim(a.customer_id) AS customer_id,
trim(a.contract_id) AS contract_id,
trim(a.compte_client) AS compte_client,
trim(a.plan_localisation) AS plan_localisation,
trim(a.contrat_soucription) AS contrat_soucription,
trim(a.acceptation_cgv) AS acceptation_cgv,
trim(a.disponibilite_scan) AS disponibilite_scan,
trim(a.nom_parent) AS nom_parent,
trim(a.prenom_tuteur) AS prenom_tuteur,
trim(a.date_naissance_tuteur) AS date_naissance_tuteur,
trim(a.numero_piece_tuteur) AS numero_piece_tuteur,
trim(a.date_expiration_tuteur) AS date_expiration_tuteur,
trim(a.id_type_piece_tuteur) AS id_type_piece_tuteur,
trim(a.type_piece_tuteur) AS type_piece_tuteur,
trim(a.adresse_tuteur) AS adresse_tuteur,
trim(a.identificateur) AS identificateur,
trim(a.localisation_identificateur) AS localisation_identificateur,
trim(a.profession) AS profession,
trim(a.odbincomingcalls) AS odbincomingcalls,
trim(a.odboutgoingcalls) AS odboutgoingcalls,
current_timestamp() AS INSERT_DATE,
'###SLICE_VALUE###' AS original_file_date
from (select * from cdr.spark_it_bdi_art where original_file_date=date_add(to_date('###SLICE_VALUE###'),1)) a
left join TMP.TT_LIGNE_ANOMALIE b
on substr(trim(a.msisdn),-9,9) = substr(trim(b.msisdn),-9,9)
where b.msisdn is null