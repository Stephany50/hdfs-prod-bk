insert into CDR.SPARK_IT_BDI_ART
select
trim(msisdn) AS msisdn,
trim(type_personne) AS type_personne,
trim(nom_prenom) AS nom_prenom,
trim(id_type_piece) AS id_type_piece,
trim(type_piece) AS type_piece,
trim(numero_piece) AS numero_piece,
trim(date_expiration) AS date_expiration,
trim(date_naissance) AS date_naissance,
trim(date_activation) AS date_activation,
trim(addresse) AS addresse,
trim(quartier) AS quartier,
trim(ville) AS ville,
trim(statut_bscs) AS statut_bscs,
trim(statut_validation_bo) AS statut_validation_bo,
trim(motif_rejet_bo) AS motif_rejet_bo,
trim(date_validation_bo) AS date_validation_bo,
trim(login_validateur_bo) AS login_validateur_bo,
trim(canal_validateur_bo) AS canal_validateur_bo,
trim(type_abonnement) AS type_abonnement,
trim(csmoddate) AS csmoddate,
trim(ccmoddate) AS ccmoddate,
trim(compte_client_structure) AS compte_client_structure,
trim(nom_structure) AS nom_structure,
trim(numero_registre_commerce) AS numero_registre_commerce,
trim(numero_piece_rep_legal) AS numero_piece_rep_legal,
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
trim(date_souscription) AS date_souscription,
trim(date_changement_statut) AS date_changement_statut,
trim(ville_structure) AS ville_structure,
trim(quartier_structure) AS quartier_structure,
trim(raison_statut) AS raison_statut,
trim(prenom) AS prenom,
trim(nom) AS nom,
trim(customer_id) AS customer_id,
trim(contract_id) AS contract_id,
trim(compte_client) AS compte_client,
trim(plan_localisation) AS plan_localisation,
trim(contrat_soucription) AS contrat_soucription,
trim(acceptation_cgv) AS acceptation_cgv,
trim(disponibilite_scan) AS disponibilite_scan,
trim(nom_parent) AS nom_parent,
trim(prenom_tuteur) AS prenom_tuteur,
trim(date_naissance_tuteur) AS date_naissance_tuteur,
trim(numero_piece_tuteur) AS numero_piece_tuteur,
trim(date_expiration_tuteur) AS date_expiration_tuteur,
trim(id_type_piece_tuteur) AS id_type_piece_tuteur,
trim(type_piece_tuteur) AS type_piece_tuteur,
trim(adresse_tuteur) AS adresse_tuteur,
trim(identificateur) AS identificateur,
trim(localisation_identificateur) AS localisation_identificateur,
trim(profession) AS profession,
trim(odbincomingcalls) AS odbincomingcalls,
trim(odboutgoingcalls) AS odboutgoingcalls,
insert_date,
original_file_date
from (
select a.*,
row_number() over(partition by msisdn order by date_activation desc nulls last) as rang
from TMP.TT_IT_BDI_ART1 a
) b where rang = 1