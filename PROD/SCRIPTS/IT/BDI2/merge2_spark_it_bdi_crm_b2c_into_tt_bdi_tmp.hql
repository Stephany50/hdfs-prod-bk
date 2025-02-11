INSERT INTO TMP.TT_BDI_TMP_1A
SELECT
nvl(B.msisdn,A.msisdn) as msisdn,
case when B.type_personne is null or trim(B.type_personne) = '' then A.type_personne else B.type_personne end as type_personne,
case when B.nom_prenom is null or trim(B.nom_prenom) = '' then A.nom_prenom else B.nom_prenom end as nom_prenom,
case when B.id_type_piece is null or trim(B.id_type_piece) = '' then A.id_type_piece else B.id_type_piece end as id_type_piece,
case when B.type_piece is null or trim(B.type_piece) = '' then A.type_piece else B.type_piece end as type_piece,
case when B.numero_piece is null or trim(B.numero_piece) = '' then A.numero_piece else B.numero_piece end as numero_piece,
case when B.date_expiration is null or trim(B.date_expiration) = '' then A.date_expiration else B.date_expiration end as date_expiration,
case when B.date_naissance is null or trim(B.date_naissance) = '' then A.date_naissance else B.date_naissance end as date_naissance,
case when B.date_activation is null or trim(B.date_activation) = '' then A.date_activation else B.date_activation end as date_activation,
case when B.adresse is null or trim(B.adresse) = '' then A.adresse else B.adresse end as adresse,
case when B.quartier is null or trim(B.quartier) = '' then A.quartier else B.quartier end as quartier,
case when B.ville is null or trim(B.ville) = '' then A.ville else B.ville end as ville,
case when B.statut is null or trim(B.statut) = '' then A.statut else B.statut end as statut,
case when A.statut_validation_bo is null or trim(A.statut_validation_bo) = '' then B.statut_validation_bo else A.statut_validation_bo end as statut_validation_bo,
case when B.motif_rejet_bo is null or trim(B.motif_rejet_bo) = '' then A.motif_rejet_bo else B.motif_rejet_bo end as motif_rejet_bo,
case when B.date_validation_bo is null or trim(B.date_validation_bo) = '' then A.date_validation_bo else B.date_validation_bo end as date_validation_bo,
case when B.login_validateur_bo is null or trim(B.login_validateur_bo) = '' then A.login_validateur_bo else B.login_validateur_bo end as login_validateur_bo,
case when B.canal_validateur_bo is null or trim(B.canal_validateur_bo) = '' then A.canal_validateur_bo else B.canal_validateur_bo end as canal_validateur_bo,
case when B.type_abonnement is null or trim(B.type_abonnement) = '' then A.type_abonnement else B.type_abonnement end as type_abonnement,
case when B.csmoddate is null or trim(B.csmoddate) = '' then A.csmoddate else B.csmoddate end as csmoddate,
case when B.ccmoddate is null or trim(B.ccmoddate) = '' then A.ccmoddate else B.ccmoddate end as ccmoddate,
case when B.compte_client_structure is null or trim(B.compte_client_structure) = '' then A.compte_client_structure else B.compte_client_structure end as compte_client_structure,
case when B.nom_structure is null or trim(B.nom_structure) = '' then A.nom_structure else B.nom_structure end as nom_structure,
case when B.numero_registre_commerce is null or trim(B.numero_registre_commerce) = '' then A.numero_registre_commerce else B.numero_registre_commerce end as numero_registre_commerce,
case when B.numero_piece_representant_legal is null or trim(B.numero_piece_representant_legal) = '' then A.numero_piece_representant_lega else B.numero_piece_representant_legal end as numero_piece_representant_lega,
case when B.imei is null or trim(B.imei) = '' then A.imei else B.imei end as imei,
case when B.statut_derogation is null or trim(B.statut_derogation) = '' then A.statut_derogation else B.statut_derogation end as statut_derogation,
case when B.region_administrative is null or trim(B.region_administrative) = '' then A.region_administrative else B.region_administrative end as region_administrative,
case when B.region_commerciale is null or trim(B.region_commerciale) = '' then A.region_commerciale else B.region_commerciale end as region_commerciale,
case when B.site_name is null or trim(B.site_name) = '' then A.site_name else B.site_name end as site_name,
case when B.ville_site is null or trim(B.ville_site) = '' then A.ville_site else B.ville_site end as ville_site,
case when B.offre_commerciale is null or trim(B.offre_commerciale) = '' then A.offre_commerciale else B.offre_commerciale end as offre_commerciale,
case when B.type_contrat is null or trim(B.type_contrat) = '' then A.type_contrat else B.type_contrat end as type_contrat,
case when B.segmentation is null or trim(B.segmentation) = '' then A.segmentation else B.segmentation end as segmentation,
case when B.date_souscription is null or trim(B.date_souscription) = '' then A.date_souscription else B.date_souscription end as date_souscription,
case when B.date_changement_statut is null or trim(B.date_changement_statut) = '' then A.date_changement_statut else B.date_changement_statut end as date_changement_statut,
case when B.ville_structure is null or trim(B.ville_structure) = '' then A.ville_structure else B.ville_structure end as ville_structure,
case when B.quartier_structure is null or trim(B.quartier_structure) = '' then A.quartier_structure else B.quartier_structure end as quartier_structure,
case when B.raison_statut is null or trim(B.raison_statut) = '' then A.raison_statut else B.raison_statut end as raison_statut,
case when B.prenom is null or trim(B.prenom) = '' then A.prenom else B.prenom end as prenom,
case when B.nom is null or trim(B.nom) = '' then A.nom else B.nom end as nom,
case when B.customer_id is null or trim(B.customer_id) = '' then A.customer_id else B.customer_id end as customer_id,
case when B.contract_id is null or trim(B.contract_id) = '' then A.contract_id else B.contract_id end as contract_id,
case when B.compte_client is null or trim(B.compte_client) = '' then A.compte_client else B.compte_client end as compte_client,
case when B.plan_localisation is null or trim(B.plan_localisation) = '' then A.plan_localisation else B.plan_localisation end as plan_localisation,
case when B.contrat_soucription is null or trim(B.contrat_soucription) = '' then A.contrat_soucription else B.contrat_soucription end as contrat_soucription,
case when B.acceptation_cgv is null or trim(B.acceptation_cgv) = '' then A.acceptation_cgv else B.acceptation_cgv end as acceptation_cgv,
case when B.disponibilite_scan is null or trim(B.disponibilite_scan) = '' then A.disponibilite_scan else B.disponibilite_scan end as disponibilite_scan,
case when B.nom_tuteur is null or trim(B.nom_tuteur) = '' then A.nom_tuteur else B.nom_tuteur end as nom_tuteur,
case when B.prenom_tuteur is null or trim(B.prenom_tuteur) = '' then A.prenom_tuteur else B.prenom_tuteur end as prenom_tuteur,
case when B.date_naissance_tuteur is null or trim(B.date_naissance_tuteur) = '' then A.date_naissance_tuteur else B.date_naissance_tuteur end as date_naissance_tuteur,
case when B.numero_piece_tuteur is null or trim(B.numero_piece_tuteur) = '' then A.numero_piece_tuteur else B.numero_piece_tuteur end as numero_piece_tuteur,
case when B.date_expiration_tuteur is null or trim(B.date_expiration_tuteur) = '' then A.date_expiration_tuteur else B.date_expiration_tuteur end as date_expiration_tuteur,
case when B.id_type_piece_tuteur is null or trim(B.id_type_piece_tuteur) = '' then A.id_type_piece_tuteur else B.id_type_piece_tuteur end as id_type_piece_tuteur,
case when B.type_piece_tuteur is null or trim(B.type_piece_tuteur) = '' then A.type_piece_tuteur else B.type_piece_tuteur end as type_piece_tuteur,
case when B.adresse_tuteur is null or trim(B.adresse_tuteur) = '' then A.adresse_tuteur else B.adresse_tuteur end as adresse_tuteur,
case when B.identificateur is null or trim(B.identificateur) = '' then A.identificateur else B.identificateur end as identificateur,
case when B.localisation_identificateur is null or trim(B.localisation_identificateur) = '' then A.localisation_identificateur else B.localisation_identificateur end as localisation_identificateur,
case when B.profession is null or trim(B.profession) = '' then A.profession else B.profession end as profession
FROM (select * from CDR.SPARK_IT_BDI_FULL where original_file_date=DATE_SUB('###SLICE_VALUE###',1)
 and not(msisdn is null or trim(msisdn) = '')) A
FULL OUTER JOIN
(SELECT * FROM CDR.SPARK_it_bdi_crm_b2c where original_file_date = '###SLICE_VALUE###' and not(msisdn is null or trim(msisdn) = '')) B
ON  FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.msisdn))