INSERT INTO TMP.TT_BDI_PERS_MORALE_TMP
SELECT
nvl(B.compte_client,A.compte_client) as compte_client,
case when B.raison_sociale is null or trim(B.raison_sociale) = '' then A.raison_sociale else B.raison_sociale end as raison_sociale,
case when B.nom_representant_legal is null or trim(B.nom_representant_legal) = '' then A.nom_representant_legal else B.nom_representant_legal end as nom_representant_legal,
trim(A.prenom_representant_legal) as prenom_representant_legal,
case when B.cni_representant_local is null or trim(B.cni_representant_local) = '' then A.cni_representant_local else B.cni_representant_local end as cni_representant_local,
trim(A.contact_telephonique) as contact_telephonique,
case when B.ville_structure is null or trim(B.ville_structure) = '' then A.ville_structure else B.ville_structure end as ville_structure,
case when B.quartier_structure is null or trim(B.quartier_structure) = '' then A.quartier_structure else B.quartier_structure end as quartier_structure,
case when B.adresse_structure is null or trim(B.adresse_structure) = '' then A.adresse_structure else B.adresse_structure end as adresse_structure,
case when B.numero_registre_commerce is null or trim(B.numero_registre_commerce) = '' then A.numero_registre_commerce else B.numero_registre_commerce end as numero_registre_commerce,
trim(A.sms_contact) as sms_contact,
trim(A.doc_plan_localisation) as doc_plan_localisation,
trim(A.doc_fiche_souscription) as doc_fiche_souscription,
trim(A.acceptation_cgv) as acceptation_cgv,
trim(A.doc_attestation_cnps) as doc_attestation_cnps,
trim(A.doc_rccm) as doc_rccm,
trim(A.disponibilite_scan) as disponibilite_scan,
case when B.typeclient is null or trim(B.typeclient) = '' then A.type_client else B.typeclient end as type_client
FROM (SELECT * FROM  CDR.SPARK_IT_BDI_PERS_MORALE WHERE ORIGINAL_FILE_DATE=DATE_SUB('###SLICE_VALUE###',1)
 and not(compte_client is null or trim(compte_client) = '')) A
FULL OUTER JOIN
(SELECT * FROM CDR.SPARK_IT_BDI_CRM_B2B where original_file_date = '###SLICE_VALUE###'
 and not(compte_client is null or trim(compte_client) = '')) B
ON trim(A.compte_client) = trim(B.compte_client)