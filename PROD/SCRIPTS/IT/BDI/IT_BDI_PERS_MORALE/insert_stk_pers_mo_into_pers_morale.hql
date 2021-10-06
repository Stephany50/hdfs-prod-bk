INSERT INTO TMP.TT_KYC_PERS_MORALE_B2B
SELECT
trim(b.compte_client) AS compte_client,
trim(s.raison_sociale) AS raison_sociale,
trim(s.nom_representant_legal) AS nom_representant_legal,
trim(s.prenom_representant_legal) AS prenom_representant_legal,
trim(s.cni_representant_local) AS cni_representant_local,
trim(s.contact_telephonique) AS contact_telephonique,
NULL AS ville_structure,
NULL AS quartier_structure,
trim(s.adresse_structure) AS adresse_structure,
trim(s.numero_registre_commerce) AS numero_registre_commerce,
NULL AS sms_contact,
NULL AS doc_plan_localisation,
NULL AS doc_fiche_souscription,
NULL AS acceptation_cgv,
NULL AS doc_attestation_cnps,
NULL AS doc_rccm,
NULL AS disponibilite_scan,
'STK' AS type_client
FROM (SELECT * FROM DIM.SPARK_DT_BDI_STK_PERS_MORALE where not(msisdn is null or trim(msisdn) = '')) s
JOIN (SELECT * FROM TMP.TT_KYC_PERS_PHYSIQUE_B2C where not(msisdn is null or trim(msisdn) = '')) b
ON  FN_FORMAT_MSISDN_TO_9DIGITS(trim(s.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
