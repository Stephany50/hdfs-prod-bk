INSERT INTO TMP.TT_BDI_PERS_MORALE_TMP
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
FROM (SELECT * FROM CDR.spark_it_bdi_stk_pers_morale where original_file_date = '2020-03-13') s
JOIN (SELECT * FROM TMP.TT_BDI_TMP1) b
ON  FN_FORMAT_MSISDN_TO_9DIGITS(trim(s.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
