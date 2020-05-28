insert into CDR.SPARK_IT_BDI_PERS_MORALE
select
trim(COMPTE_CLIENT)  AS  COMPTE_CLIENT,
trim(RAISON_SOCIALE)  AS  RAISON_SOCIALE,
trim(NOM_REPRESENTANT_LEGAL)  AS  NOM_REPRESENTANT_LEGAL,
trim(PRENOM_REPRESENTANT_LEGAL)  AS  PRENOM_REPRESENTANT_LEGAL,
trim(CNI_REPRESENTANT_LOCAL)  AS  CNI_REPRESENTANT_LOCAL,
trim(CONTACT_TELEPHONIQUE)  AS  CONTACT_TELEPHONIQUE,
trim(VILLE_STRUCTURE)  AS  VILLE_STRUCTURE,
trim(QUARTIER_STRUCTURE)  AS  QUARTIER_STRUCTURE,
trim(ADRESSE_STRUCTURE)  AS  ADRESSE_STRUCTURE,
trim(NUMERO_REGISTRE_COMMERCE)  AS  NUMERO_REGISTRE_COMMERCE,
trim(SMS_CONTACT)  AS  SMS_CONTACT,
trim(DOC_PLAN_LOCALISATION)  AS  DOC_PLAN_LOCALISATION,
trim(DOC_FICHE_SOUSCRIPTION)  AS  DOC_FICHE_SOUSCRIPTION,
trim(ACCEPTATION_CGV)  AS  ACCEPTATION_CGV,
trim(DOC_ATTESTATION_CNPS)  AS  DOC_ATTESTATION_CNPS,
trim(DOC_RCCM)  AS  DOC_RCCM,
trim(DISPONIBILITE_SCAN)  AS  DISPONIBILITE_SCAN,
current_timestamp() AS insert_date,
trim(type_client) AS  type_client,
'###SLICE_VALUE###' AS original_file_date
from TMP.TT_BDI_PERS_MORALE_TMP