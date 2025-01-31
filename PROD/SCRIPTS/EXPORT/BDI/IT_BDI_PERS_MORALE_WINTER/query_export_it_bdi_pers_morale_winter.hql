select
nvl(trim(replace(compte_client,';',' ')),'') as compte_client,
nvl(trim(replace(raison_sociale,';',' ')),'') as raison_sociale,
nvl(trim(replace(nom_representant_legal,';',' ')),'') as nom_representant_legal,
nvl(trim(replace(prenom_representant_legal,';',' ')),'') as prenom_representant_legal,
nvl(trim(replace(cni_representant_local,';',' ')),'') as cni_representant_local,
nvl(trim(replace(contact_telephonique,';',' ')),'') as contact_telephonique,
nvl(trim(replace(ville_structure,';',' ')),'') as ville_structure,
nvl(trim(replace(quartier_structure,';',' ')),'') as quartier_structure,
nvl(trim(replace(adresse_structure,';',' ')),'') as adresse_structure,
nvl(trim(replace(numero_registre_commerce,';',' ')),'') as numero_registre_commerce,
nvl(trim(replace(sms_contact,';',' ')),'') as sms_contact,
nvl(trim(replace(doc_plan_localisation,';',' ')),'') as doc_plan_localisation,
nvl(trim(replace(doc_fiche_souscription,';',' ')),'') as doc_fiche_souscription,
nvl(trim(replace(acceptation_cgv,';',' ')),'') as acceptation_cgv,
nvl(trim(replace(doc_attestation_cnps,';',' ')),'') as doc_attestation_cnps,
nvl(trim(replace(doc_rccm,';',' ')),'') as doc_rccm,
nvl(trim(replace(disponibilite_scan,';',' ')),'') as disponibilite_scan,
nvl(insert_date,'') as insert_date,
nvl(trim(replace(type_client,';',' ')),'') as type_client,
nvl(event_date,'') as original_file_date
-- from CDR.SPARK_IT_BDI_PERS_MORALE where original_file_date='###SLICE_VALUE###'
from mon.spark_ft_kyc_crm_B2B where event_date = '###SLICE_VALUE###'