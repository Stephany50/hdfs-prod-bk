insert into TMP.tt_pm01
select distinct
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(compte_client,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS compte_client,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(raison_sociale,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS raison_sociale,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nom_representant_legal,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS nom_representant_legal,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(prenom_representant_legal,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS prenom_representant_legal,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(cni_representant_local,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS cni_representant_local,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(contact_telephonique,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS contact_telephonique,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(ville_structure,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS ville_structure,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(quartier_structure,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS quartier_structure,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(adresse_structure,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS adresse_structure,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(numero_registre_commerce,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS numero_registre_commerce,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sms_contact,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS sms_contact,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(doc_plan_localisation,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS doc_plan_localisation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(doc_fiche_souscription,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS doc_fiche_souscription,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(acceptation_cgv,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS acceptation_cgv,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(doc_attestation_cnps,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS doc_attestation_cnps,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(doc_rccm,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS doc_rccm,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(disponibilite_scan,'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),';',' ')) AS disponibilite_scan,
type_client
from cdr.spark_it_bdi_pers_morale_1A
where original_file_date=date_add('###SLICE_VALUE###',1)