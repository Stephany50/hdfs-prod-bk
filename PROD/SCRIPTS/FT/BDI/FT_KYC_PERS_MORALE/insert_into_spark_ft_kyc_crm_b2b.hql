--- Merge entre la jrn précédente de MON.SPARK_FT_KYC_CRM_B2B et le différentiel de l'IT CDR.SPARK_IT_KYC_CRM_B2B du jour-J
INSERT INTO  MON.SPARK_FT_KYC_CRM_B2B
SELECT *,
(case when not(RAISON_SOCIALE_AN='OUI' or RCCM_AN='OUI' or CNI_REPRESENTANT_LEGAL_AN='OUI' or ADRESSE_STRUCTURE_AN='OUI') then 'OUI' else 'NON' end) est_conforme,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
FROM (SELECT *,
(case when trim(RAISON_SOCIALE) = '' or RAISON_SOCIALE is null or
(trim(translate(trim(RAISON_SOCIALE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(RAISON_SOCIALE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(RAISON_SOCIALE)) < 2 then 'OUI' else 'NON'
end) RAISON_SOCIALE_AN,
(case
when trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null or
(trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(NUMERO_REGISTRE_COMMERCE)) < 2
then 'OUI' else 'NON'
end) RCCM_AN,
(case
when trim(CNI_REPRESENTANT_LOCAL) = '' or CNI_REPRESENTANT_LOCAL is NULL or
trim(CNI_REPRESENTANT_LOCAL) rlike '^(\\d)\\1+$' or
length(trim(CNI_REPRESENTANT_LOCAL)) > 21 or
trim(CNI_REPRESENTANT_LOCAL) like '112233445%' or
(trim(translate(lower(trim(CNI_REPRESENTANT_LOCAL)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(CNI_REPRESENTANT_LOCAL)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(CNI_REPRESENTANT_LOCAL)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(CNI_REPRESENTANT_LOCAL)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '')
then 'OUI' else 'NON'
end) CNI_REPRESENTANT_LEGAL_AN,
(case
when trim(ADRESSE_STRUCTURE) = '' or ADRESSE_STRUCTURE is NULL or
(trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(ADRESSE_STRUCTURE)) < 2
then 'OUI' else 'NON'
end) ADRESSE_STRUCTURE_AN
FROM (SELECT
nvl(B.GUID,A.GUID) GUID,
nvl(B.COMPTE_CLIENT,A.COMPTE_CLIENT) as COMPTE_CLIENT,
case when B.RAISON_SOCIALE is null or trim(B.RAISON_SOCIALE) = '' then A.RAISON_SOCIALE else B.RAISON_SOCIALE end as RAISON_SOCIALE,
case when B.NOM_REPRESENTANT_LEGAL is null or trim(B.NOM_REPRESENTANT_LEGAL) = '' then A.NOM_REPRESENTANT_LEGAL else B.NOM_REPRESENTANT_LEGAL end as NOM_REPRESENTANT_LEGAL,
trim(A.prenom_representant_legal) as prenom_representant_legal,
case when B.CNI_REPRESENTANT_LOCAL is null or trim(B.CNI_REPRESENTANT_LOCAL) = '' then A.CNI_REPRESENTANT_LOCAL else B.CNI_REPRESENTANT_LOCAL end as CNI_REPRESENTANT_LOCAL,
trim(A.CONTACT_TELEPHONIQUE) as CONTACT_TELEPHONIQUE,
case when B.VILLE_STRUCTURE is null or trim(B.VILLE_STRUCTURE) = '' then A.VILLE_STRUCTURE else B.VILLE_STRUCTURE end as VILLE_STRUCTURE,
case when B.QUARTIER_STRUCTURE is null or trim(B.QUARTIER_STRUCTURE) = '' then A.QUARTIER_STRUCTURE else B.QUARTIER_STRUCTURE end as QUARTIER_STRUCTURE,
case when B.ADRESSE_STRUCTURE is null or trim(B.ADRESSE_STRUCTURE) = '' then A.ADRESSE_STRUCTURE else B.ADRESSE_STRUCTURE end as ADRESSE_STRUCTURE,
case when B.NUMERO_REGISTRE_COMMERCE is null or trim(B.NUMERO_REGISTRE_COMMERCE) = '' then A.NUMERO_REGISTRE_COMMERCE else B.NUMERO_REGISTRE_COMMERCE end as NUMERO_REGISTRE_COMMERCE,
trim(A.SMS_CONTACT) as SMS_CONTACT,
trim(A.doc_plan_localisation) as doc_plan_localisation,
trim(A.doc_fiche_souscription) as doc_fiche_souscription,
trim(A.acceptation_cgv) as acceptation_cgv,
trim(A.doc_attestation_cnps) as doc_attestation_cnps,
trim(A.doc_rccm) as doc_rccm,
trim(A.disponibilite_scan) as disponibilite_scan,
case when B.typeclient is null or trim(B.typeclient) = '' then A.type_client else B.typeclient end as type_client
FROM (SELECT * FROM MON.SPARK_FT_KYC_CRM_B2B WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1)
 and not(compte_client is null or trim(compte_client) = '') and upper(raison_sociale) not like '%LIGNES RENONCEES%') A
FULL OUTER JOIN
(SELECT * FROM (SELECT *,row_number() over(partition by GUID order by RAISON_SOCIALE desc nulls last) as rang 
FROM CDR.SPARK_IT_KYC_CRM_B2B WHERE ORIGINAL_FILE_DATE = DATE_ADD('###SLICE_VALUE###',1)
and not(compte_client is null or trim(compte_client) = '')) A
WHERE rang = 1 and upper(raison_sociale) not like '%LIGNES RENONCEES%') B
ON trim(A.GUID) = trim(B.GUID)))