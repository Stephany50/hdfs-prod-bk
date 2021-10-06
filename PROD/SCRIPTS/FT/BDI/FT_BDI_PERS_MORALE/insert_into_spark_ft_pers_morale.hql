INSERT INTO MON.SPARK_FT_BDI_PERS_MORALE
SELECT
COMPTE_CLIENT,
RAISON_SOCIALE,
NOM_REPRESENTANT_LEGAL,
PRENOM_REPRESENTANT_LEGAL,
CNI_REPRESENTANT_LOCAL,
CONTACT_TELEPHONIQUE,
VILLE_STRUCTURE,
QUARTIER_STRUCTURE,
ADRESSE_STRUCTURE,
NUMERO_REGISTRE_COMMERCE,
SMS_CONTACT,
DOC_PLAN_LOCALISATION,
DOC_FICHE_SOUSCRIPTION,
ACCEPTATION_CGV,
DOC_ATTESTATION_CNPS,
DOC_RCCM,
DISPONIBILITE_SCAN,
type_client,
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
end) ADRESSE_STRUCTURE_AN,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
FROM (SELECT *,row_number() over(partition by COMPTE_CLIENT order by RAISON_SOCIALE desc nulls last) as rang 
FROM CDR.SPARK_IT_BDI_PERS_MORALE WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###') A
WHERE rang = 1