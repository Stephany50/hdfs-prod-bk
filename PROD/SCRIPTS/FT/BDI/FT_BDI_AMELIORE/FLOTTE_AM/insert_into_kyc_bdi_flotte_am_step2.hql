--- Calcul de la conformité des M2MG. Tout les lignes en anomalies deviennent des M2M Génériques (M2MG)
insert into TMP.TT_KYC_BDI_FLOTTE_AM_ST2
select
A.CUST_GUID,
A.nom_structure,
A.numero_registre_commerce,
A.numero_piece_representant_legal,
A.date_souscription,
A.adresse_structure,
A.msisdn,
A.nom_prenom,
A.numero_piece,
A.imei,
A.adresse,
A.statut,
A.disponibilite_scan,
A.acceptation_cgv,
A.customer_id,
A.contract_id,
A.compte_client,
A.type_personne,
A.type_piece,
A.id_type_piece,
A.nom,
A.prenom,
A.date_naissance,
A.date_expiration,
A.ville,
A.quartier,
A.raison_statut,
A.odbincomingcalls,
A.odboutgoingcalls,
A.date_changement_statut,
A.plan_localisation,
A.contrat_soucription,
A.type_piece_tuteur,
A.numero_piece_tuteur,
A.nom_tuteur,
A.prenom_tuteur,
A.date_naissance_tuteur,
A.date_expiration_tuteur,
A.adresse_tuteur,
A.compte_client_structure,
A.statut_derogation,
A.region_administrative,
A.region_commerciale,
A.site_name,
A.ville_site,
A.offre_commerciale,
A.type_contrat,
A.segmentation,
A.derogation_identification,
'' nom_representant_legal,
'' prenom_representant_legal,
'' contact_telephonique,
'DOUALA' ville_structure,
'AKWA' quartier_structure,
'' sms_contact,
'' doc_plan_localisation,
'' doc_fiche_souscription,
'' doc_attestation_cnps,
'EMPTY' doc_rccm,
'PME' type_client,
'M2MG' type_personne_morale,
(case when trim(A.nom_structure) = '' or A.nom_structure is null or
(trim(translate(trim(A.nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(A.nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(A.nom_structure)) < 2 then 'OUI' else 'NON'
end) as nom_structure_an,
(case
when trim(A.NUMERO_REGISTRE_COMMERCE) = '' or A.NUMERO_REGISTRE_COMMERCE is null or
(trim(translate(trim(A.NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(A.NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(A.NUMERO_REGISTRE_COMMERCE)) < 2
then 'OUI' else 'NON'
end) as  RCCM_an,
(case
when trim(A.numero_piece_representant_legal) = '' or A.numero_piece_representant_legal is NULL or
trim(A.numero_piece_representant_legal) rlike '^(\\d)\\1+$' or
length(trim(A.numero_piece_representant_legal)) > 21 or
trim(A.numero_piece_representant_legal) like '112233445%' or
(trim(translate(lower(trim(A.numero_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(A.numero_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(A.numero_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(A.numero_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '')
then 'OUI' else 'NON'
end) as NUM_PIECE_RPSTANT_AN,
(case
when A.DATE_SOUSCRIPTION is NULL
then 'OUI' else 'NON'
end) as DATE_SOUSCRIPTION_AN,
(case
when trim(A.ADRESSE_STRUCTURE) = '' or A.ADRESSE_STRUCTURE is NULL or
(trim(translate(trim(A.ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(A.ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(A.ADRESSE_STRUCTURE)) < 2
then 'OUI' else 'NON'
end) as ADRESSE_STRUCTURE_AN,
(case
when trim(A.msisdn) = '' or A.msisdn is null or trim(A.msisdn) not rlike '^\\d+$'
then 'OUI' else 'NON'
end) as NUM_TEL_AN,
(case
when trim(A.NOM_PRENOM) = '' or A.NOM_PRENOM is NULL or
(trim(translate(trim(A.NOM_PRENOM),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(A.NOM_PRENOM),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' '))  = '') or
length(trim(A.NOM_PRENOM)) = 1
then 'OUI' else 'NON'
end) as NOM_PRENOM_AN,
(case
when (trim(A.NUMERO_PIECE) = '' or A.NUMERO_PIECE is NULL or
trim(A.NUMERO_PIECE) rlike '^(\\d)\\1+$' or
length(trim(A.NUMERO_PIECE)) > 21 or
trim(A.NUMERO_PIECE) like '112233445%' or
(trim(translate(lower(trim(A.NUMERO_PIECE)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(A.NUMERO_PIECE)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(A.NUMERO_PIECE)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(A.NUMERO_PIECE)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '')
) and upper(trim(A.NUMERO_PIECE)) not in ('NON ASSUJETTI')
then 'OUI' else 'NON'
end) as NUMERO_PIECE_AN,
(case
when trim(A.IMEI) = '' or IMEI is NULL or
trim(A.IMEI) not rlike '^\\d{14,16}$'
then 'OUI' else 'NON'
end) as IMEI_AN,
(case
when trim(A.ADRESSE) = '' or A.ADRESSE  is NULL or
(trim(translate(trim(A.ADRESSE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(A.ADRESSE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(A.ADRESSE)) < 2
then 'OUI' else 'NON'
end) as ADRESSE_AN,
(case
when not(trim(A.STATUT) = '' or A.STATUT is NULL) and upper(trim(A.STATUT)) in ('ACTIF','SUSPENDU_ENTRANT','SUSPENDU_SORTANT','SUSPENDU')
then 'NON' else 'OUI'
end) as STATUT_AN
from TMP.TT_KYC_BDI_FLOTTE_AM_ST1 A