insert into MON.SPARK_FT_B2B_ANOMALIES
(select type_personne,nom_structure,numero_registre_commerce,num_piece_representant_legal,date_souscription,                  
adresse_structure,msisdn,nom_prenom,numero_piece,imei,adresse,statut,compte_client,
type_personne_morale,type_piece,nom,prenom,date_naissance,date_expiration,ville,                                 
quartier,ville_structure,quartier_structure,compte_client_structure,
CONCAT(
    if(nom_structure_an='OUI','nom structure en anomalie.\n',''),
    if(rccm_an='OUI','rccm en anomalie.\n',''),
    if(num_piece_rpstant_an='OUI','piece rept legal en anomalie.\n',''),
    if(date_souscription_an='OUI','date souscription en anomalie.\n',''),
    if(adresse_structure_an='OUI','adresse structure en anomalie.\n',''),
    if(num_tel_an='OUI','numéro en anomalie.\n',''),
    if(nom_prenom_an='OUI','nom prenom en anomalie.\n',''),
    if(numero_piece_an='OUI','num piece en anomalie.\n',''),
    if(imei_an='OUI','imei en anomalie.\n',''),
    if(adresse_an='OUI','adresse en anomalie.\n',''),
    if(statut_an='OUI','statut en anomalie.\n','')
)motif_anomalie,current_timestamp() as insert_date,'###SLICE_VALUE###' as event_date
FROM (select A.* FROM
    (SELECT * FROM MON.SPARK_FT_KYC_BDI_FLOTTE A WHERE EVENT_DATE = '###SLICE_VALUE###') A
LEFT JOIN (SELECT msisdn FROM MON.SPARK_FT_KYC_BDI_FLOTTE A WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) B ON A.msisdn = B.msisdn
WHERE B.msisdn is null and trim(A.est_conforme) = 'NON' and trim(A.statut) in ('SUSPENDU_SORTANT','SUSPENDU_ENTRANT','ACTIF')) D)
UNION
(select 'ENT' type_personne,raison_sociale nom_structure,numero_registre_commerce,
cni_representant_local num_piece_representant_legal,current_timestamp() date_souscription,                  
adresse_structure,contact_telephonique msisdn,'' nom_prenom,'' numero_piece,'' imei,'' adresse,'' statut,compte_client,
'' type_personne_morale,'' type_piece,nom_representant_legal nom,prenom_representant_legal prenom,current_timestamp() date_naissance,current_timestamp() date_expiration,'' ville,
'' quartier,ville_structure,quartier_structure,compte_client compte_client_structure,
CONCAT(
    if(raison_sociale_an='OUI','nom structure en anomalie.\n',''),
    if(rccm_an='OUI','rccm en anomalie.\n',''),
    if(cni_representant_legal_an='OUI','piece rept legal en anomalie.\n',''),
    if(adresse_structure_an='OUI','adresse structure en anomalie.\n','')
)motif_anomalie,current_timestamp() as insert_date,'###SLICE_VALUE###' as event_date
FROM (select A.* FROM (SELECT * FROM  TMP.TT_KYC_FT_A_PERS_MO_DETAIL_ST1) A
LEFT JOIN (SELECT guid FROM TMP.TT_KYC_FT_A_PERS_MO_DETAIL_ST2) B ON A.guid = B.guid
WHERE B.guid is null and A.est_conforme='NON') C)
