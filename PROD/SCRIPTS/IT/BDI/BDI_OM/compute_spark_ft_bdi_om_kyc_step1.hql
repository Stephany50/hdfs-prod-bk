INSERT INTO TMP.TT_BDI_OM_KYC_STEP_1
SELECT 
    B.new_iban,
    B.new_RIB ,
    B.new_IBU new_IBU,
    L.user_first_name,
    L.user_last_name ,
    L.sex sexe,
    L.birth_date,
    L.city ,
    B.new_Natureducompte,
    B.new_Statutducompte,
    B.new_Codedevise ,
    C.new_NumerodelIBUdumandataire,
    C.new_Identifiantinternedumandataire,
    C.new_Qualitdumandataire,
    C.new_Datedenaissance,
    C.new_Nom,
    C.new_Prenom,
    C.new_Nationalite,
    B.new_Statutduclient,
    B.new_Situationjudiciaire,
    B.new_datedbutinterdictionjudiciaire,
    B.new_datefininterdictionjudiciaire,
    C.new_typepieceidentite,
    B.new_Codeagentconomique,
    B.new_Codesecteurdactivit,
    B.new_Notationinterne,
    B.new_PPE ,
    B.new_RisqueAML,
    B.new_ProfilInterne,
    D.guid,
    L.msisdn,
    B.new_Typeducompte,
    D.contrat_soucription,
    D.identificateur,
    D.statut_validation_bo,
    D.imei,
    D.region_administrative,
    D.ville ,
    B.new_Natureduclienttitulaireducompte,
    B.new_Numrodecompte,
    L.account_status est_suspendu_om,
    L.event_date
FROM (select * from (select distinct msisdn,user_first_name,user_last_name,birth_date,sex,city,account_status,event_date, row_number() over (partition by msisdn order by to_date(modified_on) desc nulls last) rang FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW WHERE event_date='###SLICE_VALUE###')P where rang=1) L
LEFT JOIN
(SELECT * FROM CDR.SPARK_IT_CRM_PARTENAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') B
ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.new_numrodecompte))
LEFT JOIN 
(SELECT * FROM CDR.SPARK_IT_CRM_MANDATAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') C
ON trim(B.new_partenaireId) = trim(C.new_Partenaire)
LEFT JOIN 
(SELECT msisdn,guid,contrat_soucription,identificateur,statut_validation_bo,ville,region_administrative,imei FROM  CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date=DATE_ADD('###SLICE_VALUE###',1)) D
ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(D.msisdn))