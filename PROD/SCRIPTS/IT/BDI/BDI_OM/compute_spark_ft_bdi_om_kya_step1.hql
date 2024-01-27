
INSERT INTO TMP.TT_BDI_OM_KYA_STEP_1 
SELECT 
    B.new_iban ,
    B.new_RIB ,
    B.new_IBU new_IBU,
    L.msisdn,
    B.new_Natureducompte,
    C.new_mandatairepartenaireId,
    B.new_Datedecration,
    B.new_Dureedevie,
    B.new_raisonsociale,
    B.new_Sigledelaraisonsociale,
    B.new_Formejuridique,
    B.new_Secteurdactiviteconomique ,
    B.new_Numeroderegistredecommerce,
    B.new_Numerodidentificationfiscale,
    B.new_PaysdusigeSocial,
    B.new_Ville,
    B.new_Statutduclient ,
    B.new_Natureduclienttitulaireducompte ,
    B.new_Typeducompte, 
    B.new_Codedevise ,
    B.new_Statutducompte,
    C.new_Identifiantinternedumandataire,
    C.new_Numerodepicedumandataire,
    C.new_NumerodecompteOMdumandataire,
    C.new_NumerodelIBUdumandataire,
    B.new_Situationjudiciaire,
    B.new_datedbutinterdictionjudiciaire ,
    B.new_datefininterdictionjudiciaire ,
    D.guid,
    B.new_Codeagentconomique,
    B.new_Codesecteurdactivit,
    B.new_Notationinterne,
    B.new_PPE ,
    B.new_RisqueAML,
    B.new_Groupe,
    B.new_ProfilInterne ,
    C.new_Qualitdumandataire,
    B.new_Responsabilitducompte,
    C.new_DatedExpirationdelapiece,
    C.new_DelivrerA,
    C.new_Nom,
    C.new_Prenom,
    C.new_Nationalite,
    C.new_Datedenaissance,
    D.est_suspendu est_suspendu_telco,
    L.account_status est_suspendu_om,
    L.event_date
FROM (select * from (select distinct msisdn,account_status,event_date, row_number() over (partition by msisdn order by to_date(modified_on) desc nulls last) rang FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW WHERE event_date='###SLICE_VALUE###')P where rang=1) L
LEFT JOIN
(SELECT * FROM CDR.SPARK_IT_CRM_PARTENAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') B
ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.new_numrodecompte))
LEFT JOIN (SELECT * FROM CDR.SPARK_IT_CRM_MANDATAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') C
ON trim(B.new_partenaireId) = trim(C.new_Partenaire)
LEFT JOIN (SELECT msisdn,guid,(case when UPPER(trim(STATUT)) in (UPPER('Suspendu')) then 'OUI' else 'NON' end) as  EST_SUSPENDU  FROM  CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date=DATE_ADD('###SLICE_VALUE###',1)) D
ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(D.msisdn))