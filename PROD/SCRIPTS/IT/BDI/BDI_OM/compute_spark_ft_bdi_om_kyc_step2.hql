INSERT INTO TMP.TT_BDI_OM_KYC_STEP_2
SELECT 
    (case when B.new_iban is not null or B.new_iban<>'' then B.new_iban else A.new_iban end) as new_iban,
    B.new_RIB ,
    B.new_IBU new_IBU,
    (case when A.FirstName is not null or A.FirstName<>'' then A.FirstName else B.user_first_name end) nom_naissance,
    A.new_NomMarital nom_marital,
    (case when A.LastName is not null or A.LastName<>'' then A.LastName else B.user_last_name end) prenom,
    B.sexe,
    (case when A.birthdate is not null or A.birthdate<>'' then A.birthdate else B.birth_date end) date_naissance,
    A.new_Paysdenaissance pays_naissance,
    (case when A.new_lieudenaissance is not null or A.new_lieudenaissance<>'' then A.new_lieudenaissance else B.city end) lieu_naissance,
    A.new_Nomsetprnomsdelamre,
    A.new_Nomsetprnomsdupre,
    (case when A.new_Natureducompte is not null or A.new_Natureducompte<>'' then A.new_Natureducompte else B.new_Natureducompte end) new_Natureducompte,
    (case when A.new_Statutducompte is not null or A.new_Statutducompte<>'' then A.new_Statutducompte else B.new_Statutducompte end) new_Statutducompte,
    B.new_Codedevise ,
    B.new_NumerodelIBUdumandataire numero_ibu_mandataire,
    B.new_Identifiantinternedumandataire id_interne_mandataire,
    B.new_Qualitdumandataire qualite_mandataire,
    B.new_Datedenaissance date_naissance_mandataire,
    B.new_Nom nom_mandataire,
    B.new_Prenom prenom_mandataire,
    B.new_Nationalite,
    (case when A.new_ProfilInterne is not null or A.new_ProfilInterne <>'' then A.new_ProfilInterne  else  B.new_ProfilInterne end) as profession_client,
    A.new_Tutelleounondelapersonne,
    (case when A.new_Statutduclient is not null or A.new_Statutduclient <>'' then A.new_Statutduclient  else  B.new_Statutduclient end) as new_Statutduclient ,
    A.new_Situationbancaire,
    (case when A.new_Situationjudiciaire is not null or A.new_Situationjudiciaire <>'' then A.new_Situationjudiciaire  else  B.new_Situationjudiciaire end) as new_Situationjudiciaire,
    (case when A.new_Datedudbutdinterdictionjudiciaire is not null or A.new_Datedudbutdinterdictionjudiciaire <>'' then A.new_Datedudbutdinterdictionjudiciaire  else  B.new_datedbutinterdictionjudiciaire end) as new_datedbutinterdictionjudiciaire,
    (case when A.new_Datedefindinterdictionjudiciaire is not null or A.new_Datedefindinterdictionjudiciaire <>'' then A.new_Datedefindinterdictionjudiciaire  else  B.new_datefininterdictionjudiciaire end) as new_datefininterdictionjudiciaire,
    A.new_Typepieceidentite,
    A.new_npiecedidentite piece_identite,
    A.new_identitydocumentissuedate,
    A.new_datedexpirationdelapiecedidentite,
    A.new_identitydocumentissueplace,
    A.new_Paysdmissiondelapicedidentification,
    A.new_Paysdersidence,
    (case when A.new_Codeagentconomique is not null or A.new_Codeagentconomique <>'' then A.new_Codeagentconomique  else  B.new_Codeagentconomique end) as new_Codeagentconomique,
    (case when A.new_Codesecteurdactivit is not null or A.new_Codesecteurdactivit <>'' then A.new_Codesecteurdactivit  else  B.new_Codesecteurdactivit end) as new_Codesecteurdactivit,
    (case when A.new_Notationinterne is not null or A.new_Notationinterne <>'' then A.new_Notationinterne  else  B.new_Notationinterne end) as new_Notationinterne,
    (case when A.new_PPE is not null or A.new_PPE <>'' then A.new_PPE  else  B.new_PPE end) as new_PPE,
    (case when A.new_RisqueAML is not null or A.new_RisqueAML <>'' then A.new_RisqueAML  else  B.new_RisqueAML end) as new_RisqueAML,
    (case when A.new_ProfilInterne is not null or A.new_ProfilInterne <>'' then A.new_ProfilInterne  else  B.new_ProfilInterne end) as new_ProfilInterne,
    B.guid,
    B.msisdn,
    (case when B.new_Typeducompte is not null or B.new_Typeducompte <>'' then B.new_Typeducompte  else  A.new_TypedecompteOM end) as new_Typeducompte,
    Z.acceptation_cgv,
    B.contrat_soucription,
    A.new_Datevalidation,
    A.new_Datecrationducompte,
    A.new_DisponibilitduScandelapicedidentification,
    B.identificateur,
    A.new_Motifrejetbackoffice,
    B.statut_validation_bo,
    A.new_Datedemisejour,
    B.imei,
    B.region_administrative,
    (case when A.address1_city is not null or A.address1_city<>'' then A.address1_city else  B.ville end) ville,
    (case when A.new_Natureduclienttitulaireducompte is not null or A.new_Natureduclienttitulaireducompte <>'' then A.new_Natureduclienttitulaireducompte  else  B.new_Natureduclienttitulaireducompte end) as new_Natureduclienttitulaireducompte ,
    (case when A.new_Numrodecompte is not null or A.new_Numrodecompte<>'' then A.new_Numrodecompte else B.new_Numrodecompte end) new_Numrodecompte,
    Z.conforme_art,
    Z.est_suspendu est_suspendu_telco,
    B.est_suspendu_om,
    B.event_date
FROM (SELECT * FROM TMP.TT_BDI_OM_KYC_STEP_1) B
LEFT JOIN 
(SELECT * FROM CDR.SPARK_IT_CRM_CONTACT_BASE WHERE original_file_date='###SLICE_VALUE###') A
ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.new_numrodecompte))
LEFT JOIN
(SELECT msisdn,est_suspendu,acceptation_cgv,conforme_art  FROM  MON.SPARK_FT_KYC_BDI_PP WHERE EVENT_DATE='###SLICE_VALUE###') Z
ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(Z.msisdn))
