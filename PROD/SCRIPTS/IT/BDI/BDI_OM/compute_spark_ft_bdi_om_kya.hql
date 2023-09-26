INSERT INTO MON.SPARK_FT_BDI_OM_KYA
SELECT
new_iban iban,
new_RIB rib,
msisdn ,
new_Datedecration  date_creaction,
new_raisonsociale  raison_sociale,
new_Sigledelaraisonsociale sigle_raison_sociale,
new_Formejuridique  forme_juridique,
new_Secteurdactivitconomique secteur_activite_eco,
new_Numeroderegistredecommerce  numero_regis_commerce,
new_Numerodidentificationfiscale  numero_identification_fiscale,
new_PaysdusigeSocial  pays_siege_social,
new_Ville  ville_siege_social,
new_Statutduclient  statut_client,
new_Natureduclienttitulaireducompte nature_client_titulaire_compte,
new_Typeducompte type_compte,
new_Codedevise code_devise,
new_Statutducompte statut_compte,
new_Identifiantinternedumandataire  identification_mandataire,
new_Numerodepicedumandataire numero_piece_mandataire,
new_NumerodecompteOMdumandataire numero_compte_om_mandataire,
new_NumerodelIBUdumandataire numero_ibu_mandataire,
new_serviceorangemoney service_souscrit,
guid guid,
new_Paysdersidence pays_residence,
new_Codeagentconomique code_agent_economique,
new_Codesecteurdactivit code_secteur_activite,
new_Notationinterne rrc,
new_PPE ppe,
new_RisqueAML risque_AML,
new_Groupe  groupe,
new_ProfilInterne pro,
new_Qualitdumandataire qualite_mandataire,
new_DatedExpirationdelapiece date_expiration_piece_mandataire,
new_DelivrerA lieu_emission_piece_mandataire,
new_Nom nom_mandataire,
new_Prenom prenom_mandataire,
est_actif_30j est_actif_30,
est_actif_90j est_actif_90,
est_client_telco est_client_telco,
est_suspendu_telco est_suspendu_telco,
est_suspendu_om est_suspendu_om,
event_date
FROM
(SELECT 
(case when B.new_iban is not null or B.new_iban<>'' then B.new_iban else A.new_iban end) as new_iban,
(case when B.new_RIB is not null or B.new_RIB<>'' then B.new_RIB else A.new_RIB end) as new_RIB,
L.msisdn,
B.new_Datedecration,
B.new_raisonsociale,
B.new_Sigledelaraisonsociale,
B.new_Formejuridique,
(case when B.new_Secteurdactiviteconomique is not null or B.new_Secteurdactiviteconomique <>'' then B.new_Secteurdactiviteconomique  else  A.new_Secteurdactivitconomique end) as new_Secteurdactivitconomique,
B.new_Numeroderegistredecommerce,
B.new_Numerodidentificationfiscale,
B.new_PaysdusigeSocial,
B.new_Ville,
(case when B.new_Statutduclient is not null or B.new_Statutduclient <>'' then B.new_Statutduclient  else  A.new_Statutduclient end) as new_Statutduclient ,
(case when B.new_Natureduclienttitulaireducompte is not null or B.new_Natureduclienttitulaireducompte <>'' then B.new_Natureduclienttitulaireducompte  else  A.new_Natureduclienttitulaireducompte end) as new_Natureduclienttitulaireducompte ,
(case when B.new_Typeducompte is not null or B.new_Typeducompte <>'' then B.new_Typeducompte  else  A.new_TypedecompteOM end) as new_Typeducompte,
(case when B.new_Codedevise is not null or B.new_Codedevise <>'' then B.new_Codedevise  else  A.new_Codedevises end) as new_Codedevise,
(case when B.new_Statutducompte is not null or B.new_Statutducompte <>'' then B.new_Statutducompte  else  A.new_Statutducompte end) as new_Statutducompte,
C.new_Identifiantinternedumandataire,
C.new_Numerodepicedumandataire,
C.new_NumerodecompteOMdumandataire,
C.new_NumerodelIBUdumandataire,
A.new_serviceorangemoney,
D.guid,
A.new_Paysdersidence,
(case when B.new_Codeagentconomique is not null or B.new_Codeagentconomique <>'' then B.new_Codeagentconomique  else  A.new_Codeagentconomique end) as new_Codeagentconomique,
(case when B.new_Codesecteurdactivit is not null or B.new_Codesecteurdactivit <>'' then B.new_Codesecteurdactivit  else  A.new_Codesecteurdactivit end) as new_Codesecteurdactivit,
(case when B.new_Notationinterne is not null or B.new_Notationinterne <>'' then B.new_Notationinterne  else  A.new_Notationinterne end) as new_Notationinterne,
(case when B.new_PPE is not null or B.new_PPE <>'' then B.new_PPE  else  A.new_PPE end) as new_PPE,
(case when B.new_RisqueAML is not null or B.new_RisqueAML <>'' then B.new_RisqueAML  else  A.new_RisqueAML end) as new_RisqueAML,
B.new_Groupe,
(case when B.new_ProfilInterne is not null or B.new_ProfilInterne <>'' then B.new_ProfilInterne  else  A.new_ProfilInterne end) as new_ProfilInterne,
C.new_Qualitdumandataire,
C.new_DatedExpirationdelapiece,
C.new_DelivrerA,
C.new_Nom,
C.new_Prenom,
(case when datediff(to_date('2023-08-29'),F.DATE_DERNIERE_ACTIVITE_OM) <= 30 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_30J,
(case when datediff(to_date('2023-08-29'),E.DATE_DERNIERE_ACTIVITE_OM) <= 90 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_90J,
M.est_client_telco,
Z.est_suspendu est_suspendu_telco,
L.account_status est_suspendu_om,
L.event_date
FROM (SELECT * FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW WHERE event_date='2023-08-29') L
LEFT JOIN
(SELECT * FROM CDR.SPARK_IT_CRM_PARTENAIRE_BASE WHERE original_file_date='2023-08-29') B
ON trim(L.msisdn)=trim(B.new_numrodecompte)
LEFT JOIN (SELECT * FROM CDR.SPARK_IT_CRM_MANDATAIRE_BASE WHERE original_file_date='2023-08-29') C
ON trim(B.new_partenaireId) = trim(C.new_Partenaire)
LEFT JOIN (SELECT msisdn,guid  FROM  CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date=DATE_ADD('2023-08-29',1)) D
ON trim(L.msisdn)=trim(D.msisdn)
LEFT JOIN (SELECT * FROM CDR.SPARK_IT_CRM_CONTACT_BASE WHERE original_file_date='2023-08-29') A
ON trim(L.msisdn) = trim(A.new_numrodecompte)
LEFT JOIN (SELECT msisdn,est_suspendu  FROM  MON.SPARK_FT_KYC_BDI_PP WHERE EVENT_DATE='2023-08-29') Z
ON trim(L.msisdn)=trim(Z.msisdn)
LEFT JOIN
(select IF(sender_msisdn is not null OR sender_msisdn<>'','OUI','NON')est_client_telco,sender_msisdn FROM (SELECT * FROM cdr.spark_it_omny_transactions WHERE transfer_datetime='2023-08-29')  H
INNER join 
(select * from DIM.SPARK_DT_REF_OPERATEURS  where country_name like '%CAMEROON%' and ncc not in ("3342","3343","655","656","6570","6571","6573","6574","69","8","22945","222945","22258","6","6572","222258","62","233","243","242") ) K
on substr(H.sender_msisdn,1,length(trim(K.ncc))) = trim(K.ncc)) M
ON trim(L.msisdn) = trim(M.sender_msisdn)
LEFT JOIN
(SELECT MSISDN,MAX(DATE_DERNIERE_ACTIVITE_OM) DATE_DERNIERE_ACTIVITE_OM FROM MON.SPARK_FT_DATAMART_OM_MONTH 
WHERE MOIS IN (substr(add_months(to_date('2023-08-29'),-3),1,7), substr(add_months(to_date('2023-08-29'),-2),1,7), substr(add_months(to_date('2023-08-29'),-1),1,7))
GROUP BY MSISDN
) E 
ON trim(L.msisdn)=trim(E.MSISDN)
LEFT JOIN
(SELECT MSISDN,MAX(DATE_DERNIERE_ACTIVITE_OM) DATE_DERNIERE_ACTIVITE_OM FROM MON.SPARK_FT_DATAMART_OM_MONTH 
WHERE MOIS = substr(add_months(to_date('2023-08-29'),-1),1,7)
GROUP BY MSISDN
) F 
ON trim(L.msisdn)=trim(F.MSISDN)
)RESULT
