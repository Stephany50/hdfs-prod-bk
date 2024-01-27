
INSERT INTO TMP.TT_BDI_OM_KYA_STEP_3
SELECT 
    C.new_npiecedidentite piece_identite,
    C.new_iban iban,
    C.new_RIB rib,
    C.new_IBU ibu,
    C.msisdn numero_compte,
    C.new_Natureducompte nature_compte,
    new_mandatairepartenaireId id_complete_mandataire,
    C.new_Datedecration  date_creation,
    C.new_Dureedevie duree_de_vie,
    C.new_raisonsociale raison_sociale,
    C.new_Sigledelaraisonsociale sigle_raison_sociale,
    C.new_Formejuridique forme_juridique,
    C.new_Secteurdactiviteconomique secteur_activite_eco,
    C.new_Numeroderegistredecommerce  numero_regis_commerce,
    C.new_Numerodidentificationfiscale  numero_identification_fiscale,
    C.new_PaysdusigeSocial  pays_siege_social,
    C.new_Ville  ville_siege_social,
    C.new_Statutduclient  statut_client,
    C.new_Natureduclienttitulaireducompte nature_client_titulaire_compte,
    C.new_Typeducompte type_compte,
    C.new_Codedevise code_devise,
    C.new_Statutducompte statut_compte,
    C.new_Identifiantinternedumandataire  id_interne_mandataire,
    C.new_Numerodepicedumandataire numero_piece_mandataire,
    C.new_NumerodecompteOMdumandataire numero_compte_om_mandataire,
    C.new_NumerodelIBUdumandataire numero_ibu_mandataire,
    C.new_Situationjudiciaire situation_judiciaire,
    C.new_datedbutinterdictionjudiciaire date_debut_interdiction_judiciaire ,
    C.new_datefininterdictionjudiciaire date_fin_interdiction_judiciaire ,
    C.new_serviceorangemoney service_souscrit,
    C.guid guid,
    C.new_Paysdersidence pays_residence,
    C.new_Codeagentconomique code_agent_economique,
    C.new_Codesecteurdactivit code_secteur_activite,
    C.new_Notationinterne notation_interne,
    C.new_PPE ppe,
    C.new_RisqueAML risque_AML,
    C.new_Groupe  groupe,
    C.new_ProfilInterne profil_interne,
    C.new_Qualitdumandataire qualite_mandataire,
    C.new_Responsabilitducompte responsabilite_compte,
    C.new_DatedExpirationdelapiece date_expiration_piece_mandataire,
    C.new_DelivrerA lieu_emission_piece_mandataire,
    C.new_Nom nom_mandataire,
    C.new_Prenom prenom_mandataire,
    C.new_Nationalite nationalite_mandataire,
    C.new_Datedenaissance date_naissance_mandataire,
    (case when datediff(to_date('###SLICE_VALUE###'),F.DATE_DERNIERE_ACTIVITE_OM) <= 30 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_30J,
    (case when datediff(to_date('###SLICE_VALUE###'),E.DATE_DERNIERE_ACTIVITE_OM) <= 90 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_90J,
    M.est_client_telco,
    C.est_suspendu_telco est_suspendu_telco,
    C.est_suspendu_om est_suspendu_om,
    C.event_date
FROM (select * from TMP.TT_BDI_OM_KYA_STEP_2) C
    LEFT JOIN
    (select IF(sender_msisdn is not null OR sender_msisdn<>'','OUI','NON')est_client_telco,sender_msisdn FROM (SELECT * FROM cdr.spark_it_omny_transactions WHERE transfer_datetime='###SLICE_VALUE###')  H
    INNER join 
    (select * from DIM.SPARK_DT_REF_OPERATEURS  where country_name like '%CAMEROON%' and ncc not in ("3342","3343","655","656","6570","6571","6573","6574","69","8","22945","222945","22258","6","6572","222258","62","233","243","242") ) K
    on substr(H.sender_msisdn,1,length(trim(K.ncc))) = trim(K.ncc)) M
    ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(C.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(M.sender_msisdn))
    LEFT JOIN
    (SELECT MSISDN,MAX(DATE_DERNIERE_ACTIVITE_OM) DATE_DERNIERE_ACTIVITE_OM FROM MON.SPARK_FT_DATAMART_OM_MONTH 
    WHERE MOIS IN (substr(add_months(to_date('###SLICE_VALUE###'),-3),1,7), substr(add_months(to_date('###SLICE_VALUE###'),-2),1,7), substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7))
    GROUP BY MSISDN
    ) E 
    ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(C.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(E.MSISDN))
    LEFT JOIN
    (SELECT MSISDN,MAX(DATE_DERNIERE_ACTIVITE_OM) DATE_DERNIERE_ACTIVITE_OM FROM MON.SPARK_FT_DATAMART_OM_MONTH 
    WHERE MOIS = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)
    GROUP BY MSISDN
    ) F 
    ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(C.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(F.MSISDN))