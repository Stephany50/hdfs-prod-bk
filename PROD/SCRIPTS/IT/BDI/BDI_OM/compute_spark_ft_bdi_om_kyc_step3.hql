INSERT INTO TMP.TT_BDI_OM_KYC_STEP_3
SELECT
    C.new_iban iban,
    C.new_RIB rib ,
    C.new_IBU ibu,
    C.nom_naissance nom_naiss,
    C.nom_marital nom_marital,
    C.prenom prenom,
    C.sexe sexe,
    C.date_naissance date_naissance,
    C.pays_naissance pays_naissance,
    C.lieu_naissance lieu_naissance,
    C.new_Nomsetprnomsdelamre nom_prenom_mere,
    C.new_Nomsetprnomsdupre nom_prenom_pere,
    C.new_Natureducompte nature_compte,
    C.new_Statutducompte statut_compte,
    C.new_Codedevise code_devise,
    C.numero_ibu_mandataire ibu_mandataire,
    C.id_interne_mandataire,
    C.qualite_mandataire,
    C.date_naissance_mandataire,
    C.nom_mandataire,
    C.prenom_mandataire,
    C.new_Nationalite nationalite,
    C.profession_client profession_client,
    (case when C.new_Tutelleounondelapersonne is not null or  C.new_Tutelleounondelapersonne <> '' then 'OUI' else 'END' end) nom_tutelle,
    C.new_Statutduclient statut_client,
    C.new_Situationbancaire situation_bancaire,
    C.new_Situationjudiciaire situation_judiciaire,
    C.new_datedbutinterdictionjudiciaire date_debut_interdiction_judiciaire ,
    C.new_datefininterdictionjudiciaire date_fin_interdiction_judiciaire ,
    C.new_typepieceidentite type_piece_identite,
    C.piece_identite piece_identite,
    C.new_identitydocumentissuedate date_emission_piece_identification,
    C.new_datedexpirationdelapiecedidentite date_fin_validite,
    C.new_identitydocumentissueplace lieu_emission_piece,
    C.new_Paysdmissiondelapicedidentification pays_emission_piece,
    C.new_Paysdersidence pays_residence,
    C.new_Codeagentconomique code_agent_economique,
    C.new_Codesecteurdactivit code_secteur_activite,
    C.new_Notationinterne notation_interne,
    C.new_PPE ppe,
    C.new_RisqueAML risque_AML,
    C.new_ProfilInterne profil_interne,
    C.guid ,
    C.msisdn,
    C.new_Typeducompte type_compte,
    C.acceptation_cgv acceptation_cgu,
    C.contrat_soucription,
    C.new_Datevalidation date_validation,
    C.new_Datecrationducompte date_creation_compte,
    C.new_DisponibilitduScandelapicedidentification disponibilite_scan,
    C.identificateur,
    C.new_Motifrejetbackoffice motif_rejet_bo,
    C.statut_validation_bo,
    C.new_Datedemisejour date_maj_om,
    C.imei,
    C.region_administrative,
    C.ville,
    C.new_Natureduclienttitulaireducompte nature_client_titulaire_compte,
    C.new_Numrodecompte numero_compte,
    (case when datediff(to_date('###SLICE_VALUE###'),F.DATE_DERNIERE_ACTIVITE_OM) <= 30 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_30J,
    (case when datediff(to_date('###SLICE_VALUE###'),E.DATE_DERNIERE_ACTIVITE_OM) <= 90 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_90J,
    M.est_client_telco,
    C.conforme_art est_conforme_art,
    C.est_suspendu_telco,
    C.est_suspendu_om,
    C.event_date
FROM (select * from TMP.TT_BDI_OM_KYC_STEP_2) C
    LEFT JOIN
    ( select IF(sender_msisdn is not null OR sender_msisdn<>'','OUI','NON')est_client_telco,sender_msisdn FROM (SELECT * FROM cdr.spark_it_omny_transactions WHERE transfer_datetime='###SLICE_VALUE###')  H
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
    