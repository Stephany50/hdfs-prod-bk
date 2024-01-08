INSERT INTO TMP.TT_BDI_OM_KYC_1
SELECT 
    case when B.iban is null or trim(B.iban) ='' then A.iban else B.iban end as iban,
    case when B.nom_naiss is null or trim(B.nom_naiss) ='' then A.nom_naiss else B.nom_naiss end as nom_naiss,
    case when B.nom_marital is null or trim(B.nom_marital) ='' then A.nom_marital else B.nom_marital end as nom_marital,
    case when B.prenom is null or trim(B.prenom) ='' then A.prenom else B.prenom end as prenom,
    case when B.sexe is null or trim(B.sexe) ='' then A.sexe else B.sexe end as sexe,
    case when B.date_naissance is null or trim(B.date_naissance) ='' then A.date_naissance else B.date_naissance end as date_naissance,
    case when B.pays_naissance is null or trim(B.pays_naissance) ='' then A.pays_naissance else B.pays_naissance end as pays_naissance,
    case when B.lieu_naissance is null or trim(B.lieu_naissance) ='' then A.lieu_naissance else B.lieu_naissance end as lieu_naissance,
    case when B.nom_prenom_mere is null or trim(B.nom_prenom_mere) ='' then A.nom_prenom_mere else B.nom_prenom_mere end as nom_prenom_mere,
    case when B.nom_prenom_pere is null or trim(B.nom_prenom_pere) ='' then A.nom_prenom_pere else B.nom_prenom_pere end as nom_prenom_pere,
    case when B.nationalite is null or trim(B.nationalite) ='' then A.nationalite else B.nationalite end as nationalite,
    case when B.profession_client is null or trim(B.profession_client) ='' then A.profession_client else B.profession_client end as profession_client,
    case when B.nom_tutelle is null or trim(B.nom_tutelle) ='' then A.nom_tutelle else B.nom_tutelle end as nom_tutelle,
    case when B.statut_client is null or trim(B.statut_client) ='' then A.statut_client else B.statut_client end as statut_client,
    case when B.situation_bancaire is null or trim(B.situation_bancaire) ='' then A.situation_bancaire else B.situation_bancaire end as situation_bancaire,
    case when B.situation_judiciaire is null or trim(B.situation_judiciaire) ='' then A.situation_judiciaire else B.situation_judiciaire end as situation_judiciaire,
    case when B.date_debut_interdiction_judiciaire is null or trim(B.date_debut_interdiction_judiciaire) ='' then A.date_debut_interdiction_judiciaire else B.date_debut_interdiction_judiciaire end as date_debut_interdiction_judiciaire,
    case when B.date_fin_interdiction_judiciaire is null or trim(B.date_fin_interdiction_judiciaire) ='' then A.date_fin_interdiction_judiciaire else B.date_fin_interdiction_judiciaire end as date_fin_interdiction_judiciaire,
    case when B.type_piece_identite is null or trim(B.type_piece_identite) ='' then A.type_piece_identite else B.type_piece_identite end as type_piece_identite,
    case when B.piece_identite is null or trim(B.piece_identite) ='' then A.piece_identite else B.piece_identite end as piece_identite,
    case when B.date_emission_piece_identification is null or trim(B.date_emission_piece_identification) ='' then A.date_emission_piece_identification else B.date_emission_piece_identification end as date_emission_piece_identification,
    case when B.date_fin_validite is null or trim(B.date_fin_validite) ='' then A.date_fin_validite else B.date_fin_validite end as date_fin_validite,
    case when B.lieu_emission_piece is null or trim(B.lieu_emission_piece) ='' then A.lieu_emission_piece else B.lieu_emission_piece end as lieu_emission_piece,
    case when B.pays_emission_piece is null or trim(B.pays_emission_piece) ='' then A.pays_emission_piece else B.pays_emission_piece end as pays_emission_piece,
    case when B.pays_residence is null or trim(B.pays_residence) ='' then A.pays_residence else B.pays_residence end as pays_residence,
    case when B.code_agent_economique is null or trim(B.code_agent_economique) ='' then A.code_agent_economique else B.code_agent_economique end as code_agent_economique,
    case when B.code_secteur_activite is null or trim(B.code_secteur_activite) ='' then A.code_secteur_activite else B.code_secteur_activite end as code_secteur_activite,
    case when B.rrc is null or trim(B.rrc) ='' then A.rrc else B.rrc end as rrc,
    case when B.ppe is null or trim(B.ppe) ='' then A.ppe else B.ppe end as ppe,
    case when B.risque_AML is null or trim(B.risque_AML) ='' then A.risque_AML else B.risque_AML end as risque_AML,
    case when B.pro is null or trim(B.pro) ='' then A.pro else B.pro end as pro,
    case when B.guid is null or trim(B.guid) ='' then A.guid else B.guid end as guid,
    case when B.msisdn is null or trim(B.msisdn) ='' then A.msisdn else B.msisdn end as msisdn,
    case when B.type_compte is null or trim(B.type_compte) ='' then A.type_compte else B.type_compte end as type_compte,
    case when B.acceptation_cgu is null or trim(B.acceptation_cgu) ='' then A.acceptation_cgu else B.acceptation_cgu end as acceptation_cgu,
    case when B.contrat_soucription is null or trim(B.contrat_soucription) ='' then A.contrat_soucription else B.contrat_soucription end as contrat_soucription,
    case when B.date_validation is null or trim(B.date_validation) ='' then A.date_validation else B.date_validation end as date_validation,
    case when B.date_creaction_compte is null or trim(B.date_creaction_compte) ='' then A.date_creaction_compte else B.date_creaction_compte end as date_creaction_compte,
    case when B.disponibilite_scan is null or trim(B.disponibilite_scan) ='' then A.disponibilite_scan else B.disponibilite_scan end as disponibilite_scan,
    case when B.identificateur is null or trim(B.identificateur) ='' then A.identificateur else B.identificateur end as identificateur,
    case when B.motif_rejet_bo is null or trim(B.motif_rejet_bo) ='' then A.motif_rejet_bo else B.motif_rejet_bo end as motif_rejet_bo,
    case when B.statut_validation_bo is null or trim(B.statut_validation_bo) ='' then A.statut_validation_bo else B.statut_validation_bo end as statut_validation_bo,
    case when B.date_maj_om is null or trim(B.date_maj_om) ='' then A.date_maj_om else B.date_maj_om end as date_maj_om,
    case when B.imei is null or trim(B.imei) ='' then A.imei else B.imei end as imei,
    case when B.region_administrative is null or trim(B.region_administrative) ='' then A.region_administrative else B.region_administrative end as region_administrative,
    case when B.ville is null or trim(B.ville) ='' then A.ville else B.ville end as ville,
    case when B.nature_client_titulaire_compte is null or trim(B.nature_client_titulaire_compte) = '' then A.nature_client_titulaire_compte else B.nature_client_titulaire_compte end as nature_client_titulaire_compte,
    case when B.numero_compte is null or trim(B.numero_compte) ='' then A.numero_compte else B.numero_compte end as numero_compte,
    case when B.EST_ACTIF_30J is null or trim(B.EST_ACTIF_30J) ='' then A.EST_ACTIF_30J else B.EST_ACTIF_30J end as EST_ACTIF_30J,
    case when B.EST_ACTIF_90J is null or trim(B.EST_ACTIF_90J) ='' then A.EST_ACTIF_90J else B.EST_ACTIF_90J end as EST_ACTIF_90J,
    case when B.est_client_telco is null or trim(B.est_client_telco) ='' then A.est_client_telco else B.est_client_telco end as est_client_telco,
    case when B.est_conforme_art is null or trim(B.est_conforme_art) ='' then A.est_conforme_art else B.est_conforme_art end as est_conforme_art,
    case when B.est_suspendu_telco is null or trim(B.est_suspendu_telco) ='' then A.est_suspendu_telco else B.est_suspendu_telco end as est_suspendu_telco,
    case when B.est_suspendu_om is null or trim(B.est_suspendu_om) ='' then A.est_suspendu_om else B.est_suspendu_om end as est_suspendu_om,
    B.event_date
FROM (SELECT * FROM MON.SPARK_FT_BDI_OM_KYC WHERE EVENT_DATE=date_sub('###SLICE_VALUE###',1) ) A
    FULL OUTER JOIN
    (SELECT
        new_iban iban,
        nom_naissance nom_naiss,
        nom_marital nom_marital,
        prenom prenom,
        sexe sexe,
        date_naissance date_naissance,
        pays_naissance pays_naissance,
        lieu_naissance lieu_naissance,
        new_Nomsetprnomsdelamre nom_prenom_mere,
        new_Nomsetprnomsdupre nom_prenom_pere,
        new_Nationalite nationalite,
        profession_client profession_client,
        new_Tutelleounondelapersonne nom_tutelle,
        new_Statutduclient statut_client,
        new_Situationbancaire situation_bancaire,
        new_Situationjudiciaire situation_judiciaire,
        new_datedbutinterdictionjudiciaire date_debut_interdiction_judiciaire ,
        new_datefininterdictionjudiciaire date_fin_interdiction_judiciaire ,
        new_typepieceidentite type_piece_identite,
        piece_identite piece_identite,
        new_identitydocumentissuedate date_emission_piece_identification,
        new_datedexpirationdelapiecedidentite date_fin_validite,
        new_identitydocumentissueplace lieu_emission_piece,
        new_Paysdmissiondelapicedidentification pays_emission_piece,
        new_Paysdersidence pays_residence,
        new_Codeagentconomique code_agent_economique,
        new_Codesecteurdactivit code_secteur_activite,
        new_Notationinterne rrc,
        new_PPE ppe,
        new_RisqueAML risque_AML,
        new_ProfilInterne pro,
        guid ,
        msisdn,
        new_Typeducompte type_compte,
        acceptation_cgv acceptation_cgu,
        contrat_soucription,
        new_Datevalidation date_validation,
        new_Datecrationducompte date_creaction_compte,
        new_DisponibilitduScandelapicedidentification disponibilite_scan,
        identificateur,
        new_Motifrejetbackoffice motif_rejet_bo,
        statut_validation_bo,
        new_Datedemisejour date_maj_om,
        imei,
        region_administrative,
        ville,
        new_Natureduclienttitulaireducompte nature_client_titulaire_compte,
        new_Numrodecompte numero_compte,
        EST_ACTIF_30J,
        EST_ACTIF_90J,
        est_client_telco,
        conforme_art est_conforme_art,
        est_suspendu_telco,
        est_suspendu_om,
        event_date
    FROM
        (SELECT 
            (case when B.new_iban is not null or B.new_iban<>'' then B.new_iban else A.new_iban end) as new_iban,
            (case when A.FirstName is not null or A.FirstName<>'' then A.FirstName else L.user_first_name end) nom_naissance,
            A.new_NomMarital nom_marital,
            (case when A.LastName is not null or A.LastName<>'' then A.LastName else L.user_last_name end) prenom,
            L.sex sexe,
            (case when A.birthdate is not null or A.birthdate<>'' then A.birthdate else L.birth_date end) date_naissance,
            A.new_Paysdenaissance pays_naissance,
            (case when A.new_lieudenaissance is not null or A.new_lieudenaissance<>'' then A.new_lieudenaissance else L.city end) lieu_naissance,
            A.new_Nomsetprnomsdelamre,
            A.new_Nomsetprnomsdupre,
            C.new_Nationalite,
            (case when B.new_ProfilInterne is not null or B.new_ProfilInterne <>'' then B.new_ProfilInterne  else  A.new_ProfilInterne end) as profession_client,
            A.new_Tutelleounondelapersonne,
            (case when B.new_Statutduclient is not null or B.new_Statutduclient <>'' then B.new_Statutduclient  else  A.new_Statutduclient end) as new_Statutduclient ,
            A.new_Situationbancaire,
            (case when B.new_Situationjudiciaire is not null or B.new_Situationjudiciaire <>'' then B.new_Situationjudiciaire  else  A.new_Situationjudiciaire end) as new_Situationjudiciaire,
            (case when B.new_datedbutinterdictionjudiciaire is not null or B.new_datedbutinterdictionjudiciaire <>'' then B.new_datedbutinterdictionjudiciaire  else  A.new_Datedudbutdinterdictionjudiciaire end) as new_datedbutinterdictionjudiciaire,
            (case when B.new_datefininterdictionjudiciaire is not null or B.new_datefininterdictionjudiciaire <>'' then B.new_datefininterdictionjudiciaire  else  A.new_Datedefindinterdictionjudiciaire end) as new_datefininterdictionjudiciaire,
            (case when A.new_Typepieceidentite is not null or A.new_Typepieceidentite<>'' then A.new_Typepieceidentite else C.new_typepieceidentite end) new_typepieceidentite,
            A.new_npiecedidentite piece_identite,
            A.new_identitydocumentissuedate,
            A.new_datedexpirationdelapiecedidentite,
            A.new_identitydocumentissueplace,
            A.new_Paysdmissiondelapicedidentification,
            A.new_Paysdersidence,
            (case when B.new_Codeagentconomique is not null or B.new_Codeagentconomique <>'' then B.new_Codeagentconomique  else  A.new_Codeagentconomique end) as new_Codeagentconomique,
            (case when B.new_Codesecteurdactivit is not null or B.new_Codesecteurdactivit <>'' then B.new_Codesecteurdactivit  else  A.new_Codesecteurdactivit end) as new_Codesecteurdactivit,
            (case when B.new_Notationinterne is not null or B.new_Notationinterne <>'' then B.new_Notationinterne  else  A.new_Notationinterne end) as new_Notationinterne,
            (case when B.new_PPE is not null or B.new_PPE <>'' then B.new_PPE  else  A.new_PPE end) as new_PPE,
            (case when B.new_RisqueAML is not null or B.new_RisqueAML <>'' then B.new_RisqueAML  else  A.new_RisqueAML end) as new_RisqueAML,
            (case when B.new_ProfilInterne is not null or B.new_ProfilInterne <>'' then B.new_ProfilInterne  else  A.new_ProfilInterne end) as new_ProfilInterne,
            D.guid,
            L.msisdn,
            (case when B.new_Typeducompte is not null or B.new_Typeducompte <>'' then B.new_Typeducompte  else  A.new_TypedecompteOM end) as new_Typeducompte,
            Z.acceptation_cgv,
            D.contrat_soucription,
            A.new_Datevalidation,
            A.new_Datecrationducompte,
            A.new_DisponibilitduScandelapicedidentification,
            D.identificateur,
            A.new_Motifrejetbackoffice,
            D.statut_validation_bo,
            A.new_Datedemisejour,
            D.imei,
            D.region_administrative,
            (case when D.ville is not null or D.ville<>'' then D.ville else  A.address1_city end) ville,
            (case when B.new_Natureduclienttitulaireducompte is not null or B.new_Natureduclienttitulaireducompte <>'' then B.new_Natureduclienttitulaireducompte  else  A.new_Natureduclienttitulaireducompte end) as new_Natureduclienttitulaireducompte ,
            (case when A.new_Numrodecompte is not null or A.new_Numrodecompte<>'' then A.new_Numrodecompte else B.new_Numrodecompte end) new_Numrodecompte,
            (case when datediff(to_date('###SLICE_VALUE###'),F.DATE_DERNIERE_ACTIVITE_OM) <= 30 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_30J,
            (case when datediff(to_date('###SLICE_VALUE###'),E.DATE_DERNIERE_ACTIVITE_OM) <= 90 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_90J,
            M.est_client_telco,
            M.conforme_art,
            Z.est_suspendu est_suspendu_telco,
            L.account_status est_suspendu_om,
            L.event_date
        FROM (SELECT * FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW WHERE event_date='###SLICE_VALUE###') L
        LEFT JOIN
        (SELECT * FROM CDR.SPARK_IT_CRM_PARTENAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') B
        ON trim(L.msisdn)=trim(B.new_numrodecompte)
        LEFT JOIN (SELECT * FROM CDR.SPARK_IT_CRM_MANDATAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') C
        ON trim(B.new_partenaireId) = trim(C.new_Partenaire)
        LEFT JOIN (SELECT msisdn,guid,contrat_soucription,identificateur,statut_validation_bo,ville,region_administrative,imei FROM  CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date=DATE_ADD('###SLICE_VALUE###',1)) D
        ON trim(L.msisdn)=trim(D.msisdn)
        LEFT JOIN (SELECT * FROM CDR.SPARK_IT_CRM_CONTACT_BASE WHERE original_file_date='###SLICE_VALUE###') A
        ON trim(L.msisdn) = trim(A.new_numrodecompte)
        LEFT JOIN (SELECT msisdn,est_suspendu,acceptation_cgv,conforme_art  FROM  MON.SPARK_FT_KYC_BDI_PP WHERE EVENT_DATE='###SLICE_VALUE###') Z
        ON trim(L.msisdn)=trim(Z.msisdn)
        LEFT JOIN
        (select IF(sender_msisdn is not null OR sender_msisdn<>'','OUI','NON')est_client_telco,sender_msisdn FROM (SELECT * FROM cdr.spark_it_omny_transactions WHERE transfer_datetime='###SLICE_VALUE###')  H
        INNER join 
        (select * from DIM.SPARK_DT_REF_OPERATEURS  where country_name like '%CAMEROON%' and ncc not in ("3342","3343","655","656","6570","6571","6573","6574","69","8","22945","222945","22258","6","6572","222258","62","233","243","242") ) K
        on substr(H.sender_msisdn,1,length(trim(K.ncc))) = trim(K.ncc)) M
        ON trim(L.msisdn) = trim(M.sender_msisdn)
        LEFT JOIN
        (SELECT MSISDN,MAX(DATE_DERNIERE_ACTIVITE_OM) DATE_DERNIERE_ACTIVITE_OM FROM MON.SPARK_FT_DATAMART_OM_MONTH 
        WHERE MOIS IN (substr(add_months(to_date('###SLICE_VALUE###'),-3),1,7), substr(add_months(to_date('###SLICE_VALUE###'),-2),1,7), substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7))
        GROUP BY MSISDN
        ) E 
        ON trim(L.msisdn)=trim(E.MSISDN)
        LEFT JOIN
        (SELECT MSISDN,MAX(DATE_DERNIERE_ACTIVITE_OM) DATE_DERNIERE_ACTIVITE_OM FROM MON.SPARK_FT_DATAMART_OM_MONTH 
        WHERE MOIS = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)
        GROUP BY MSISDN
        ) F 
        ON trim(L.msisdn)=trim(F.MSISDN)
        )RESULT
    ) B
ON  trim(A.msisdn) = trim(B.msisdn)