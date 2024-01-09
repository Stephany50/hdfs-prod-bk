--- INSERT INTO MON.SPARK_FT_BDI_OM_KYA
INSERT INTO TMP.TT_BDI_OM_KYA_1
SELECT 
nvl(B.piece_identite,A.piece_identite) as piece_identite,
case when B.iban is null or trim(B.iban) = '' then A.iban else B.iban end as iban,
case when B.rib is null or trim(B.rib) = '' then A.rib else B.rib end as rib,
case when B.ibu is null or trim(B.ibu) = '' then A.ibu else B.ibu end as ibu,
nvl(B.msisdn,A.msisdn) as msisdn,
case when B.date_creaction is null or trim(B.date_creaction) = '' then A.date_creaction else B.date_creaction end as date_creaction,
case when B.duree_de_vie is null or trim(B.duree_de_vie) = '' then A.duree_de_vie else B.duree_de_vie end as duree_de_vie,
case when B.raison_sociale is null or trim(B.raison_sociale) = '' then A.raison_sociale else B.raison_sociale end as raison_sociale,
case when B.sigle_raison_sociale is null or trim(B.sigle_raison_sociale) = '' then A.sigle_raison_sociale else B.sigle_raison_sociale end as sigle_raison_sociale,
case when B.forme_juridique is null or trim(B.forme_juridique) = '' then A.forme_juridique else B.forme_juridique end as forme_juridique,
case when B.secteur_activite_eco is null or trim(B.secteur_activite_eco) = '' then A.secteur_activite_eco else B.secteur_activite_eco end as secteur_activite_eco,
case when B.numero_regis_commerce is null or trim(B.numero_regis_commerce) = '' then A.numero_regis_commerce else B.numero_regis_commerce end as numero_regis_commerce,
case when B.numero_identification_fiscale is null or trim(B.numero_identification_fiscale) = '' then A.numero_identification_fiscale else B.numero_identification_fiscale end as numero_identification_fiscale,
case when B.pays_siege_social is null or trim(B.pays_siege_social) = '' then A.pays_siege_social else B.pays_siege_social end as pays_siege_social,
case when B.ville_siege_social is null or trim(B.ville_siege_social) = '' then A.ville_siege_social else B.ville_siege_social end as ville_siege_social,
case when B.statut_client is null or trim(B.statut_client) = '' then A.statut_client else B.statut_client end as statut_client,
case when B.nature_client_titulaire_compte is null or trim(B.nature_client_titulaire_compte) = '' then A.nature_client_titulaire_compte else B.nature_client_titulaire_compte end as nature_client_titulaire_compte,
case when B.type_compte is null or trim(B.type_compte) = '' then A.type_compte else B.type_compte end as type_compte,
case when B.code_devise is null or trim(B.code_devise) = '' then A.code_devise else B.code_devise end as code_devise,
case when B.statut_compte is null or trim(B.statut_compte) = '' then A.statut_compte else B.statut_compte end as statut_compte,
case when B.identification_mandataire is null or trim(B.identification_mandataire) = '' then A.identification_mandataire else B.identification_mandataire end as identification_mandataire,
case when B.numero_piece_mandataire is null or trim(B.numero_piece_mandataire) = '' then A.numero_piece_mandataire else B.numero_piece_mandataire end as numero_piece_mandataire,
case when B.numero_compte_om_mandataire is null or trim(B.numero_compte_om_mandataire) = '' then A.numero_compte_om_mandataire else B.numero_compte_om_mandataire end as numero_compte_om_mandataire,
case when B.numero_ibu_mandataire is null or trim(B.numero_ibu_mandataire) = '' then A.numero_ibu_mandataire else B.numero_ibu_mandataire end as numero_ibu_mandataire,
case when B.situation_judiciaire is null or trim(B.situation_judiciaire) = '' then A.situation_judiciaire else B.situation_judiciaire end as situation_judiciaire,
case when B.date_debut_interdiction_judiciaire is null or trim(B.date_debut_interdiction_judiciaire) = '' then A.date_debut_interdiction_judiciaire else B.date_debut_interdiction_judiciaire end as date_debut_interdiction_judiciaire,
case when B.date_fin_interdiction_judiciaire is null or trim(B.date_fin_interdiction_judiciaire) = '' then A.date_fin_interdiction_judiciaire else B.date_fin_interdiction_judiciaire end as date_fin_interdiction_judiciaire,
case when B.service_souscrit is null or trim(B.service_souscrit) = '' then A.service_souscrit else B.service_souscrit end as service_souscrit,
nvl(B.guid,A.guid) as guid,
case when B.pays_residence is null or trim(B.pays_residence) = '' then A.pays_residence else B.pays_residence end as pays_residence,
case when B.code_agent_economique is null or trim(B.code_agent_economique) = '' then A.code_agent_economique else B.code_agent_economique end as code_agent_economique,
case when B.code_secteur_activite is null or trim(B.code_secteur_activite) = '' then A.code_secteur_activite else B.code_secteur_activite end as code_secteur_activite,
case when B.rrc is null or trim(B.rrc) = '' then A.rrc else B.rrc end as rrc,
case when B.ppe is null or trim(B.ppe) = '' then A.ppe else B.ppe end as ppe,
case when B.risque_AML is null or trim(B.risque_AML) = '' then A.risque_AML else B.risque_AML end as risque_AML,
case when B.groupe is null or trim(B.groupe) = '' then A.groupe else B.groupe end as groupe,
case when B.pro is null or trim(B.pro) = '' then A.pro else B.pro end as pro,
case when B.qualite_mandataire is null or trim(B.qualite_mandataire) = '' then A.qualite_mandataire else B.qualite_mandataire end as qualite_mandataire,
case when B.responsabilite_compte is null or trim(B.responsabilite_compte) = '' then A.responsabilite_compte else B.responsabilite_compte end as responsabilite_compte,
case when B.date_expiration_piece_mandataire is null or trim(B.date_expiration_piece_mandataire) = '' then A.date_expiration_piece_mandataire else B.date_expiration_piece_mandataire end as date_expiration_piece_mandataire,
case when B.lieu_emission_piece_mandataire is null or trim(B.lieu_emission_piece_mandataire) = '' then A.lieu_emission_piece_mandataire else B.lieu_emission_piece_mandataire end as lieu_emission_piece_mandataire,
case when B.nom_mandataire is null or trim(B.nom_mandataire) = '' then A.nom_mandataire else B.nom_mandataire end as nom_mandataire,
case when B.prenom_mandataire is null or trim(B.prenom_mandataire) = '' then A.prenom_mandataire else B.prenom_mandataire end as prenom_mandataire,
case when B.nationalite_mandataire is null or trim(B.nationalite_mandataire) = '' then A.nationalite_mandataire else B.nationalite_mandataire end as nationalite_mandataire,
case when B.date_naissance_mandataire is null or trim(B.date_naissance_mandataire) = '' then A.date_naissance_mandataire else B.date_naissance_mandataire end as date_naissance_mandataire,
case when B.est_actif_30 is null or trim(B.est_actif_30) = '' then A.est_actif_30 else B.est_actif_30 end as est_actif_30,
case when B.est_actif_90 is null or trim(B.est_actif_90) = '' then A.est_actif_90 else B.est_actif_90 end as est_actif_90,
case when B.est_client_telco is null or trim(B.est_client_telco) = '' then A.est_client_telco else B.est_client_telco end as est_client_telco,
case when B.est_suspendu_telco is null or trim(B.est_suspendu_telco) = '' then A.est_suspendu_telco else B.est_suspendu_telco end as est_suspendu_telco,
case when B.est_suspendu_om is null or trim(B.est_suspendu_om) = '' then A.est_suspendu_om else B.est_suspendu_om end as est_suspendu_om,
B.event_date
    FROM (SELECT * FROM MON.SPARK_FT_BDI_OM_KYA WHERE EVENT_DATE=date_sub('###SLICE_VALUE###',1) ) A
    FULL OUTER JOIN
    (SELECT
        new_npiecedidentite piece_identite,
        new_iban iban,
        new_RIB rib,
        new_IBU ibu,
        msisdn ,
        new_Datedecration  date_creaction,
        new_Dureedevie duree_de_vie,
        new_raisonsociale raison_sociale,
        new_Sigledelaraisonsociale sigle_raison_sociale,
        new_Formejuridique forme_juridique,
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
        new_Situationjudiciaire situation_judiciaire,
        new_datedbutinterdictionjudiciaire date_debut_interdiction_judiciaire ,
        new_datefininterdictionjudiciaire date_fin_interdiction_judiciaire ,
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
        new_Responsabilitducompte responsabilite_compte,
        new_DatedExpirationdelapiece date_expiration_piece_mandataire,
        new_DelivrerA lieu_emission_piece_mandataire,
        new_Nom nom_mandataire,
        new_Prenom prenom_mandataire,
        new_Nationalite nationalite_mandataire,
        new_Datedenaissance date_naissance_mandataire,
        est_actif_30j est_actif_30,
        est_actif_90j est_actif_90,
        est_client_telco est_client_telco,
        est_suspendu_telco est_suspendu_telco,
        est_suspendu_om est_suspendu_om,
        event_date
    FROM
        (SELECT 
            A.new_npiecedidentite,
            (case when B.new_iban is not null or B.new_iban<>'' then B.new_iban else A.new_iban end) as new_iban,
            (case when B.new_RIB is not null or B.new_RIB<>'' then B.new_RIB else A.new_RIB end) as new_RIB,
            (case when B.new_IBU is not null or B.new_IBU<>'' then B.new_IBU else A.new_IBU end) as new_IBU,
            L.msisdn,
            B.new_Datedecration,
            B.new_Dureedevie,
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
            (case when B.new_Situationjudiciaire is not null or B.new_Situationjudiciaire <>'' then B.new_Situationjudiciaire  else  A.new_Situationjudiciaire end) as new_Situationjudiciaire,
            (case when B.new_datedbutinterdictionjudiciaire is not null or B.new_datedbutinterdictionjudiciaire <>'' then B.new_datedbutinterdictionjudiciaire  else  A.new_Datedudbutdinterdictionjudiciaire end) as new_datedbutinterdictionjudiciaire,
            (case when B.new_datefininterdictionjudiciaire is not null or B.new_datefininterdictionjudiciaire <>'' then B.new_datefininterdictionjudiciaire  else  A.new_Datedefindinterdictionjudiciaire end) as new_datefininterdictionjudiciaire,
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
            B.new_Responsabilitducompte,
            C.new_DatedExpirationdelapiece,
            C.new_DelivrerA,
            C.new_Nom,
            C.new_Prenom,
            C.new_Nationalite,
            C.new_Datedenaissance,
            (case when datediff(to_date('###SLICE_VALUE###'),F.DATE_DERNIERE_ACTIVITE_OM) <= 30 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_30J,
            (case when datediff(to_date('###SLICE_VALUE###'),E.DATE_DERNIERE_ACTIVITE_OM) <= 90 THEN 'OUI' ELSE 'NON' END) AS EST_ACTIF_90J,
            M.est_client_telco,
            Z.est_suspendu est_suspendu_telco,
            L.account_status est_suspendu_om,
            L.event_date
        FROM (select * from (select distinct msisdn,account_status,event_date, row_number() over (partition by msisdn order by to_date(modified_on) desc nulls last) rang FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW WHERE event_date='###SLICE_VALUE###')P where rang=1) L
        LEFT JOIN
        (SELECT * FROM CDR.SPARK_IT_CRM_PARTENAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') B
        ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.new_numrodecompte))
        LEFT JOIN (SELECT * FROM CDR.SPARK_IT_CRM_MANDATAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') C
        ON trim(B.new_partenaireId) = trim(C.new_Partenaire)
        LEFT JOIN (SELECT msisdn,guid  FROM  CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date=DATE_ADD('###SLICE_VALUE###',1)) D
        ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(D.msisdn))
        LEFT JOIN (SELECT * FROM CDR.SPARK_IT_CRM_CONTACT_BASE WHERE original_file_date='###SLICE_VALUE###') A
        ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.new_numrodecompte))
        LEFT JOIN (SELECT msisdn,est_suspendu  FROM  MON.SPARK_FT_KYC_BDI_PP WHERE EVENT_DATE='###SLICE_VALUE###') Z
        ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(Z.msisdn))
        LEFT JOIN
        (select IF(sender_msisdn is not null OR sender_msisdn<>'','OUI','NON')est_client_telco,sender_msisdn FROM (SELECT * FROM cdr.spark_it_omny_transactions WHERE transfer_datetime='###SLICE_VALUE###')  H
        INNER join 
        (select * from DIM.SPARK_DT_REF_OPERATEURS  where country_name like '%CAMEROON%' and ncc not in ("3342","3343","655","656","6570","6571","6573","6574","69","8","22945","222945","22258","6","6572","222258","62","233","243","242") ) K
        on substr(H.sender_msisdn,1,length(trim(K.ncc))) = trim(K.ncc)) M
        ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(M.sender_msisdn))
        LEFT JOIN
        (SELECT MSISDN,MAX(DATE_DERNIERE_ACTIVITE_OM) DATE_DERNIERE_ACTIVITE_OM FROM MON.SPARK_FT_DATAMART_OM_MONTH 
        WHERE MOIS IN (substr(add_months(to_date('###SLICE_VALUE###'),-3),1,7), substr(add_months(to_date('###SLICE_VALUE###'),-2),1,7), substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7))
        GROUP BY MSISDN
        ) E 
        ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(E.MSISDN))
        LEFT JOIN
        (SELECT MSISDN,MAX(DATE_DERNIERE_ACTIVITE_OM) DATE_DERNIERE_ACTIVITE_OM FROM MON.SPARK_FT_DATAMART_OM_MONTH 
        WHERE MOIS = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)
        GROUP BY MSISDN
        ) F 
        ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(L.msisdn))=FN_FORMAT_MSISDN_TO_9DIGITS(trim(F.MSISDN))
        )RESULT
    ) B
ON  trim(A.msisdn) = trim(B.msisdn)