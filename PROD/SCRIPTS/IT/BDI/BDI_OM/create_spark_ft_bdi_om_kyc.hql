CREATE TABLE MON.SPARK_FT_BDI_OM_KYC
(
    iban  varchar(250)  ,
    rib   varchar(250),
    ibu   varchar(250),
    nom_naiss varchar(250)  ,
    nom_marital   varchar(250),
    prenom    varchar(250)  ,
    sexe varchar(50) ,
    date_naissance    varchar(250)  ,
    pays_naissance    varchar(250),
    lieu_naissance    varchar(250)  ,
    nom_prenom_mere   varchar(250),
    nom_prenom_pere   varchar(250),
    nature_compte varchar(250)  ,
    statut_compte varchar(250)  ,
    code_devise   varchar(250),
    ibu_mandataire    varchar(250),
    id_interne_mandataire varchar(250),
    qualite_mandataire    varchar(250),
    date_naissance_mandataire varchar(250),
    nom_mandataire    varchar(250),
    prenom_mandataire varchar(250),
    nationalite   varchar(250),
    profession_client varchar(250)  ,
    nom_tutelle   varchar(250),
    statut_client varchar(250)  ,
    situation_bancaire    varchar(250),
    situation_judiciaire  varchar(250)  ,
    date_debut_interdiction_judiciaire    varchar(250)  ,
    date_fin_interdiction_judiciaire  varchar(250)  ,
    type_piece_identite   varchar(250),
    piece_identite    varchar(250),
    date_emission_piece_identification    varchar(250),
    date_fin_validite varchar(250),
    lieu_emission_piece   varchar(250),
    pays_emission_piece   varchar(250),
    pays_residence    varchar(250),
    code_agent_economique varchar(250)  ,
    code_secteur_activite varchar(250)  ,
    notation_interne  varchar(250)  ,
    ppe   varchar(250)  ,
    risque_aml    varchar(250)  ,
    profil_interne    varchar(250)  ,
    guid  varchar(250)  ,
    msisdn   varchar(50) ,
    type_compte   varchar(250)  ,
    acceptation_cgu   varchar(250),
    contrat_soucription   varchar(250)  ,
    date_validation   varchar(250),
    date_creation_compte  varchar(250),
    disponibilite_scan    varchar(250),
    identificateur    varchar(250)  ,
    motif_rejet_bo    varchar(250),
    statut_validation_bo  varchar(250)  ,
    date_maj_om   varchar(250),
    imei  varchar(250)  ,
    region_administrative varchar(250)  ,
    ville varchar(250)  ,
    nature_client_titulaire_compte    varchar(250)  ,
    numero_compte varchar(250)  ,
    est_conforme_hors_cip varchar(20),
    est_conforme_cip varchar(20),
    est_actif_30j varchar(250)  ,
    est_actif_90j varchar(250)  ,
    est_multicompte_om varchar(250),
    est_client_telco  varchar(250)  ,
    est_conforme_art  varchar(250),
    est_suspendu_telco    varchar(250),
    est_suspendu_om  varchar(50),
    iban_absent varchar(250),
    nom_naiss_absent varchar(250),
    nom_marital_absent varchar(250),
    prenom_absent varchar(250),
    sexe_absent varchar(250),
    date_naissance_absent varchar(250),
    pays_naissance_absent varchar(250),
    lieu_naissance_absent varchar(250),
    nom_prenom_mere_absent varchar(250),
    nom_prenom_pere_absent varchar(250),
    nationalite_absent varchar(250),
    profession_client_absent varchar(250),
    nom_tutelle_absent varchar(250),
    statut_client_absent varchar(250),
    situation_bancaire_absent varchar(250),
    situation_judiciaire_absent varchar(250),
    date_debut_interdiction_judiciaire_absent varchar(250),
    date_fin_interdiction_judiciaire_absent varchar(250),
    type_piece_identite_absent varchar(250),
    piece_identite_absent varchar(250),
    date_emission_piece_identification_absent varchar(250),
    date_fin_validite_absent varchar(250),
    lieu_emission_piece_absent varchar(250),
    pays_emission_piece_absent varchar(250) 

 )
  PARTITIONED BY (EVENT_DATE DATE)
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");

CREATE TABLE TMP.TT_BDI_OM_KYC_STEP_1
(
    new_iban varchar(250),
    new_rib  varchar(250),
    new_ibu  varchar(250),
    user_first_name  varchar(250),
    user_last_name varchar(250),
    sexe    varchar(50) ,
    birth_date     varchar(250),
    city    varchar(100),
    new_natureducompte   varchar(250),
    new_statutducompte   varchar(250),
    new_codedevise varchar(250),
    new_numerodelibudumandataire   varchar(250),
    new_identifiantinternedumandataire   varchar(250),
    new_qualitdumandataire   varchar(250),
    new_datedenaissance  varchar(250),
    new_nom  varchar(250),
    new_prenom     varchar(250),
    new_nationalite      varchar(250),
    new_statutduclient   varchar(250),
    new_situationjudiciaire    varchar(250),
    new_datedbutinterdictionjudiciaire     varchar(250),
    new_datefininterdictionjudiciaire      varchar(250),
    new_typepieceidentite      varchar(250),
    new_codeagentconomique     varchar(250),
    new_codesecteurdactivit    varchar(250),
    new_notationinterne  varchar(250),
    new_ppe  varchar(250),
    new_risqueaml  varchar(250),
    new_profilinterne    varchar(250),
    guid     varchar(250),
    msisdn  varchar(50) ,
    new_typeducompte     varchar(250),
    contrat_soucription  varchar(250),
    identificateur varchar(250),
    statut_validation_bo varchar(250),
    imei     varchar(250),
    region_administrative      varchar(250),
    ville    varchar(250),
    new_natureduclienttitulaireducompte    varchar(250),
    new_numrodecompte    varchar(250),
    est_suspendu_om     varchar(50) ,
    event_date    date
);

CREATE TABLE TMP.TT_BDI_OM_KYC_STEP_2
(
    new_iban     varchar(250),
    new_rib      varchar(250),
    new_ibu      varchar(250),
    nom_naissance      varchar(250),
    nom_marital  varchar(250),
    prenom varchar(250),
    sexe  varchar(50) ,
    date_naissance     varchar(250),
    pays_naissance     varchar(250),
    lieu_naissance     varchar(250),
    new_nomsetprnomsdelamre  varchar(250),
    new_nomsetprnomsdupre    varchar(250),
    new_natureducompte varchar(250),
    new_statutducompte varchar(250),
    new_codedevise     varchar(250),
    numero_ibu_mandataire    varchar(250),
    id_interne_mandataire    varchar(250),
    qualite_mandataire varchar(250),
    date_naissance_mandataire      varchar(250),
    nom_mandataire     varchar(250),
    prenom_mandataire  varchar(250),
    new_nationalite    varchar(250),
    profession_client  varchar(250),
    new_tutelleounondelapersonne   varchar(250),
    new_statutduclient varchar(250),
    new_situationbancaire    varchar(250),
    new_situationjudiciaire  varchar(250),
    new_datedbutinterdictionjudiciaire   varchar(250),
    new_datefininterdictionjudiciaire    varchar(250),
    new_typepieceidentite    varchar(250),
    piece_identite     varchar(250),
    new_identitydocumentissuedate  varchar(250),
    new_datedexpirationdelapiecedidentite      varchar(250),
    new_identitydocumentissueplace varchar(250),
    new_paysdmissiondelapicedidentification    varchar(250),
    new_paysdersidence varchar(250),
    new_codeagentconomique   varchar(250),
    new_codesecteurdactivit  varchar(250),
    new_notationinterne      varchar(250),
    new_ppe      varchar(250),
    new_risqueaml      varchar(250),
    new_profilinterne  varchar(250),
    guid   varchar(250),
    msisdn varchar(50) ,
    new_typeducompte   varchar(250),
    acceptation_cgv    varchar(250),
    contrat_soucription      varchar(250),
    new_datevalidation varchar(250),
    new_datecrationducompte  varchar(250),
    new_disponibilitduscandelapicedidentification    varchar(250),
    identificateur     varchar(250),
    new_motifrejetbackoffice varchar(250),
    statut_validation_bo     varchar(250),
    new_datedemisejour varchar(250),
    imei   varchar(250),
    region_administrative    varchar(250),
    ville  varchar(250),
    new_natureduclienttitulaireducompte  varchar(250),
    new_numrodecompte  varchar(250),
    conforme_art varchar(250),
    est_suspendu_telco varchar(250),
    est_suspendu_om   varchar(50) ,
    event_date  date
);


CREATE TABLE TMP.TT_BDI_OM_KYC_STEP_3
(
    iban  varchar(250)  ,
    rib   varchar(250),
    ibu   varchar(250),
    nom_naiss varchar(250)  ,
    nom_marital   varchar(250),
    prenom    varchar(250)  ,
    sexe varchar(50) ,
    date_naissance    varchar(250)  ,
    pays_naissance    varchar(250),
    lieu_naissance    varchar(250)  ,
    nom_prenom_mere   varchar(250),
    nom_prenom_pere   varchar(250),
    nature_compte varchar(250)  ,
    statut_compte varchar(250)  ,
    code_devise   varchar(250),
    ibu_mandataire    varchar(250),
    id_interne_mandataire varchar(250),
    qualite_mandataire    varchar(250),
    date_naissance_mandataire varchar(250),
    nom_mandataire    varchar(250),
    prenom_mandataire varchar(250),
    nationalite   varchar(250),
    profession_client varchar(250)  ,
    nom_tutelle   varchar(250),
    statut_client varchar(250)  ,
    situation_bancaire    varchar(250),
    situation_judiciaire  varchar(250)  ,
    date_debut_interdiction_judiciaire    varchar(250)  ,
    date_fin_interdiction_judiciaire  varchar(250)  ,
    type_piece_identite   varchar(250),
    piece_identite    varchar(250),
    date_emission_piece_identification    varchar(250),
    date_fin_validite varchar(250),
    lieu_emission_piece   varchar(250),
    pays_emission_piece   varchar(250),
    pays_residence    varchar(250),
    code_agent_economique varchar(250)  ,
    code_secteur_activite varchar(250)  ,
    notation_interne  varchar(250)  ,
    ppe   varchar(250)  ,
    risque_aml    varchar(250)  ,
    profil_interne    varchar(250)  ,
    guid  varchar(250)  ,
    msisdn   varchar(50) ,
    type_compte   varchar(250)  ,
    acceptation_cgu   varchar(250),
    contrat_soucription   varchar(250)  ,
    date_validation   varchar(250),
    date_creation_compte  varchar(250),
    disponibilite_scan    varchar(250),
    identificateur    varchar(250)  ,
    motif_rejet_bo    varchar(250),
    statut_validation_bo  varchar(250)  ,
    date_maj_om   varchar(250),
    imei  varchar(250)  ,
    region_administrative varchar(250)  ,
    ville varchar(250)  ,
    nature_client_titulaire_compte    varchar(250)  ,
    numero_compte varchar(250)  ,
    est_actif_30j varchar(250)  ,
    est_actif_90j varchar(250)  ,
    est_client_telco  varchar(250)  ,
    est_conforme_art  varchar(250),
    est_suspendu_telco    varchar(250),
    est_suspendu_om  varchar(50),
    EVENT_DATE DATE

 );
 
 CREATE TABLE TMP.TT_BDI_OM_KYC_STEP_4
(
    numeropiece varchar(250),
    est_multicompte_om varchar(20)
);


CREATE TABLE TMP.TT_BDI_OM_KYC_STEP_5
(
    iban  varchar(250)  ,
    rib   varchar(250),
    ibu   varchar(250),
    nom_naiss varchar(250)  ,
    nom_marital   varchar(250),
    prenom    varchar(250)  ,
    sexe varchar(50) ,
    date_naissance    varchar(250)  ,
    pays_naissance    varchar(250),
    lieu_naissance    varchar(250)  ,
    nom_prenom_mere   varchar(250),
    nom_prenom_pere   varchar(250),
    nature_compte varchar(250)  ,
    statut_compte varchar(250)  ,
    code_devise   varchar(250),
    ibu_mandataire    varchar(250),
    id_interne_mandataire varchar(250),
    qualite_mandataire    varchar(250),
    date_naissance_mandataire varchar(250),
    nom_mandataire    varchar(250),
    prenom_mandataire varchar(250),
    nationalite   varchar(250),
    profession_client varchar(250)  ,
    nom_tutelle   varchar(250),
    statut_client varchar(250)  ,
    situation_bancaire    varchar(250),
    situation_judiciaire  varchar(250)  ,
    date_debut_interdiction_judiciaire    varchar(250)  ,
    date_fin_interdiction_judiciaire  varchar(250)  ,
    type_piece_identite   varchar(250),
    piece_identite    varchar(250),
    date_emission_piece_identification    varchar(250),
    date_fin_validite varchar(250),
    lieu_emission_piece   varchar(250),
    pays_emission_piece   varchar(250),
    pays_residence    varchar(250),
    code_agent_economique varchar(250)  ,
    code_secteur_activite varchar(250)  ,
    notation_interne  varchar(250)  ,
    ppe   varchar(250)  ,
    risque_aml    varchar(250)  ,
    profil_interne    varchar(250)  ,
    guid  varchar(250)  ,
    msisdn   varchar(50) ,
    type_compte   varchar(250)  ,
    acceptation_cgu   varchar(250),
    contrat_soucription   varchar(250)  ,
    date_validation   varchar(250),
    date_creation_compte  varchar(250),
    disponibilite_scan    varchar(250),
    identificateur    varchar(250)  ,
    motif_rejet_bo    varchar(250),
    statut_validation_bo  varchar(250)  ,
    date_maj_om   varchar(250),
    imei  varchar(250)  ,
    region_administrative varchar(250)  ,
    ville varchar(250)  ,
    nature_client_titulaire_compte    varchar(250)  ,
    numero_compte varchar(250)  ,
    est_actif_30j varchar(250)  ,
    est_actif_90j varchar(250)  ,
    est_multicompte_om varchar(250),
    est_client_telco  varchar(250)  ,
    est_conforme_art  varchar(250),
    est_suspendu_telco    varchar(250),
    est_suspendu_om  varchar(50),
    EVENT_DATE DATE

 );