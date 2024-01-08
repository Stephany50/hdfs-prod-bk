CREATE TABLE MON.SPARK_FT_BDI_OM_KYC
(
    iban varchar(250),
    nom_naiss varchar(250),
    nom_marital varchar(250),
    prenom varchar(250),
    sexe varchar(250),
    date_naissance varchar(250),
    pays_naissance varchar(250),
    lieu_naissance varchar(250),
    nom_prenom_mere varchar(250),
    nom_prenom_pere varchar(250),
    nationalite varchar(250),
    profession_client varchar(250),
    nom_tutelle varchar(250),
    statut_client varchar(250),
    situation_bancaire varchar(250),
    situation_judiciaire varchar(250),
    date_debut_interdiction_judiciaire varchar(250),
    date_fin_interdiction_judiciaire varchar(250),
    type_piece_identite varchar(250),
    piece_identite varchar(250),
    date_emission_piece_identification varchar(250),
    date_fin_validite varchar(250),
    lieu_emission_piece varchar(250),
    pays_emission_piece varchar(250),
    pays_residence varchar(250),
    code_agent_economique varchar(250),
    code_secteur_activite varchar(250),
    rrc varchar(250),
    ppe varchar(250),
    risque_AML varchar(250),
    pro varchar(250),
    guid varchar(250),
    msisdn varchar(250),
    type_compte varchar(250),
    acceptation_cgu varchar(250),
    contrat_soucription varchar(250),
    date_validation varchar(250),
    date_creaction_compte varchar(250),
    disponibilite_scan varchar(250),
    identificateur varchar(250),
    motif_rejet_bo varchar(250),
    statut_validation_bo varchar(250),
    date_maj_om varchar(250),
    imei varchar(250),
    region_administrative varchar(250),
    ville varchar(250),
    nature_client_titulaire_compte varchar(250),
    numero_compte varchar(250),
    est_conforme_beac varchar(250),
    EST_ACTIF_30J varchar(250),
    EST_ACTIF_90J varchar(250),
    est_multicompte_om varchar(250),
    est_client_telco varchar(250),
    est_conforme_art varchar(250),
    est_suspendu_telco varchar(250),
    est_suspendu_om varchar(250),
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


  CREATE TABLE TMP.TT_BDI_OM_KYC_1
(
    iban varchar(250),
    nom_naiss varchar(250),
    nom_marital varchar(250),
    prenom varchar(250),
    sexe varchar(250),
    date_naissance varchar(250),
    pays_naissance varchar(250),
    lieu_naissance varchar(250),
    nom_prenom_mere varchar(250),
    nom_prenom_pere varchar(250),
    nationalite varchar(250),
    profession_client varchar(250),
    nom_tutelle varchar(250),
    statut_client varchar(250),
    situation_bancaire varchar(250),
    situation_judiciaire varchar(250),
    date_debut_interdiction_judiciaire varchar(250),
    date_fin_interdiction_judiciaire varchar(250),
    type_piece_identite varchar(250),
    piece_identite varchar(250),
    date_emission_piece_identification varchar(250),
    date_fin_validite varchar(250),
    lieu_emission_piece varchar(250),
    pays_emission_piece varchar(250),
    pays_residence varchar(250),
    code_agent_economique varchar(250),
    code_secteur_activite varchar(250),
    rrc varchar(250),
    ppe varchar(250),
    risque_AML varchar(250),
    pro varchar(250),
    guid varchar(250),
    msisdn varchar(250),
    type_compte varchar(250),
    acceptation_cgu varchar(250),
    contrat_soucription varchar(250),
    date_validation varchar(250),
    date_creaction_compte varchar(250),
    disponibilite_scan varchar(250),
    identificateur varchar(250),
    motif_rejet_bo varchar(250),
    statut_validation_bo varchar(250),
    date_maj_om varchar(250),
    imei varchar(250),
    region_administrative varchar(250),
    ville varchar(250),
    nature_client_titulaire_compte varchar(250),
    numero_compte varchar(250),
    EST_ACTIF_30J varchar(250),
    EST_ACTIF_90J varchar(250),
    est_client_telco varchar(250),
    est_conforme_art varchar(250),
    est_suspendu_telco varchar(250),
    est_suspendu_om varchar(250),
    EVENT_DATE DATE
 );
 
 CREATE TABLE TMP.TT_BDI_OM_KYC_2
(
    numeropiece varchar(250),
    est_multicompte_om varchar(20)
);