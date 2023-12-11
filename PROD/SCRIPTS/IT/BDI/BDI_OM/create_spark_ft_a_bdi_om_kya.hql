CREATE TABLE AGG.SPARK_FT_BDI_OM_KYA
(
    piece_identite varchar(250),
    iban varchar(250),
    rib varchar(250),
    ibu varchar(250),
    msisdn varchar(250),
    date_creaction varchar(250),
    duree_de_vie varchar(250),
    raison_sociale varchar(250),
    sigle_raison_sociale varchar(250),
    forme_juridique varchar(250),
    secteur_activite_eco varchar(250),
    numero_regis_commerce varchar(250),
    numero_identification_fiscale varchar(250),
    pays_siege_social varchar(250),
    ville_siege_social varchar(250),
    statut_client varchar(250),
    nature_client_titulaire_compte varchar(250),
    type_compte varchar(250),
    code_devise varchar(250),
    statut_compte varchar(250),
    identification_mandataire varchar(250),
    numero_piece_mandataire varchar(250),
    numero_compte_om_mandataire varchar(250),
    numero_ibu_mandataire varchar(250),
    situation_judiciaire varchar(250),
    date_debut_interdiction_judiciaire varchar(250),
    date_fin_interdiction_judiciaire varchar(250),
    service_souscrit varchar(250),
    guid varchar(250),
    pays_residence varchar(250),
    code_agent_economique varchar(250),
    code_secteur_activite varchar(250),
    rrc varchar(250),
    ppe varchar(250),
    risque_AML varchar(250),
    groupe varchar(250),
    pro varchar(250),
    qualite_mandataire varchar(250),
    responsabilite_compte varchar(250),
    date_expiration_piece_mandataire varchar(250),
    lieu_emission_piece_mandataire varchar(250),
    nom_mandataire varchar(250),
    prenom_mandataire varchar(250),
    nationalite_mandataire varchar(250),
    date_naissance_mandataire varchar(250),
    est_conforme_beac varchar(20),
    est_multicompte_om varchar(20),
    est_actif_30 varchar(250),
    est_actif_90 varchar(250),
    est_client_telco varchar(250),
    est_suspendu_telco varchar(250),
    est_suspendu_om varchar(250)
   )
  PARTITIONED BY (EVENT_DATE DATE)
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");

  --Staging table in DWH
CREATE TABLE MON.SQ_FT_A_BDI_OM_KYA (
    piece_identite varchar(250),
    iban varchar(250),
    rib varchar(250),
    ibu varchar(250),
    msisdn varchar(250),
    date_creaction varchar(250),
    duree_de_vie varchar(250),
    raison_sociale varchar(250),
    sigle_raison_sociale varchar(250),
    forme_juridique varchar(250),
    secteur_activite_eco varchar(250),
    numero_regis_commerce varchar(250),
    numero_identification_fiscale varchar(250),
    pays_siege_social varchar(250),
    ville_siege_social varchar(250),
    statut_client varchar(250),
    nature_client_titulaire_compte varchar(250),
    type_compte varchar(250),
    code_devise varchar(250),
    statut_compte varchar(250),
    identification_mandataire varchar(250),
    numero_piece_mandataire varchar(250),
    numero_compte_om_mandataire varchar(250),
    numero_ibu_mandataire varchar(250),
    situation_judiciaire varchar(250),
    date_debut_interdiction_jud varchar(250),
    date_fin_interdiction_jud varchar(250),
    service_souscrit varchar(250),
    guid varchar(250),
    pays_residence varchar(250),
    code_agent_economique varchar(250),
    code_secteur_activite varchar(250),
    rrc varchar(250),
    ppe varchar(250),
    risque_AML varchar(250),
    groupe varchar(250),
    pro varchar(250),
    qualite_mandataire varchar(250),
    responsabilite_compte varchar(250),
    date_expiration_piece_mand varchar(250),
    lieu_emission_piece_mand varchar(250),
    nom_mandataire varchar(250),
    prenom_mandataire varchar(250),
    nationalite_mandataire varchar(250),
    date_naissance_mandataire varchar(250),
    est_conforme_beac varchar(20),
    est_multicompte_om varchar(20),
    est_actif_30 varchar(250),
    est_actif_90 varchar(250),
    est_client_telco varchar(250),
    est_suspendu_telco varchar(250),
    est_suspendu_om varchar(250),
    EVENT_DATE DATE
);



---Staging table in data lake
CREATE TABLE TMP.FT_A_BDI_OM_KYA (
    piece_identite varchar(250),
    iban varchar(250),
    rib varchar(250),
    ibu varchar(250),
    msisdn varchar(250),
    date_creaction varchar(250),
    duree_de_vie varchar(250),
    raison_sociale varchar(250),
    sigle_raison_sociale varchar(250),
    forme_juridique varchar(250),
    secteur_activite_eco varchar(250),
    numero_regis_commerce varchar(250),
    numero_identification_fiscale varchar(250),
    pays_siege_social varchar(250),
    ville_siege_social varchar(250),
    statut_client varchar(250),
    nature_client_titulaire_compte varchar(250),
    type_compte varchar(250),
    code_devise varchar(250),
    statut_compte varchar(250),
    identification_mandataire varchar(250),
    numero_piece_mandataire varchar(250),
    numero_compte_om_mandataire varchar(250),
    numero_ibu_mandataire varchar(250),
    situation_judiciaire varchar(250),
    date_debut_interdiction_jud varchar(250),
    date_fin_interdiction_jud varchar(250),
    service_souscrit varchar(250),
    guid varchar(250),
    pays_residence varchar(250),
    code_agent_economique varchar(250),
    code_secteur_activite varchar(250),
    rrc varchar(250),
    ppe varchar(250),
    risque_AML varchar(250),
    groupe varchar(250),
    pro varchar(250),
    qualite_mandataire varchar(250),
    responsabilite_compte varchar(250),
    date_expiration_piece_mand varchar(250),
    lieu_emission_piece_mand varchar(250),
    nom_mandataire varchar(250),
    prenom_mandataire varchar(250),
    nationalite_mandataire varchar(250),
    date_naissance_mandataire varchar(250),
    est_conforme_beac varchar(20),
    est_multicompte_om varchar(20),
    est_actif_30 varchar(250),
    est_actif_90 varchar(250),
    est_client_telco varchar(250),
    est_suspendu_telco varchar(250),
    est_suspendu_om varchar(250),
    EVENT_DATE DATE
);


DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_A_BDI_OM_KYA';
  MIN_DATE_PARTITION := '20231101';
  MAX_DATE_PARTITION := '20260101';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_A_BDI_OM_KYA';
  PART_PARTITION_NAME := 'FT_A_BDI_OM_KYA_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_P_CDR_J01_16M';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;