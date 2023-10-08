CREATE TABLE MON.SPARK_FT_BDI_OM_KYA
(
    iban varchar(250),
    rib varchar(250),
    msisdn varchar(250),
    date_creaction varchar(250),
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
    date_expiration_piece_mandataire varchar(250),
    lieu_emission_piece_mandataire varchar(250),
    nom_mandataire varchar(250),
    prenom_mandataire varchar(250),
    est_actif_30 varchar(250),
    est_actif_90 varchar(250),
    est_client_telco varchar(250),
    est_suspendu_telco varchar(250),
    est_suspendu_om varchar(250)
   )
  PARTITIONED BY (EVENT_DATE DATE)
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");