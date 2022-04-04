

CREATE TABLE AGG.SPARK_FT_A_BDI_B2B(
  key varchar(255),
  value bigint,
  insert_date timestamp
)comment 'SPARK_FT_A_BDI_B2B table'
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


CREATE TABLE AGG.SPARK_FT_A_BDI_B2B(
    type_personne string,             
    nom_structure_an bigint,          
    num_rccm_an bigint,               
    num_rpstant_legal_an bigint,      
    date_souscription_an bigint,      
    adresse_structure_an bigint,      
    num_telephone_an bigint,          
    nom_prenom_an bigint,             
    numero_piece_an bigint,           
    imei_an bigint,                   
    adresse_an bigint,                
    statut_an bigint,                 
    m2m_generique_nb int,             
    m2m_generique_actif int,          
    nb_ligne_en_anomalie bigint,      
    nb_actifs bigint,                 
    nb_total bigint,                  
    pers string,                      
    nom_structure_an_new bigint,      
    num_rccm_an_new bigint,           
    num_rpstant_legal_an_new bigint,  
    date_souscription_an_new bigint,  
    adresse_structure_an_new bigint,  
    num_telephone_an_new bigint,      
    nom_prenom_an_new bigint,         
    numero_piece_an_new bigint,       
    imei_an_new bigint,               
    adresse_an_new bigint,            
    statut_an_new bigint,             
    nb_ligne_en_anomalie_new bigint,  
    nb_actifs_new bigint,             
    nb_total_new bigint,
    nbr_corrected_by_day bigint,
    nbr_regression_by_day bigint,
    col1 bigint,
    col2 bigint,
    col3 bigint,
    col4 bigint,            
    insert_date timestamp)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 




----Stagging table in DATALAKE
CREATE TABLE TMP.SQ_SPARK_FT_A_BDI_B2B(
  type_personne varchar(255),             
  nom_structure_an decimal(20,3),          
  num_rccm_an decimal(20,3),               
  num_rpstant_legal_an decimal(20,3),      
  date_souscription_an decimal(20,3),      
  adresse_structure_an decimal(20,3),      
  num_telephone_an decimal(20,3),          
  nom_prenom_an decimal(20,3),             
  numero_piece_an decimal(20,3),           
  imei_an decimal(20,3),                   
  adresse_an decimal(20,3),                
  statut_an decimal(20,3),                 
  m2m_generique_nb int,             
  m2m_generique_actif int,          
  nb_ligne_en_anomalie decimal(20,3),      
  nb_actifs decimal(20,3),                 
  nb_total decimal(20,3),                  
  pers varchar(255),                      
  nom_structure_an_new decimal(20,3),      
  num_rccm_an_new decimal(20,3),           
  num_rpstant_legal_an_new decimal(20,3),  
  date_souscription_an_new decimal(20,3),  
  adresse_structure_an_new decimal(20,3),  
  num_telephone_an_new decimal(20,3),      
  nom_prenom_an_new decimal(20,3),         
  numero_piece_an_new decimal(20,3),       
  imei_an_new decimal(20,3),               
  adresse_an_new decimal(20,3),            
  statut_an_new decimal(20,3),             
  nb_ligne_en_anomalie_new decimal(20,3),  
  nb_actifs_new decimal(20,3),             
  nb_total_new decimal(20,3),
  nbr_corrected_by_day decimal(20,3),
  nbr_regression_by_day decimal(20,3),
  col1 decimal(20,3),
  col2 decimal(20,3),
  col3 decimal(20,3),
  col4 decimal(20,3),            
  insert_date timestamp,
  event_date DATE); 


---Stagging table in DWH
CREATE TABLE MON.SQ_FT_A_BDI_B2B(
 type_personne varchar(255),             
  nom_structure_an decimal(20,3),          
  num_rccm_an decimal(20,3),               
  num_rpstant_legal_an decimal(20,3),      
  date_souscription_an decimal(20,3),      
  adresse_structure_an decimal(20,3),      
  num_telephone_an decimal(20,3),          
  nom_prenom_an decimal(20,3),             
  numero_piece_an decimal(20,3),           
  imei_an decimal(20,3),                   
  adresse_an decimal(20,3),                
  statut_an decimal(20,3),                 
  m2m_generique_nb int,             
  m2m_generique_actif int,          
  nb_ligne_en_anomalie decimal(20,3),      
  nb_actifs decimal(20,3),                 
  nb_total decimal(20,3),                  
  pers varchar(255),                      
  nom_structure_an_new decimal(20,3),      
  num_rccm_an_new decimal(20,3),           
  num_rpstant_legal_an_new decimal(20,3),  
  date_souscription_an_new decimal(20,3),  
  adresse_structure_an_new decimal(20,3),  
  num_telephone_an_new decimal(20,3),      
  nom_prenom_an_new decimal(20,3),         
  numero_piece_an_new decimal(20,3),       
  imei_an_new decimal(20,3),               
  adresse_an_new decimal(20,3),            
  statut_an_new decimal(20,3),             
  nb_ligne_en_anomalie_new decimal(20,3),  
  nb_actifs_new decimal(20,3),             
  nb_total_new decimal(20,3),
  nbr_corrected_by_day decimal(20,3),
  nbr_regression_by_day decimal(20,3),
  col1 decimal(20,3),
  col2 decimal(20,3),
  col3 decimal(20,3),
  col4 decimal(20,3),            
  insert_date timestamp,
  event_date DATE);


---TABLE IN DWH
DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_A_BDI_B2B';
  MIN_DATE_PARTITION := '20220201';
  MAX_DATE_PARTITION := '20230301';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_A_BDI_B2B';
  PART_PARTITION_NAME := 'FT_A_B2B_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_P_MON_Jour_16M';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;