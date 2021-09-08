---NAME : MON.SPARK_FT_B2B_ANOMALIES
---DESC : cette table contient la liste des personnes morales entreprise, flotte et M2M mon conforme.
---FIELDS:
---      * type_personne permet de savoir s'il s'agit d'une entreprise(ENT), Flotte(FLOTTE) ou M2M
CREATE TABLE MON.SPARK_FT_B2B_ANOMALIES(
type_personne VARCHAR(255),             
nom_structure VARCHAR(255),                         
numero_registre_commerce VARCHAR(255),              
num_piece_representant_legal VARCHAR(255),          
date_souscription date,                  
adresse_structure VARCHAR(255),                     
msisdn VARCHAR(255), 
nom_prenom VARCHAR(255),                            
numero_piece VARCHAR(255),                          
imei VARCHAR(255),                                  
adresse VARCHAR(255),                               
statut VARCHAR(255),                                
compte_client VARCHAR(255),
type_personne_morale VARCHAR(255),
type_piece VARCHAR(255),                            
nom VARCHAR(255),                                   
prenom VARCHAR(255),                                
date_naissance date,                          
date_expiration date,                         
ville VARCHAR(255),                                 
quartier VARCHAR(255),
ville_structure VARCHAR(255),                       
quartier_structure VARCHAR(255),                               
compte_client_structure VARCHAR(255),
motif_anomalie VARCHAR(512),
INSERT_DATE TIMESTAMP
) COMMENT 'MON.SPARK_FT_B2B_ANOMALIES'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 


---Stagging table in dataLake
CREATE TABLE TMP.SQ_FT_B2B_ANOMALIES(
type_personne VARCHAR(255),             
nom_structure VARCHAR(255),                         
numero_registre_commerce VARCHAR(255),              
num_piece_representant_legal VARCHAR(255),          
date_souscription date,                  
adresse_structure VARCHAR(255),                     
msisdn VARCHAR(255), 
nom_prenom VARCHAR(255),                            
numero_piece VARCHAR(255),                          
imei VARCHAR(255),                                  
adresse VARCHAR(255),                               
statut VARCHAR(255),                                
compte_client VARCHAR(255),
type_personne_morale VARCHAR(255),
type_piece VARCHAR(255),                            
nom VARCHAR(255),                                   
prenom VARCHAR(255),                                
date_naissance date,                          
date_expiration date,                         
ville VARCHAR(255),                                 
quartier VARCHAR(255),
ville_structure VARCHAR(255),                       
quartier_structure VARCHAR(255),                               
compte_client_structure VARCHAR(255),
motif_anomalie VARCHAR(512),
insert_date TIMESTAMP,
event_date DATE);


---Staging table in DWH
CREATE TABLE MON.SQ_FT_B2B_ANOMALIES(
type_personne VARCHAR(255 BYTE),             
nom_structure VARCHAR(255 BYTE),                         
numero_registre_commerce VARCHAR(255 BYTE),              
num_piece_representant_legal VARCHAR(255 BYTE),          
date_souscription date,                  
adresse_structure VARCHAR(255 BYTE),                     
msisdn VARCHAR(255 BYTE), 
nom_prenom VARCHAR(255 BYTE),                            
numero_piece VARCHAR(255 BYTE),                          
imei VARCHAR(255 BYTE),                                  
adresse VARCHAR(255 BYTE),                               
statut VARCHAR(255 BYTE),                                
compte_client VARCHAR(255 BYTE),
type_personne_morale VARCHAR(255 BYTE),
type_piece VARCHAR(255 BYTE),                            
nom VARCHAR(255 BYTE),                                   
prenom VARCHAR(255 BYTE),                                
date_naissance date,                          
date_expiration date,                         
ville VARCHAR(255 BYTE),                                 
quartier VARCHAR(255 BYTE),
ville_structure VARCHAR(255 BYTE),                       
quartier_structure VARCHAR(255 BYTE),                               
compte_client_structure VARCHAR(255 BYTE),
motif_anomalie VARCHAR(512 BYTE),
insert_date TIMESTAMP,
event_date DATE);



DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_B2B_ANOMALIES';
  MIN_DATE_PARTITION := '20210701';
  MAX_DATE_PARTITION := '20220901';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_B2B_ANOMALIES';
  PART_PARTITION_NAME := 'FT_B2B_AN_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_MIG_64K';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;