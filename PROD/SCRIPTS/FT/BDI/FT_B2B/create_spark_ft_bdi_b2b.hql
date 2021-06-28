--Staging table in DataLake
CREATE TABLE TMP.SQ_FT_BDI_B2B(              
nom_structure VARCHAR(255),                         
numero_registre_commerce VARCHAR(255),              
num_piece_representant_legal VARCHAR(255),          
date_souscription timestamp,                  
adresse_structure VARCHAR(255),                     
msisdn VARCHAR(255),                                
nom_prenom VARCHAR(255),                            
numero_piece VARCHAR(255),                          
imei VARCHAR(255),                                  
adresse VARCHAR(255),                               
statut VARCHAR(255),                                
disponibilite_scan VARCHAR(255),                    
acceptation_cgv VARCHAR(255),                       
customer_id VARCHAR(255),                           
contract_id VARCHAR(255),                           
compte_client VARCHAR(255),                         
type_personne VARCHAR(255),                         
type_piece VARCHAR(255),                            
id_type_piece VARCHAR(255),                         
nom VARCHAR(255),                                   
prenom VARCHAR(255),                                
date_naissance date,                          
date_expiration date,                         
ville VARCHAR(255),                                 
quartier VARCHAR(255),                              
statut_old VARCHAR(255),                            
raison_statut VARCHAR(255),                         
odbic VARCHAR(255),                                 
odboc VARCHAR(255),                                 
date_changement_statut timestamp,             
plan_localisation VARCHAR(255),                     
contrat_soucription VARCHAR(255),                   
type_piece_tuteur VARCHAR(255),                     
numero_piece_tuteur VARCHAR(255),                   
nom_tuteur VARCHAR(255),                            
prenom_tuteur VARCHAR(255),                         
date_naissance_tuteur date,                   
date_expiration_tuteur date,                  
adresse_tuteur VARCHAR(255),                        
compte_client_structure VARCHAR(255),               
statut_derogation VARCHAR(255),                     
region_administrative VARCHAR(255),                 
region_commerciale VARCHAR(255),                    
site_name VARCHAR(255),                             
ville_site VARCHAR(255),                            
offre_commerciale VARCHAR(255),                     
type_contrat VARCHAR(255),                          
segmentation VARCHAR(255),                          
derogation_identification VARCHAR(255),             
compte_client_parent VARCHAR(255),                  
nom_representant_legal VARCHAR(255),                
prenom_representant_legal VARCHAR(255),             
contact_telephonique VARCHAR(255),                  
ville_structure VARCHAR(255),                       
quartier_structure VARCHAR(255),                    
sms_contact VARCHAR(255),                           
doc_plan_localisation VARCHAR(255),                 
doc_fiche_souscription VARCHAR(255),                
doc_attestation_cnps VARCHAR(255),                  
doc_rccm VARCHAR(255),                              
type_client VARCHAR(255),                           
rang int,                                     
type_personne_morale VARCHAR(255),                  
nom_structure_an VARCHAR(255),                      
rccm_an VARCHAR(255),                               
num_piece_rpstant_an VARCHAR(255),                  
date_souscription_an VARCHAR(255),                  
adresse_structure_an VARCHAR(255),                  
num_tel_an VARCHAR(255),                            
nom_prenom_an VARCHAR(255),                         
numero_piece_an VARCHAR(255),                       
imei_an VARCHAR(255),                               
adresse_an VARCHAR(255),                            
statut_an VARCHAR(255),                             
est_conforme VARCHAR(255),                          
insert_date timestamp,
event_date date);   


--Staging table in DWH
CREATE TABLE MON.SQ_FT_BDI_B2B(              
nom_structure VARCHAR(255),                         
numero_registre_commerce VARCHAR(255),              
num_piece_representant_legal VARCHAR(255),          
date_souscription timestamp,                  
adresse_structure VARCHAR(255),                     
msisdn VARCHAR(255),                                
nom_prenom VARCHAR(255),                            
numero_piece VARCHAR(255),                          
imei VARCHAR(255),                                  
adresse VARCHAR(255),                               
statut VARCHAR(255),                                
disponibilite_scan VARCHAR(255),                    
acceptation_cgv VARCHAR(255),                       
customer_id VARCHAR(255),                           
contract_id VARCHAR(255),                           
compte_client VARCHAR(255),                         
type_personne VARCHAR(255),                         
type_piece VARCHAR(255),                            
id_type_piece VARCHAR(255),                         
nom VARCHAR(255),                                   
prenom VARCHAR(255),                                
date_naissance date,                          
date_expiration date,                         
ville VARCHAR(255),                                 
quartier VARCHAR(255),                              
statut_old VARCHAR(255),                            
raison_statut VARCHAR(255),                         
odbic VARCHAR(255),                                 
odboc VARCHAR(255),                                 
date_changement_statut timestamp,             
plan_localisation VARCHAR(255),                     
contrat_soucription VARCHAR(255),                   
type_piece_tuteur VARCHAR(255),                     
numero_piece_tuteur VARCHAR(255),                   
nom_tuteur VARCHAR(255),                            
prenom_tuteur VARCHAR(255),                         
date_naissance_tuteur date,                   
date_expiration_tuteur date,                  
adresse_tuteur VARCHAR(255),                        
compte_client_structure VARCHAR(255),               
statut_derogation VARCHAR(255),                     
region_administrative VARCHAR(255),                 
region_commerciale VARCHAR(255),                    
site_name VARCHAR(255),                             
ville_site VARCHAR(255),                            
offre_commerciale VARCHAR(255),                     
type_contrat VARCHAR(255),                          
segmentation VARCHAR(255),                          
derogation_identification VARCHAR(255),             
compte_client_parent VARCHAR(255),                  
nom_representant_legal VARCHAR(255),                
prenom_representant_legal VARCHAR(255),             
contact_telephonique VARCHAR(255),                  
ville_structure VARCHAR(255),                       
quartier_structure VARCHAR(255),                    
sms_contact VARCHAR(255),                           
doc_plan_localisation VARCHAR(255),                 
doc_fiche_souscription VARCHAR(255),                
doc_attestation_cnps VARCHAR(255),                  
doc_rccm VARCHAR(255),                              
type_client VARCHAR(255),                           
rang int,                                     
type_personne_morale VARCHAR(255),                  
nom_structure_an VARCHAR(255),                      
rccm_an VARCHAR(255),                               
num_piece_rpstant_an VARCHAR(255),                  
date_souscription_an VARCHAR(255),                  
adresse_structure_an VARCHAR(255),                  
num_tel_an VARCHAR(255),                            
nom_prenom_an VARCHAR(255),                         
numero_piece_an VARCHAR(255),                       
imei_an VARCHAR(255),                               
adresse_an VARCHAR(255),                            
statut_an VARCHAR(255),                             
est_conforme VARCHAR(255),                          
insert_date timestamp,
event_date date);





--final table in DWH
DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_BDI_B2B';
  MIN_DATE_PARTITION := '20210601';
  MAX_DATE_PARTITION := '20220601';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_BDI_B2B';
  PART_PARTITION_NAME := 'FT_BDI_B2B_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_MIG_64K';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE (SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;