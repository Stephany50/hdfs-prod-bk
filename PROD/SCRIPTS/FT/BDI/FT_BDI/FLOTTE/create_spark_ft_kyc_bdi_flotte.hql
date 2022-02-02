CREATE TABLE MON.SPARK_FT_KYC_BDI_FLOTTE(      
cust_guid VARCHAR(255),         
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
insert_date timestamp) COMMENT 'SPARK_FT_KYC_BDI_FLOTTE'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');