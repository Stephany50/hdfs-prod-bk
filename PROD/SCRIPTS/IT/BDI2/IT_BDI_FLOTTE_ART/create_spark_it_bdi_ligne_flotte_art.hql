 CREATE TABLE  CDR.SPARK_IT_BDI_FLOTTE_ART (         
     msisdn  varchar(50),                             
     customer_id  varchar(255),                       
     contract_id  varchar(255),                       
     compte_client  varchar(255),                     
     type_personne  string,                           
     type_piece  string,                              
     numero_piece  string,                            
     id_type_piece  string,                           
     nom_prenom  string,                              
     nom  string,                                     
     prenom  string,                                  
     date_naissance  string,                          
     date_expiration  string,                         
     adresse  string,                                 
     ville  string,                                   
     quartier  string,                                
     date_souscription  varchar(255),                 
     date_activation  varchar(255),                   
     statut  varchar(255),                            
     raison_statut  varchar(255),                     
     date_changement_statut  varchar(255),            
     plan_localisation  string,                       
     contrat_soucription  string,                     
     disponibilite_scan  string,                      
     acceptation_cgv  string,                         
     type_piece_tuteur  string,                       
     numero_piece_tuteur  string,                     
     nom_tuteur  string,                              
     prenom_tuteur  string,                           
     date_naissance_tuteur  string,                   
     date_expiration_tuteur  string,                  
     adresse_tuteur  string,                          
     compte_client_structure  string,                 
     nom_structure  string,                           
     numero_registre_commerce  string,                
     numero_piece_representant_legal  string,         
     imei  varchar(255),                              
     statut_derogation  string,                       
     region_administrative  varchar(255),             
     region_commerciale  varchar(255),                
     site_name  varchar(255),                         
     ville_site  varchar(255),                        
     offre_commerciale  varchar(255),                 
     type_contrat  varchar(255),                      
     segmentation  varchar(255),                      
     odbincomingcalls  varchar(255),                  
     odboutgoingcalls  varchar(255),                  
     derogation_identification  string,               
     insert_date  timestamp) COMMENT 'CDR.SPARK_IT_BDI_FLOTTE_ART'
PARTITIONED BY (original_file_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');