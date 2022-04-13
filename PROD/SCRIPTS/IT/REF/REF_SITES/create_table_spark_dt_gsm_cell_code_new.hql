CREATE TABLE DIM.SPARK_DT_GSM_CELL_CODE_NEW(
    SITE_CODE VARCHAR(100),     
    CELLNAME VARCHAR(100),      
    SITE_NAME VARCHAR(100),     
    LONGITUDE VARCHAR(100),     
    LATITUDE VARCHAR(100),      
    LAC VARCHAR(100),           
    CI_2G_3G VARCHAR(100),      
    IDBTS_4G VARCHAR(100),      
    CI BIGINT,                  
    SITE_PROGRAM VARCHAR(100),  
    TYPEDESITE VARCHAR(100),    
    TOWERHEIGHT VARCHAR(100),   
    DATEMAD VARCHAR(100),       
    DATEMET VARCHAR(100),       
    DATEARRET VARCHAR(100),     
    TECHNOSITE VARCHAR(100),    
    TECHNOLOGIE VARCHAR(100),   
    FREQUENCES VARCHAR(100),    
    FREQUENCES2 VARCHAR(100),   
    CATEGORIE_SITE VARCHAR(100),
    NOMBAILLEUR VARCHAR(100),   
    EXPIRATIONBAIL VARCHAR(100),
    TYPOLOGIESITE VARCHAR(100), 
    TYPOLOGIESITE2 VARCHAR(100),
    CODEPARTENAIRE VARCHAR(100),
    NBRETENANTS VARCHAR(100),   
    PRIORITE VARCHAR(100),      
    CONFIG VARCHAR(100),        
    TOPOLOGY VARCHAR(100),      
    TYPOLOGIETRANS VARCHAR(100),
    AGGREGATION VARCHAR(100),   
    CANALDETRANSMISSION VARCHAR(100),
    TOWNNAME VARCHAR(100),      
    QUARTIER VARCHAR(100),      
    REGION VARCHAR(100),        
    DEPARTEMENT VARCHAR(100),   
    ARRONDISSEMENT VARCHAR(100),
    TYPEDEZONE VARCHAR(100),    
    COMMERCIAL_REGION VARCHAR(100),  
    ZONEPMO VARCHAR(100),       
    SECTEUR_OM VARCHAR(100),    
    ZONE VARCHAR(50),           
    SECTEUR VARCHAR(50),        
    REGION_BUS VARCHAR(200)
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')




CREATE EXTERNAL TABLE CDR.TT_DIM_REF_SITES_NEW
(
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,         
    ORIGINAL_FILE_LINE_COUNT INT,   
    CODESITE VARCHAR(200),          
    CODESITE_NEW VARCHAR(200),      
    NOMDUSITE VARCHAR(200),         
    NOMDUSITE_NEW VARCHAR(200),     
    LONGITUDE VARCHAR(200),         
    LATITUDE VARCHAR(200),          
    CATEGORIE_DE_SITE VARCHAR(200), 
    CELLULE VARCHAR(200),           
    LAC VARCHAR(200),               
    CI VARCHAR(200),                
    ID_BTS VARCHAR(200),            
    ID_BTS_NEW VARCHAR(200),        
    DATE_MET_CELL VARCHAR(200),     
    DATE_ARRET_CELL VARCHAR(200),   
    TECHNO_CELL VARCHAR(200),       
    FREQUENCE_CELL VARCHAR(200),    
    PROGRAMME VARCHAR(200),         
    TYPE_DE_SITE VARCHAR(200),      
    TOWER_HEIGHT VARCHAR(200),      
    DATE_MAD_SITE VARCHAR(200),     
    DATE_MET_SITE VARCHAR(200),     
    DATE_ARRET_SITE VARCHAR(200),   
    TECHNO_SITE VARCHAR(200),       
    FREQUENCES_SITE VARCHAR(200),   
    NOM_BAILLEUR VARCHAR(200),      
    EXPIRATION__BAIL VARCHAR(200),  
    TYPOLOGIE_SITE VARCHAR(200),    
    TYPOLOGIE_SITE_2 VARCHAR(200),  
    CODE_PARTENAIRE VARCHAR(200),   
    NBRE_TENANTS VARCHAR(200),      
    PRIORITE VARCHAR(200),          
    CONFIG VARCHAR(200),            
    TOPOLOGY VARCHAR(200),          
    TYPOLOGIE_TRANS VARCHAR(200),   
    AGGREGATION VARCHAR(200),       
    CANAL_DE_TRANSMISSION VARCHAR(200),
    LOCALITE VARCHAR(200),          
    QUARTIER VARCHAR(200),          
    REGION_TERR VARCHAR(200),       
    REGION_BUS VARCHAR(200),        
    DEPARTEMENT VARCHAR(200),       
    ARRONDISSEMENT VARCHAR(200),    
    TYPE_DE_ZONE VARCHAR(200),      
    REGION_COMERCIALE VARCHAR(200), 
    ZONE_PMO VARCHAR(200),          
    SECTEUR_OM VARCHAR(200),        
    ERP_GEO_CODE VARCHAR(200)
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/REF_SITES_NEW'
TBLPROPERTIES ('SERIALIZATION.NULL.FORMAT'='')

SELECT CI, COLLECT_SET(QUARTIER), collect_set(technologie) FROM DIM.SPARK_DT_GSM_CELL_CODE GROUP BY CI HAVING COUNT(distinct region) > 1;