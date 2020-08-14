create table TMP.TT_DT_BASE_ID_CONFORMITE (
msisdn                      varchar(100),            
nom                         varchar(100),             
prenom                      varchar(100),             
nee_le                      date,                    
nee_a                       varchar(100),             
profession                  varchar(150),             
quartier_residence          varchar(150),             
ville_village               varchar(100),             
cni                         varchar(150),             
date_identification         varchar(100),             
type_document               varchar(255),            
fichier_chargement          varchar(150),             
date_insertion              varchar(100),             
est_snappe                  varchar(10),              
identificateur              varchar(100),             
date_mise_a_jour            varchar(100),             
date_table_mis_a_jour       varchar(100),             
genre                       varchar(10),              
civilite                    varchar(15),              
type_piece_identification   varchar(50),              
profession_identificateur   varchar(100),             
motif_rejet                 varchar(100),
--

)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')