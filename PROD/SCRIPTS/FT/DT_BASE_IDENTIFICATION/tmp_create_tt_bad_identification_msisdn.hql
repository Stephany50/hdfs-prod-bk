-- auto-generated definition
create table DATALAB.TT_BAD_IDENTIFICATION_MSISDN
(
    MSISDN                    VARCHAR(4000),
    NOM                       VARCHAR(100),
    PRENOM                    VARCHAR(100),
    NEE_LE                    DATE,
    NEE_A                     VARCHAR(100),
    PROFESSION                VARCHAR(150),
    QUARTIER_RESIDENCE        VARCHAR(150),
    VILLE_VILLAGE             VARCHAR(100),
    CNI                       VARCHAR(150),
    DATE_IDENTIFICATION       TIMESTAMP ,
    TYPE_DOCUMENT             VARCHAR(4000),
    FICHIER_CHARGEMENT        VARCHAR(150),
    DATE_INSERTION            TIMESTAMP,
    EST_SNAPPE                VARCHAR(10),
    IDENTIFICATEUR            VARCHAR(100),
    DATE_MISE_A_JOUR          TIMESTAMP ,
    DATE_TABLE_MIS_A_JOUR     TIMESTAMP ,
    GENRE                     VARCHAR(10),
    CIVILITE                  VARCHAR(15),
    TYPE_PIECE_IDENTIFICATION VARCHAR(50),
    PROFESSION_IDENTIFICATEUR VARCHAR(100)
)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;