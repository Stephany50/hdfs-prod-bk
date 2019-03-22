
CREATE TABLE MON.FT_X_INTERCO_FINAL (
    SRC 	VARCHAR (40) ,
    CRA_SRC 	VARCHAR (20) ,
    HEURE 	VARCHAR (2) ,
    FAISCEAU 	VARCHAR (40) ,
    USAGE_APPEL 	VARCHAR (40) ,
    INDICATION_APPEL 	VARCHAR (40) ,
    TYPE_APPEL 	VARCHAR (25) ,
    TYPE_ABONNE 	VARCHAR (40) ,
    DESTINATION_APPEL 	VARCHAR (50) ,
    TYPE_HEURE 	VARCHAR (15) ,
    NBRE_APPEL 	INT,
    DUREE_APPEL 	INT ,
    INSERTED_DATE	TIMESTAMP,
    OPERATOR_CODE 	VARCHAR (25)
 ) COMMENT 'MON.FT_X_INTERCO_FINAL'
PARTITIONED BY (SDATE	DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")


