CREATE EXTERNAL  TABLE TMP.TT_KYC_CRM_B2C(
ORIGINAL_FILE_NAME    VARCHAR(100),
ORIGINAL_FILE_SIZE    VARCHAR(100),
ORIGINAL_FILE_LINE_COUNT INT,
GUID varchar(200),
DATE_ACTIVATION   VARCHAR(100),
STATUT   VARCHAR(100),
RAISON_STATUT   VARCHAR(100),
ENTITYHEADOFFICE   VARCHAR(100),
COMPTE_CLIENT   VARCHAR(100),
COMPTE_CLIENT_STRUCTURE   VARCHAR(100),
NOM_STRUCTURE   VARCHAR(100),
NUMERO_REGISTRE_COMMERCE   VARCHAR(100),
NUMERO_PIECE_REPRESENTANT_LEGAL   VARCHAR(100),
MSISDN   VARCHAR(100),
ID_TYPE_PIECE   VARCHAR(100),
NUMERO_PIECE   VARCHAR(100),
NOM   VARCHAR(100),
PRENOM   VARCHAR(100),
DATE_NAISSANCE   VARCHAR(100),
DATE_EXPIRATION   VARCHAR(100),
BIRTHPLACE   VARCHAR(100),
GENDER   VARCHAR(100),
quartier   VARCHAR(100),
VILLE   VARCHAR(100),
ADRESSE   VARCHAR(100),
OTHER_ADRESS_INFO   VARCHAR(100),
POST_CODE   VARCHAR(100),
REGION_PROVINCE   VARCHAR(100),
FATHERNAME   VARCHAR(100),
DOCUMENTISSUEDATE   VARCHAR(100),
DOCUMENTISSUEPLACE   VARCHAR(100),
ID_TYPE_PIECE_TUTEUR   VARCHAR(100),
NUMERO_PIECE_TUTEUR   VARCHAR(100),
PRENOM_TUTEUR   VARCHAR(100),
NOM_TUTEUR   VARCHAR(100),
DATE_NAISSANCE_TUTEUR   VARCHAR(100),
TUTORPROFESSION   VARCHAR(100),
TUTORGENDER   VARCHAR(100),
CCMODDATE VARCHAR(100)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/KYC/CRM/B2C/'
TBLPROPERTIES ('serialization.null.format'='');