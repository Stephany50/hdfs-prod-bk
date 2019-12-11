CREATE TABLE CDR.IT_PREPAID_CLIENT_DIRECTORY (


  IDENT_ID               INT,

  NUMERO_TEL             VARCHAR(50),

  NOM                    VARCHAR(100),

  PRENOM                 VARCHAR(100),

  NEE_LE                 VARCHAR(25),

  NEE_A                  VARCHAR(100),

  PROFESSION             VARCHAR(150),

  QUARTIER_RESIDENCE     VARCHAR(150),

  VILLE_VILLAGE          VARCHAR(100),

  CNI                    VARCHAR(150),

  CNI_VALIDE_DE          VARCHAR(30),

  CNI_VALIDE_A           VARCHAR(30),

  ETABLIE_A              VARCHAR(75),

  NUMERO_AUTRE_1         VARCHAR(40),

  NUMERO_AUTRE_2         VARCHAR(40),

  INDATE                 VARCHAR(30),

  OUTDATE                VARCHAR(30),

  IDENTIFICATION_STATUS  VARCHAR(50),

  UTILISATEUR            VARCHAR(100),

  TYPE_DOCUMENT          VARCHAR(150),

  NUMERO_DOCUMENT        VARCHAR(150),

  FICHIER_CHARGEMENT     VARCHAR(150),

  SOURCE_PLATFORM        VARCHAR(40),

  ACTIVATION_STATUS      VARCHAR(50),

  STATUS_ICC             VARCHAR(50),

  STATUS_HLR_ENTRANT     VARCHAR(50),

  STATUS_HLR_SORTANT     VARCHAR(50),

  IMAGE_FILE             VARCHAR(200),

  ORIGINAL_FILE_NAME VARCHAR(50),

  ORIGINAL_FILE_SIZE INT,

  ORIGINAL_FILE_LINE_COUNT INT,

  INSERT_DATE TIMESTAMP
  )

PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(IDENT_ID) INTO 64 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");