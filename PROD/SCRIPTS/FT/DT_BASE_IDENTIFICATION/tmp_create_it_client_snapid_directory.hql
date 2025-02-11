CREATE TABLE TMP.IT_CLIENT_SNAPID_DIRECTORY
(
    MSISDN                  VARCHAR(50),
    PWDCLIENT               VARCHAR(100),
    LASTMOD                 DATE,
    NOM                     VARCHAR(100),
    PRENOM                  VARCHAR(100),
    DATENAISSANCE           VARCHAR(50),
    LIEUNAISSANCE           VARCHAR(100),
    GENRE                   VARCHAR(2),
    CIVILITE                VARCHAR(5),
    TYPEPIECEIDENTIFICATION VARCHAR(50),
    IDPIECEIDENTIFICATION   VARCHAR(50),
    DATEDELIVRANCE          VARCHAR(50),
    LIEUDELIVRANCE          VARCHAR(100),
    DATEEXPIRATION          VARCHAR(50),
    VILLE                   VARCHAR(100),
    QUARTIER                VARCHAR(100),
    SOURCE                  VARCHAR(15),
    COPIE                   VARCHAR(15),
    DATECREATION            VARCHAR(50),
    DATEDERNIEREMODIF       VARCHAR(50),
    SELLER_MSISDN           VARCHAR(40),
    PROFESSION              VARCHAR(255),
    ORIGINAL_FILE_NAME      VARCHAR(100),
    INSERT_DATE             TIMESTAMP,
    DATE_CAPTURE            VARCHAR(50)
)PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;
INSERT INTO TMP.IT_CLIENT_SNAPID_DIRECTORY SELECT
  MSISDN,
  PWDCLIENT,
  LASTMOD,
  NOM,
  PRENOM,
  DATENAISSANCE,
  LIEUNAISSANCE,
  GENRE,
  CIVILITE,
  TYPEPIECEIDENTIFICATION,
  IDPIECEIDENTIFICATION,
  DATEDELIVRANCE,
  LIEUDELIVRANCE,
  DATEEXPIRATION,
  VILLE,
  QUARTIER,
  SOURCE,
  COPIE,
  DATECREATION,
  DATEDERNIEREMODIF,
  SELLER_MSISDN,
  PROFESSION,
  ORIGINAL_FILE_NAME,
  INSERT_DATE,
  DATE_CAPTURE,
  TO_DATE(ORIGINAL_FILE_DATE)
FROM BACKUP_DWH.IT_CLIENT_SNAPID_DIRECTORY


CREATE TABLE BACKUP_DWH.IT_CLIENT_SNAPID_DIRECTORY
(
    MSISDN                  VARCHAR(50),
    PWDCLIENT               VARCHAR(100), 
    LASTMOD                 VARCHAR(50),
    NOM                     VARCHAR(100), 
    PRENOM                  VARCHAR(100), 
    DATENAISSANCE           VARCHAR(50), 
    LIEUNAISSANCE           VARCHAR(100), 
    GENRE                   VARCHAR(2), 
    CIVILITE                VARCHAR(5), 
    TYPEPIECEIDENTIFICATION VARCHAR(50), 
    IDPIECEIDENTIFICATION   VARCHAR(50), 
    DATEDELIVRANCE          VARCHAR(50), 
    LIEUDELIVRANCE          VARCHAR(100), 
    DATEEXPIRATION          VARCHAR(50), 
    VILLE                   VARCHAR(100), 
    QUARTIER                VARCHAR(100), 
    SOURCE                  VARCHAR(15), 
    COPIE                   VARCHAR(15), 
    DATECREATION            VARCHAR(50), 
    DATEDERNIEREMODIF       VARCHAR(50), 
    SELLER_MSISDN           VARCHAR(40), 
    PROFESSION              VARCHAR(255), 
    ORIGINAL_FILE_DATE      VARCHAR(50),
    ORIGINAL_FILE_NAME      VARCHAR(100),
    INSERT_DATE             VARCHAR(50),
    DATE_CAPTURE            VARCHAR(50)
)
STORED AS ORC ;