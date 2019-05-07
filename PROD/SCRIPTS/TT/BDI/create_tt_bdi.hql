CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_BDI (
  MSISDN                    VARCHAR(400),
  NUMERO_PIECE              VARCHAR(400),
  TYPE_PIECE                VARCHAR(400),
  NOM                       VARCHAR(400),
  DATE_NAISSANCE            VARCHAR(100),
  DATE_EXPIRATION           VARCHAR(100),
  ADDRESSE                  VARCHAR(400),
  NUMERO_PIECE_TUTEUR       VARCHAR(400),
  NOM_PARENT                VARCHAR(400),
  DATE_NAISSANCE_TUTEUR     VARCHAR(400),
  NOM_STRUCTURE             VARCHAR(400),
  NUMERO_REGISTRE_COMMERCE  VARCHAR(400),
  NUMERO_PIECE_REP_LEGAL    VARCHAR(400),
  TYPE_PERSONNE             VARCHAR(400),
  DATE_ACTIVATION           VARCHAR(400),
  DATE_CHANGEMENT_STATUT    VARCHAR(400),
  STATUT_BSCS               VARCHAR(400),
  ODBINCOMINGCALLS          VARCHAR(400),
  ODBOUTGOINGCALLS          VARCHAR(400),
  IMEI                      VARCHAR(400),
  ADRESSE_TUTEUR            VARCHAR(400),
  ACCEPTATION_CGV           VARCHAR(400),
  CONTRAT_SOUCRIPTION       VARCHAR(400),
  DISPONIBILITE_SCAN        VARCHAR(400),
  PLAN_LOCALISATION         VARCHAR(400),
  TYPE_PIECE_TUTEUR         VARCHAR(400),
  DATE_VALIDATION_BO        VARCHAR(100),
  STATUT_VALIDATION_BO      VARCHAR(100),
  MOTIF_REJET_BO            VARCHAR(400),
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'BDI external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/BDI'
TBLPROPERTIES ('serialization.null.format'='')
;