CREATE EXTERNAL TABLE CDR.TT_CRM_ABONNEMENTS(
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ID VARCHAR(100),
  COMPTE_B2B VARCHAR(100),
  COMPTE_B2C VARCHAR(100),
  COMPTE_FACTURATION VARCHAR(100),
  TYPE_ABONNEMENT VARCHAR(100),
  LBL_TYPE_ABONNEMENT VARCHAR(100),
  MSISDN_IDWIMAX VARCHAR(100),
  ICCI_ADRESSE_MAC VARCHAR(100),
  IMSI VARCHAR(100),
  CODE_PUK1 VARCHAR(100),
  CODE_PUK2 VARCHAR(100),
  DATE_ACTIVATION VARCHAR(50),
  STATUT_ABONNEMENT VARCHAR(100),
  LBL_STATUT_ABONNEMENT VARCHAR(100),
  DATE_MAJ_STATUT VARCHAR(50),
  OFFRE VARCHAR(100),
  LBL_OFFRE VARCHAR(100),
  PROPRIETAIRE VARCHAR(100),
  LBL_PROPRIETAIRE VARCHAR(100)
)COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u00B6'
LOCATION '/PROD/TT/CRM/CRM_ABONNEMENTS'
TBLPROPERTIES ('serialization.null.format'='');

