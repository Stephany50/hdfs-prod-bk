CREATE EXTERNAL TABLE CDR.TT_CRM_COMPTE_B2C
   (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
COMPTE_B2C VARCHAR(100),
  COMPTE_CLIENT_PRODUIT VARCHAR(100),
  CIVILITE VARCHAR(100),
  LBL_CIVILITE VARCHAR(100),
  NOM VARCHAR(100),
  PRENOM VARCHAR(100),
  DEUXIEME_PRENOM VARCHAR(100),
  TYPE_RELATION VARCHAR(100),
  LBL_TYPE_RELATION VARCHAR(50),
  TYPE_PIECE_IDENTITE VARCHAR(100),
  LBL_TYPE_PIECE_IDENTITE VARCHAR(50),
  NUM_PIECE_IDENTITE VARCHAR(50),
  DATE_EXPIRATION_CNI VARCHAR(100),
  RESPONSABLE_DU_COMPTE VARCHAR(100),
  CATEGORIE_CLIENT VARCHAR(100),
  LBL_CATEGORIE_CLIENT VARCHAR(50),
  FONCTION VARCHAR(50),
  SOCIETE VARCHAR(100),
  COURRIER_ELECTRONIQUE VARCHAR(100),
  TELEPHONE_PROFESSIONEL VARCHAR(50),
  TELEPHONE_MOBILE VARCHAR(50),
  TELECOPIE VARCHAR(100),
  TELEPHONE_PERSONNEL VARCHAR(50),
  VIP_VOIX VARCHAR(100),
  VIP_SMS VARCHAR(100),
  VIP_DATA VARCHAR(100),
  VIP_OM VARCHAR(100),
  VIP_GLOBALE VARCHAR(100),
  LANGUE VARCHAR(100),
  LBL_LANGUE VARCHAR(100),
  DEUXIEME_LANGUE VARCHAR(100),
  LBL_DEUXIEME_LANGUE VARCHAR(100),
  TROISIEME_LANGUE VARCHAR(100),
  LBL_TROISIEME_LANGUE VARCHAR(100),
  STATUT_CLIENT VARCHAR(100),
  LBL_STATUT_CLIENT VARCHAR(50),
  GENRE VARCHAR(100),
  LBL_GENRE VARCHAR(100),
  ETAT_CIVIL VARCHAR(100),
  LBL_ETAT_CIVIL VARCHAR(100),
  NOM_CONJOINT VARCHAR(100),
  DATE_NAISSANCE VARCHAR(100),
  LIEU_NAISSANCE VARCHAR(100),
  NATIONALITE VARCHAR(100),
  DATE_CREATION VARCHAR(100),
  ALIAS_LINKEDIN VARCHAR(100),
  ALIAS_FACEBOOK VARCHAR(100),
  ALIAS_TWITTER VARCHAR(100),
  HOMOLOGATION_SCORING VARCHAR(100),
  PROPRIETAIRE VARCHAR(100),
  LBL_PROPRIETAIRE VARCHAR(100),
  PROSPECT_ORIGINE VARCHAR(100),
  LBL_PROSPECT_ORIGINE VARCHAR(100),
  DATE_DERNIERE_CAMPAGNE_MKT VARCHAR(100),
  ENVOYER_DOCUMENTS_MKT VARCHAR(100),
  LBL_ENVOYER_DOCUMENTS_MKT VARCHAR(100),
  LIMITE_CREDIT VARCHAR(100),
  SUSPENSION_CREDIT VARCHAR(100),
  EXONERATION_TVA VARCHAR(100),
  VALIDE_AU VARCHAR(100),
  MODE_COMMUNICATION_PRIVILEGIE VARCHAR(100),
  LBL_MODE_COMM_PRIVILEGIE VARCHAR(100),
  JOUR_PRIVILEGIE VARCHAR(100),
  LBL_JOUR_PRIVILEGIE VARCHAR(100),
  HEURE_PRIVILEGIEE VARCHAR(100),
  LBL_HEURE_PRIVILEGIEE VARCHAR(100),
  ENVOI_COURRIER_ELECTRONIQUE VARCHAR(100),
  LBL_ENVOI_COURRIER_ELECT VARCHAR(100),
  ENVOI_COURRIER_ELECT_EN_NBRE VARCHAR(100),
  LBL_ENVOI_COURRIER_ELECT_NBRE VARCHAR(100),
  ENVOI_TELEPHONE_SMS VARCHAR(100),
  LBL_ENVOI_TELEPHONE_SMS VARCHAR(100),
  ENVOI_TELECOPIE VARCHAR(100),
  LBL_ENVOI_TELECOPIE VARCHAR(100),
  ENVOI_COURRIER_POSTAL VARCHAR(100),
  LBL_ENVOI_COURRIER_POSTAL VARCHAR(100)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY 'U+204B'
LOCATION '/PROD/TT/CRM/CRM_COMPTE_B2C/'
TBLPROPERTIES ('serialization.null.format'='');