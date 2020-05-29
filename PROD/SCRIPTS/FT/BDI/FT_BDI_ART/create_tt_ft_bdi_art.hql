create table TMP.TT_FT_BDI_ART (
MSISDN  VARCHAR(400),
TYPE_PIECE  VARCHAR(400),
NUMERO_PIECE  VARCHAR(400),
NOM_PRENOM  VARCHAR(400),
NOM  VARCHAR(400),
PRENOM  VARCHAR(400),
DATE_NAISSANCE  DATE,
DATE_EXPIRATION  DATE,
ADRESSE  VARCHAR(400),
NUMERO_PIECE_TUTEUR  VARCHAR(400),
NOM_PARENT  VARCHAR(400),
DATE_NAISSANCE_TUTEUR  DATE,
NOM_STRUCTURE  VARCHAR(400),
NUMERO_REGISTRE_COMMERCE  VARCHAR(400),
NUMERO_PIECE_REP_LEGAL  VARCHAR(400),
DATE_ACTIVATION  TIMESTAMP,
DATE_CHANGEMENT_STATUT  TIMESTAMP,
STATUT_BSCS  VARCHAR(400),
ODBINCOMINGCALLS  VARCHAR(400),
ODBOUTGOINGCALLS  VARCHAR(400),
IMEI  VARCHAR(400),
STATUT_DEROGATION  VARCHAR(400),
REGION_ADMINISTRATIVE  VARCHAR(400),
REGION_COMMERCIALE  VARCHAR(400),
SITE_NAME  VARCHAR(400),
VILLE  VARCHAR(400),
LONGITUDE  VARCHAR(400),
LATITUDE  VARCHAR(400),
OFFRE_COMMERCIALE  VARCHAR(400),
TYPE_CONTRAT  VARCHAR(400),
SEGMENTATION  VARCHAR(400),
REV_M_3  DECIMAL(17,5),
REV_M_2  DECIMAL(17,5),
REV_M_1  DECIMAL(17,5),
REV_MOY  DECIMAL(17,5),
STATUT_IN  VARCHAR(400),
NUMERO_PIECE_ABSENT  CHAR(3),
NUMERO_PIECE_TUT_ABSENT  CHAR(3),
NUMERO_PIECE_INF_4  CHAR(3),
NUMERO_PIECE_TUT_INF_4  CHAR(3),
NUMERO_PIECE_NON_AUTHORISE  CHAR(3),
NUMERO_PIECE_TUT_NON_AUTH  CHAR(3),
NUMERO_PIECE_EGALE_MSISDN  CHAR(3),
NUMERO_PIECE_TUT_EGALE_MSISDN  CHAR(3),
NUMERO_PIECE_A_CARACT_NON_AUTH  CHAR(3),
NUMERO_PIECE_TUT_CARAC_NON_A  CHAR(3),
NUMERO_PIECE_UNIQUEMENT_LETTRE  CHAR(3),
NUMERO_PIECE_TUT_UNIQ_LETTRE  CHAR(3),
NOM_PRENOM_ABSENT  CHAR(3),
NOM_PARENT_ABSENT  CHAR(3),
NOM_PRENOM_DOUTEUX  CHAR(3),
NOM_PARENT_DOUTEUX  CHAR(3),
DATE_NAISSANCE_ABSENT  CHAR(3),
DATE_NAISSANCE_TUT_ABSENT  CHAR(3),
DATE_EXPIRATION_ABSENT  CHAR(3),
ADRESSE_ABSENT  CHAR(3),
ADRESSE_DOUTEUSE  CHAR(3),
TYPE_PERSONNE_INCONNU  CHAR(3),
MINEUR_MAL_IDENTIFIE  CHAR(3),
EST_PREMIUM  VARCHAR(10),
TYPE_PIECE_TUTEUR  VARCHAR(400),
ADRESSE_TUTEUR  VARCHAR(400),
ACCEPTATION_CGV  VARCHAR(400),
CONTRAT_SOUCRIPTION  VARCHAR(400),
DISPONIBILITE_SCAN  VARCHAR(400),
PLAN_LOCALISATION  VARCHAR(400),
TYPE_PERSONNE_I  VARCHAR(400),
DATE_VALIDATION_BO  TIMESTAMP,
STATUT_VALIDATION_BO  VARCHAR(100),
MOTIF_REJET_BO  VARCHAR(400),
INSERT_DATE  TIMESTAMP,
EVENT_DATE  DATE
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')