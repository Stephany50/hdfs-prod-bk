create table MON.SPARK_FT_A_BDI_ART (
TYPE_PERSONNE  varchar(50),
NUM_PCE_ABSENT_NB  BIGINT,
NUM_PCE_INF4_NB  BIGINT,
NUM_PCE_NO_AUT_NB  BIGINT,
SCAN_PCE_ABSENT_NB  BIGINT,
NUM_PCE_TUT_ABSENT_NB  BIGINT,
NUM_PCE_TUT_NO_AUT_NB  BIGINT,
NUM_PCE_TUT_EG_M_NB  BIGINT,
NOM_PRENOM_ABSENT_NB  BIGINT,
NOM_PRENOM_DOUTEUX_NB  BIGINT,
DATE_EXPIR_ABSENT_NB  BIGINT,
DATE_EXPIR_DOUTEUX_NB  BIGINT,
DATE_NAISS_ABSENT_NB  BIGINT,
DATE_NAISS_DOUTEUX_NB  BIGINT,
MULTISIM_NB  BIGINT,
IMEI_ABSENT_NB  BIGINT,
NOM_TUT_ABSENT_NB  BIGINT,
NOM_TUT_DOUTEUX_NB  BIGINT,
DATE_NAISS_TUT_ABSENT_NB  BIGINT,
DATE_NAISS_TUT_DOUTEUX_NB  BIGINT,
DATE_ACTIV_ABSENT_NB  BIGINT,
NUM_TEL_ABSENT_NB  BIGINT,
TYPE_PCE_ID_ABSENT_NB  BIGINT,
ADRESSE_ABSENT_NB  BIGINT,
ADRESSE_DOUTEUX_NB  BIGINT,
CNI_EXPIR_NB  BIGINT,
CONTRAT_SOUSCRI_ABSENT_NB  BIGINT,
SCAN_FANTESISTE_NB  BIGINT,
NUMERO_PIECE_UNIQ_EN_LETTRE_NB  BIGINT,
NUM_PIECE_A_CARACT_NON_AUT_NB  BIGINT,
NUMERO_PIECE_EGALE_MSISDN_NB  BIGINT,
DATE_SOUSCRIPTION_ABSENT_NB  BIGINT,
PLAN_LOCAL_ABSENT_NB  BIGINT,
LIGNE_EN_ANOMALIE_NB  BIGINT,
ACTIF_FAMILLE_NB  BIGINT,
TOTAL_FAMILLE_NB  BIGINT,
insert_date timestamp
) COMMENT 'SPARK_FT_A_BDI_ART'
PARTITIONED BY (EVENT_DATE_PLUS1 DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')