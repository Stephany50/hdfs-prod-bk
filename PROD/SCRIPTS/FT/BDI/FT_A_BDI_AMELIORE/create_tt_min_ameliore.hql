create table TMP.TT_MIN_AMELIORE (
NB_NUMERO_PIECE_ABSENT BIGINT,
NB_NUMERO_PIECE_INF_4 BIGINT,
NB_NUMERO_PIECE_NON_AUTHORISE BIGINT,
NB_SCAN_INDISPONIBLE BIGINT,
NB_NUMERO_PIECE_TUT_ABSENT BIGINT,
NB_NUMERO_PIECE_TUT_NON_AUTH BIGINT,
NB_NUM_PIECE_TUT_EGALE_MSISDN BIGINT,
NB_NOM_PRENOM_ABSENT BIGINT,
NOM_PRENOM_DOUTEUX BIGINT,
NB_DATE_EXPIRATION_ABSENT BIGINT,
NB_DATE_EXPIRATION_DOUTEUSE BIGINT,
NB_DATE_NAISSANCE_ABSENT BIGINT,
NB_DATE_NAISSANCE_DOUTEUX BIGINT,
NB_MULTI_SIM BIGINT,
NB_IMEI_ABSENT BIGINT,
NB_NOM_PARENT_ABSENT BIGINT,
NB_NOM_PARENT_DOUTEUX BIGINT,
NB_DATE_NAISSANCE_TUT_ABSENT BIGINT,
NB_DATE_NAISSANCE_TUT_DOUTEUX BIGINT,
NB_DATE_ACTIVATION_ABSENTE BIGINT,
NB_MSISDN_ABSENT BIGINT,
NB_TYPE_PIECE_IDENTITE_ABSENTE BIGINT,
NB_ADRESSE_ABSENT BIGINT,
NB_ADDRESSE_DOUTEUSE BIGINT,
NB_CNI_EXPIRE BIGINT,
NB_CONTRAT_SOUCRIPTION_ABSENT BIGINT,
SCAN_FANTAISISTE BIGINT,
NB_PLAN_LOCALISATION_ABSENT BIGINT,
NB_LIGNE_EN_ANOMALIE BIGINT,
nb_actifs BIGINT,
NB_FAMILLE BIGINT
) STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')