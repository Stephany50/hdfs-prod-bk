create table MON.SPARK_FT_BDI_ART (
MSISDN VARCHAR(255),
TYPE_PIECE VARCHAR(255),
NUMERO_PIECE VARCHAR(255),
NOM_PRENOM VARCHAR(255),
NOM VARCHAR(255),
PRENOM VARCHAR(255),
DATE_NAISSANCE DATE,
DATE_EXPIRATION DATE,
ADRESSE VARCHAR(255),
NUMERO_PIECE_TUTEUR VARCHAR(255),
NOM_PARENT VARCHAR(255),
DATE_NAISSANCE_TUTEUR DATE,
NOM_STRUCTURE VARCHAR(255),
NUMERO_REGISTRE_COMMERCE VARCHAR(255),
NUMERO_PIECE_REP_LEGAL VARCHAR(255),
DATE_ACTIVATION TIMESTAMP,
DATE_CHANGEMENT_STATUT TIMESTAMP,
STATUT_BSCS VARCHAR(255),
ODBINCOMINGCALLS VARCHAR(255),
ODBOUTGOINGCALLS VARCHAR(255),
IMEI VARCHAR(255),
STATUT_DEROGATION VARCHAR(255),
REGION_ADMINISTRATIVE VARCHAR(255),
REGION_COMMERCIALE VARCHAR(255),
SITE_NAME VARCHAR(255),
VILLE VARCHAR(255),
LONGITUDE VARCHAR(255),
LATITUDE VARCHAR(255),
OFFRE_COMMERCIALE VARCHAR(255),
TYPE_CONTRAT VARCHAR(255),
SEGMENTATION VARCHAR(255),
REV_M_3 DECIMAL(17,2),
REV_M_2 DECIMAL(17,2),
REV_M_1 DECIMAL(17,2),
REV_MOY DECIMAL(17,2),
STATUT_IN VARCHAR(255),
NUMERO_PIECE_ABSENT CHAR(3),
NUMERO_PIECE_TUT_ABSENT CHAR(3),
NUMERO_PIECE_INF_4 CHAR(3),
NUMERO_PIECE_TUT_INF_4 CHAR(3),
NUMERO_PIECE_NON_AUTHORISE CHAR(3),
NUMERO_PIECE_TUT_NON_AUTH CHAR(3),
NUMERO_PIECE_EGALE_MSISDN CHAR(3),
NUMERO_PIECE_TUT_EGALE_MSISDN CHAR(3),
NUMERO_PIECE_A_CARACT_NON_AUTH CHAR(3),
NUMERO_PIECE_TUT_CARAC_NON_A CHAR(3),
NUMERO_PIECE_UNIQUEMENT_LETTRE CHAR(3),
NUMERO_PIECE_TUT_UNIQ_LETTRE CHAR(3),
NOM_PRENOM_ABSENT CHAR(3),
NOM_PARENT_ABSENT CHAR(3),
NOM_PRENOM_DOUTEUX CHAR(3),
NOM_PARENT_DOUTEUX CHAR(3),
DATE_NAISSANCE_ABSENT CHAR(3),
DATE_NAISSANCE_TUT_ABSENT CHAR(3),
DATE_EXPIRATION_ABSENT CHAR(3),
ADRESSE_ABSENT CHAR(3),
ADRESSE_DOUTEUSE CHAR(3),
TYPE_PERSONNE_INCONNU CHAR(3),
MINEUR_MAL_IDENTIFIE CHAR(3),
TYPE_PERSONNE VARCHAR(255),
DATE_ACQUISITION DATE,
DATE_NAISSANCE_DOUTEUX CHAR(3),
DATE_NAISSANCE_TUT_DOUTEUX CHAR(3),
DATE_EXPIRATION_DOUTEUSE CHAR(3),
CNI_EXPIRE CHAR(3),
MULTI_SIM CHAR(3),
EST_PRESENT_OM CHAR(3),
EST_PRESENT_ZEB CHAR(3),
EST_PRESENT_ART VARCHAR(255),
EST_PRESENT_GP VARCHAR(255),
EST_PRESENT_OCM CHAR(3),
EST_ACTIF_OM CHAR(3),
EST_CLIENT_VIP CHAR(3),
REV_OM_M_3 DECIMAL(17,2),
REV_OM_M_2 DECIMAL(17,2),
REV_OM_M_1 DECIMAL(17,2),
EST_ACTIF_DATA CHAR(3),
TRAFFIC_DATA_M_3 DECIMAL(17,7),
TRAFFIC_DATA_M_2 DECIMAL(17,7),
TRAFFIC_DATA_M_1 DECIMAL(17,7),
CONFORM_OCM_P_MORALE_M2M CHAR(3),
CONFORM_ART_P_MORALE_M2M CHAR(3),
CONFORM_OCM_P_MORALE_FLOTTE CHAR(3),
CONFORM_ART_P_MORALE_FLOTTE CHAR(3),
CONFORM_OCM_P_PHY_MAJEUR CHAR(3),
CONFORM_ART_P_PHY_MAJEUR CHAR(3),
CONFORM_OCM_P_PHY_MINEUR VARCHAR(255),
CONFORM_ART_P_PHY_MINEUR VARCHAR(255),
EST_SUSPENDU VARCHAR(255),
NOM_STRUCTURE_ABSENT VARCHAR(255),
NUMERO_REGISTRE_ABSENT VARCHAR(255),
NUMERO_REGISTRE_DOUTEUX VARCHAR(255),
CONFORME_ART VARCHAR(255),
CONFORME_OCM VARCHAR(255),
IMEI_ABSENT VARCHAR(255),
EST_PREMIUM VARCHAR(10),
ADRESSE_TUTEUR VARCHAR(255),
TYPE_PIECE_TUTEUR VARCHAR(255),
ACCEPTATION_CGV VARCHAR(255),
CONTRAT_SOUCRIPTION VARCHAR(255),
DISPONIBILITE_SCAN VARCHAR(255),
PLAN_LOCALISATION VARCHAR(255),
IDENTIFICATEUR VARCHAR(50),
PROFESSION_IDENTIFICATEUR VARCHAR(50),
DATE_VALIDATION_BO TIMESTAMP,
STATUT_VALIDATION_BO VARCHAR(100),
MOTIF_REJET_BO VARCHAR(255),
STATUT_VALIDATION_BOO VARCHAR(255),
DISPONIBILITE_SCAN_SID VARCHAR(50),
EST_CONFORME_MAJ_KYC CHAR(3),
EST_CONFORME_MIN_KYC CHAR(3),
EST_SNAPPE VARCHAR(50),
INSERT_DATE TIMESTAMP
) COMMENT 'MON_SPARK_FT_BDI'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')