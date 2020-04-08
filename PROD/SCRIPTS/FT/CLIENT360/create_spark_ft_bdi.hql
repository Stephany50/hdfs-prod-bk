CREATE TABLE MON.SPARK_FT_BDI
(
    MSISDN                          VARCHAR(400),
    TYPE_PIECE                      VARCHAR(400),
    NUMERO_PIECE                    VARCHAR(400),
    NOM_PRENOM                      VARCHAR(400),
    NOM                             VARCHAR(400),
    PRENOM                          VARCHAR(400),
    DATE_NAISSANCE                  DATE,
    DATE_EXPIRATION                 DATE,
    ADDRESSE                        VARCHAR(400),
    NUMERO_PIECE_TUTEUR             VARCHAR(400),
    NOM_PARENT                      VARCHAR(400),
    DATE_NAISSANCE_TUTEUR           DATE,
    NOM_STRUCTURE                   VARCHAR(400),
    NUMERO_REGISTRE_COMMERCE        VARCHAR(400),
    NUMERO_PIECE_REP_LEGAL          VARCHAR(400),
    DATE_ACTIVATION                 DATE,
    DATE_CHANGEMENT_STATUT          DATE,
    STATUT_BSCS                     VARCHAR(400),
    ODBINCOMINGCALLS                VARCHAR(400),
    ODBOUTGOINGCALLS                VARCHAR(400),
    IMEI                            VARCHAR(400),
    STATUT_DEROGATION               VARCHAR(400),
    REGION_ADMINISTRATIVE           VARCHAR(400),
    REGION_COMMERCIALE              VARCHAR(400),
    SITE_NAME                       VARCHAR(400),
    VILLE                           VARCHAR(400),
    LONGITUDE                       VARCHAR(400),
    LATITUDE                        VARCHAR(400),
    OFFRE_COMMERCIALE               VARCHAR(400),
    TYPE_CONTRAT                    VARCHAR(400),
    SEGMENTATION                    VARCHAR(400),
    REV_M_3                         DECIMAL(20, 2),
    REV_M_2                         DECIMAL(20, 2),
    REV_M_1                         DECIMAL(20, 2),
    REV_MOY                         DECIMAL(20, 2),
    STATUT_IN                       VARCHAR(400),
    NUMERO_PIECE_ABSENT             VARCHAR(3),
    NUMERO_PIECE_TUT_ABSENT         VARCHAR(3),
    NUMERO_PIECE_INF_4              VARCHAR(3),
    NUMERO_PIECE_TUT_INF_4          VARCHAR(3),
    NUMERO_PIECE_NON_AUTHORISE      VARCHAR(3),
    NUMERO_PIECE_TUT_NON_AUTH       VARCHAR(3),
    NUMERO_PIECE_EGALE_MSISDN       VARCHAR(3),
    NUMERO_PIECE_TUT_EGALE_MSISDN   VARCHAR(3),
    NUMERO_PIECE_A_CARACT_NON_AUTH  VARCHAR(3),
    NUMERO_PIECE_TUT_CARAC_NON_A    VARCHAR(3),
    NUMERO_PIECE_UNIQUEMENT_LETTRE  VARCHAR(3),
    NUMERO_PIECE_TUT_UNIQ_LETTRE    VARCHAR(3),
    NOM_PRENOM_ABSENT               VARCHAR(3),
    NOM_PARENT_ABSENT               VARCHAR(3),
    NOM_PRENOM_DOUTEUX              VARCHAR(3),
    NOM_PARENT_DOUTEUX              VARCHAR(3),
    DATE_NAISSANCE_ABSENT           VARCHAR(3),
    DATE_NAISSANCE_TUT_ABSENT       VARCHAR(3),
    DATE_EXPIRATION_ABSENT          VARCHAR(3),
    ADDRESSE_ABSENT                 VARCHAR(3),
    ADDRESSE_DOUTEUSE               VARCHAR(3),
    TYPE_PERSONNE_INCONNU           VARCHAR(3),
    MINEUR_MAL_IDENTIFIE            VARCHAR(3),
    INSERT_DATE                     DATE,
    TYPE_PERSONNE                   VARCHAR(400),
    DATE_ACQUISITION                DATE,
    DATE_NAISSANCE_DOUTEUX          VARCHAR(3),
    DATE_NAISSANCE_TUT_DOUTEUX      VARCHAR(3),
    DATE_EXPIRATION_DOUTEUSE        VARCHAR(3),
    CNI_EXPIRE                      VARCHAR(3),
    MULTI_SIM                       VARCHAR(3),
    EST_PRESENT_OM                  VARCHAR(3),
    EST_PRESENT_ZEB                 VARCHAR(3),
    EST_PRESENT_ART                 VARCHAR(400),
    EST_PRESENT_GP                  VARCHAR(400),
    EST_PRESENT_OCM                 VARCHAR(3),
    EST_ACTIF_OM                    VARCHAR(3),
    EST_CLIENT_VIP                  VARCHAR(3),
    REV_OM_M_3                      DECIMAL(20, 2),
    REV_OM_M_2                      DECIMAL(20, 2),
    REV_OM_M_1                      DECIMAL(20, 2),
    EST_ACTIF_DATA                  VARCHAR(3),
    TRAFFIC_DATA_M_3                DECIMAL(20, 2),
    TRAFFIC_DATA_M_2                DECIMAL(20, 2),
    TRAFFIC_DATA_M_1                DECIMAL(20, 2),
    CONFORM_OCM_P_MORALE_M2M        VARCHAR(3),
    CONFORM_ART_P_MORALE_M2M        VARCHAR(3),
    CONFORM_OCM_P_MORALE_FLOTTE     VARCHAR(3),
    CONFORM_ART_P_MORALE_FLOTTE     VARCHAR(3),
    CONFORM_OCM_P_PHY_MAJEUR        VARCHAR(3),
    CONFORM_ART_P_PHY_MAJEUR        VARCHAR(3),
    CONFORM_OCM_P_PHY_MINEUR        VARCHAR(400),
    CONFORM_ART_P_PHY_MINEUR        VARCHAR(400),
    EST_SUSPENDU                    VARCHAR(400),
    NOM_STRUCTURE_ABSENT            VARCHAR(400),
    NUMERO_REGISTRE_ABSENT          VARCHAR(400),
    NUMERO_REGISTRE_DOUTEUX         VARCHAR(400),
    CONFORME_ART                    VARCHAR(400),
    CONFORME_OCM                    VARCHAR(400),
    IMEI_ABSENT                     VARCHAR(400),
    ADRESSE_ABSENT                  VARCHAR(400),
    EST_PREMIUM                     VARCHAR(10),
    ADRESSE_TUTEUR                  VARCHAR(400),
    TYPE_PIECE_TUTEUR               VARCHAR(400),
    ACCEPTATION_CGV                 VARCHAR(400),
    CONTRAT_SOUCRIPTION             VARCHAR(400),
    DISPONIBILITE_SCAN              VARCHAR(400),
    PLAN_LOCALISATION               VARCHAR(400),
    IDENTIFICATEUR                  VARCHAR(50),
    PROFESSION_IDENTIFICATEUR       VARCHAR(50),
    DATE_VALIDATION_BO              VARCHAR(100),
    STATUT_VALIDATION_BO            VARCHAR(100),
    MOTIF_REJET_BO                  VARCHAR(400),
    STATUT_VALIDATION_BOO           VARCHAR(400),
    EST_SNAPPE VARCHAR(50),
    DISPONIBILITE_SCAN_NEW VARCHAR(50)
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')