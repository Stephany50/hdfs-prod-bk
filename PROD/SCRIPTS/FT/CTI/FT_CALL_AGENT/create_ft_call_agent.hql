CREATE TABLE CTI.FT_CALL_AGENT

(
    START_DATE_TIME_KEY     INT,
    INTERACTION_TYPE         VARCHAR(32),
    RESOURCE_NAME            VARCHAR(255),
    AGENT_FIRST_NAME         VARCHAR(64),
    AGENT_LAST_NAME          VARCHAR(64),
    RESOURCE_TYPE            VARCHAR(32),
    TECHNICAL_RESULT         VARCHAR(255),
    RESULT_REASON            VARCHAR(32),
    RESOURCE_ROLE            VARCHAR(32),
    ROLE_REASON              VARCHAR(32),
    UD_LANGUE                VARCHAR(255),
    UD_VIPGSM                VARCHAR(255),
    UD_VIPOMY                VARCHAR(255),
    UD_ISOMY                 VARCHAR(255),
    UD_LISTSEGMENT           VARCHAR(255),
    UD_SEGMENT               VARCHAR(255),
    UD_SITE_CIBLE            VARCHAR(255),
    UD_SITE_CHOISI           VARCHAR(255),
    UD_COMP                  VARCHAR(255),
    UD_COMP_DEB              VARCHAR(255),
    UD_COMP_CHOISI           VARCHAR(255),
    UD_CRISE_SITE            VARCHAR(255),
    UD_CRISE_FERM            VARCHAR(255),
    UD_CRISE_FLUX            VARCHAR(255),
    UD_CRISE_DISSU           VARCHAR(255),
    UD_DISTRIBUTED           VARCHAR(255),
    UD_NRONA                 VARCHAR(255),
    UD_HNO                   VARCHAR(255),
    UD_XFER_PRESTA           VARCHAR(255),
    UD_FERMETURE             VARCHAR(255),
    UD_DISSUASION            VARCHAR(255),
    UD_DNIS                  VARCHAR(255),
    UD_DUREEATTENTE          VARCHAR(255),
    UD_PRIORITEFLUX          VARCHAR(255),
    CLIENT                   VARCHAR(255),
    TPS_RP                   INT,
    TPS_ACW                  INT,
    TPS_SONNERIE             INT,
    TPS_CONVERSATION_AGENT   INT,
    TPS_CONVERSATION_CLIENT  INT,
    HOLD_DURATION            INT,
    IRF_INTERACTION_ID       INT,
    SDATE_DATETIME           TIMESTAMP,
    INSERT_DATE              TIMESTAMP
)
COMMENT 'CTI.FT_CALL_AGENT - FT'
PARTITIONED BY (SDATE    DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
