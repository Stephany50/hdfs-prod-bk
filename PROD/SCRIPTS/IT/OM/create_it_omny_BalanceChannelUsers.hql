CREATE TABLE CDR.IT_OMNY_BalanceChannelUsers (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
CATEGORY_CODE                 VARCHAR(30),
USER_ID                       VARCHAR(30),
WALLET_NUMBER                 VARCHAR(30),
MSISDN                        VARCHAR(30),
PARENT_USER_ID                VARCHAR(30),
PARENT_MSISDN                 VARCHAR(30),
OWNER_USER_ID                 VARCHAR(30),
OWNER_MSISDN                  VARCHAR(30),
OPENING_BALANCE               DECIMAL(17,2),
CLOSING_BALANCE               DECIMAL(17,2),
AMT_TRANS_OUT                 DECIMAL(17,2),
NB_TRANS_OUT                  BIGINT,
AMT_TRANS_IN                  DECIMAL(17,2),
NB_TRANS_IN                   BIGINT,
AMT_TRANS_REIMB_OUT           DECIMAL(17,2),
NB_TRANS_REIMB_OUT            BIGINT,
AMT_TRANS_REIMB_IN            DECIMAL(17,2),
NB_TRANS_REIMB_IN             BIGINT,
AMT_SC_OUT                    DECIMAL(17,2),
NB_SC_OUT                     BIGINT,
AMT_SC_IN                     DECIMAL(17,2),
NB_SC_IN                      BIGINT,
AMT_COM_OUT                   DECIMAL(17,2),
NB_COM_OUT                    BIGINT,
AMT_COM_IN                    DECIMAL(17,2),
NB_COM_IN                     BIGINT,
AMT_COM_REIMB_OUT             DECIMAL(17,2),
NB_COM_REIMB_OUT              BIGINT,
AMT_COM_REIMB_IN              DECIMAL(17,2),
NB_COM_REIMB_IN               BIGINT,
AMT_COM_OUT_OTHER             DECIMAL(17,2),
NB_COM_OUT_OTHER              BIGINT,
AMT_COM_IN_OTHER              DECIMAL(17,2),
NB_COM_IN_OTHER               BIGINT,
AMT_COM_REIMB_OUT_OTHER       DECIMAL(17,2),
NB_COM_REIMB_OUT_OTHER        BIGINT,
AMT_COM_REIMB_IN_OTHER        DECIMAL(17,2),
NB_COM_REIMB_IN_OTHER         BIGINT,
NB_NONFIN_TRANS               BIGINT,
NB_NONFIN_TRANS_CANCELLED     BIGINT,
AMT_SC_NONFIN_OUT             DECIMAL(17,2),
NB_SC_NONFIN_OUT              BIGINT,
AMT_SC_NONFIN_IN              DECIMAL(17,2),
NB_SC_NONFIN_IN               BIGINT,
AMT_COM_NONFIN_OUT            DECIMAL(17,2),
NB_COM_NONFIN_OUT             BIGINT,
AMT_COM_NONFIN_IN             DECIMAL(17,2),
NB_COM_NONFIN_IN              BIGINT,
AMT_TOTAL_OUT                 DECIMAL(17,2),
NB_TOTAL_OUT                  BIGINT,
AMT_TOTAL_IN                  DECIMAL(17,2),
NB_TOTAL_IN                   BIGINT,
INSERT_DATE                  TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(CATEGORY_CODE) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
