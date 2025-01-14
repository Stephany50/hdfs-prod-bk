create table MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY
(
    PROFILE              VARCHAR(100),
    STATUT               VARCHAR(10),
    ETAT                 VARCHAR(16),
    BSCS_COMM_OFFER      VARCHAR(100),
    TRANCHE_AGE          INT,
    CUST_BILLCYCLE       VARCHAR(100),
    CREDIT               DOUBLE,
    EFFECTIF             BIGINT,
    ACTIVATIONS          BIGINT,
    RESILIATIONS         BIGINT,
    SRC_TABLE            VARCHAR(30),
    INSERT_DATE          TIMESTAMP,
    PLATFORM_STATUS      VARCHAR(20),
    EFFECTIF_CLIENTS_OCM BIGINT,
    DECONNEXIONS         BIGINT,
    CONNEXIONS           BIGINT,
    RECONNEXIONS         BIGINT,
    OPERATOR_CODE        VARCHAR(50)
)
PARTITIONED BY (EVENT_DATE    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;

alter table MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY add columns (location_ci varchar(50))