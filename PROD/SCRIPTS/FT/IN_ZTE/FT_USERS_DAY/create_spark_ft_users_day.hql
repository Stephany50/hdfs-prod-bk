CREATE TABLE MON.SPARK_FT_USERS_DAY
(
    FORMULE                VARCHAR(70),
    SERVICE                VARCHAR(20),
    ANY_DESTINATION        BIGINT,
    NATIONAL               BIGINT,
    MTN                    BIGINT,
    CAMTEL                 BIGINT,
    INTERNATIONAL          BIGINT,
    ONNET                  BIGINT,
    `SET`                  BIGINT,
    ROAM                   BIGINT,
    INROAM                 BIGINT,
    NEXTTEL                BIGINT,
    BUNDLE                 BIGINT,
    OPERATOR_CODE          VARCHAR(50),
    INSERT_DATE            TIMESTAMP,
     location_ci VARCHAR(50)
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')