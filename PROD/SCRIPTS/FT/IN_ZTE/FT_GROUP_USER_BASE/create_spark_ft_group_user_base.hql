CREATE TABLE MON.SPARK_FT_GROUP_USER_BASE
(
    FORMULE                   VARCHAR(50),
    EFFECTIF                  BIGINT,
    ALL30DAYSBASE             BIGINT,
    DAILYBASE                 BIGINT,
    ALL30DAYSWINBACK          BIGINT,
    ALL30DAYSLOST             BIGINT,
    CHURN                     BIGINT,
    GROSSADDS                 BIGINT,
    INSERT_DATE               TIMESTAMP,
    SRC_TABLE                 VARCHAR(100)
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

alter table MON.SPARK_FT_GROUP_USER_BASE add columns (location_ci varchar(50));
