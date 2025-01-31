CREATE TABLE MON.SPARK_ACTIVATION_ALL_BY_DAY
(
    MSISDN VARCHAR(60),
    ACTIVATION_DATE TIMESTAMP,
    IDENTIFICATEUR VARCHAR(100),
    GENRE VARCHAR(10),
    CIVILITE VARCHAR(15),
    EST_SNAPPE VARCHAR(10),
    INSERT_DATE TIMESTAMP
)
    PARTITIONED BY (EVENT_DATE DATE)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY")