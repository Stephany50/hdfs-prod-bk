CREATE TABLE MON.SPARK_FT_C2S_BY_CHANNEL_DAY
(
    IS_VIA_ORANGE_MONEY INT,
    SITE_NAME VARCHAR(50),
    TOWNNAME VARCHAR(50),
    REGION VARCHAR(50),
    MONTANT INT,
    NOMBRE INT,
    INSERT_DATE DATE

)
PARTITIONED BY(EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")














