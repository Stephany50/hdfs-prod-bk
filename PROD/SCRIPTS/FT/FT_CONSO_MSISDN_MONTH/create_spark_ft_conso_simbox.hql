CREATE TABLE MON.SPARK_FT_CONSO_SIMBOX
(MSISDN VARCHAR(20 ),
DETECT_MONTH VARCHAR(10 ),
CONSO_MONTH VARCHAR(10 ),
CONSO double,
TEL_DURATION bigint,
TEL_COUNT int,
INSERTED_DATE DATE,
PLATFORM VARCHAR(20 ),
PROFILE VARCHAR(30 ),
MTN_DURATION int,
CAMTEL_DURATION int,
INTERNATIONAL_DURATION int
)COMMENT ' SPARK_FT_CONSO_SIMBOX '
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")