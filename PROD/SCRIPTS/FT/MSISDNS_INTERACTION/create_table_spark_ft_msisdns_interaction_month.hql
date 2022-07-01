CREATE TABLE MON.SPARK_FT_MSISDNS_INTERACTION_MONTH (
    msisdn     VARCHAR(100),
    contact1 VARCHAR(100),
    contact2 VARCHAR(100),
    contact3 VARCHAR(100),
    contact4 VARCHAR(100),
    contact5 VARCHAR(100),
    contact6 VARCHAR(100),
    contact7 VARCHAR(100),
    contact8 VARCHAR(100),
    contact9 VARCHAR(100),
    contact10 VARCHAR(100),
    contact11 VARCHAR(100),
    contact12 VARCHAR(100),
    contact13 VARCHAR(100),
    contact14 VARCHAR(100),
    contact15 VARCHAR(100),
    contact16 VARCHAR(100),
    contact17 VARCHAR(100),
    contact18 VARCHAR(100),
    contact19 VARCHAR(100),
    contact20 VARCHAR(100),
    insert_date timestamp
)
PARTITIONED BY (event_month VARCHAR(10))
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')