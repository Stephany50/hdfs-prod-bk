CREATE TABLE MON.SPARK_FT_ROAMING_CEMAC_IN_OUT  (
    CLIENT_OPERATOR VARCHAR (255),
    DIRECTION VARCHAR (255),
    ROAMING_PARTNER_NAME VARCHAR (255),
    ROAMING_PARTNER_COUNTRY VARCHAR (255),
    SERVICE_FAMILY VARCHAR (255),
    NUMBER_OF_CALLS DECIMAL(20,5),
    MINUTES DECIMAL(20,5),
    IMSI VARCHAR (55)
)
COMMENT 'SPARK_FT_ROAMING_CEMAC_IN_OUT - FT'
PARTITIONED BY (CALL_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


