CREATE TABLE MON.SPARK_FT_CBM_HANDSET_MONTHLY 
(
    MSISDN VARCHAR(50),
    IMEI VARCHAR(50),
    TOTAL_DAYS_COUNT INT,
    REGION VARCHAR(100),
    TOWN VARCHAR(100),
    QUARTER VARCHAR(100),
    PH_BRAND VARCHAR(300),
    PH_MODEL VARCHAR(300),
    DATA_2G INT,
    DATA_2_5G INT,
    DATA_2_75G INT,
    DATA_3G INT,
    DATA_4G INT,
    ACTIVATION_DATE DATE,
    LANG_ID VARCHAR(100),
    LAST_LOCATION_MONTH VARCHAR(20),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (PERIOD VARCHAR(20))
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')