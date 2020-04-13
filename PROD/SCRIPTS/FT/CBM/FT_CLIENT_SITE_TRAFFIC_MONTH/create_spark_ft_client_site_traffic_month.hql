CREATE TABLE MON.SPARK_FT_CLIENT_SITE_TRAFFIC_MONTH 
(
    MSISDN VARCHAR(25), 
    SITE_NAME VARCHAR(50), 
    DUREE_SORTANT DECIMAL(17, 2), 
    NBRE_TEL_SORTANT BIGINT, 
    DUREE_ENTRANT DECIMAL(17, 2), 
    NBRE_TEL_ENTRANT BIGINT, 
    NBRE_SMS_SORTANT BIGINT, 
    NBRE_SMS_ENTRANT BIGINT, 
    REFRESH_DATE DATE, 
    TOWNNAME VARCHAR(50), 
    ADMINISTRATIVE_REGION VARCHAR(50), 
    COMMERCIAL_REGION VARCHAR(50), 
    OPERATOR_CODE VARCHAR(20)
)
PARTITIONED BY (EVENT_MONTH VARCHAR(6))
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')