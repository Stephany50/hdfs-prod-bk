CREATE TABLE MON.SPARK_FT_MSISDN_TRAFIC_DATA_SITE
(    
    MSISDN VARCHAR(75),
    IMEI VARCHAR(75),
    SITE_NAME VARCHAR(75),
    DUREE_TEL_ENTRANT DECIMAL(20,2),
    DUREE_TEL_SORTANT DECIMAL(20,2),
    NBRE_TEL_ENTRANT DECIMAL(20,2),
    NBRE_TEL_SORTANT DECIMAL(20,2),
    NBRE_SMS_ENTRANT DECIMAL(20,2),
    NBRE_SMS_SORTANT DECIMAL(20,2),
    BYTES_USED DECIMAL(20,2),
    PAYASYOUGO DECIMAL(20,2),
    BUNDLE_VOLUME_USED DECIMAL(20,2),
    SOURCE VARCHAR(75),
    INSERT_DATE DATE,
    OTARIE_BYTES_2G DECIMAL(20,2),
    OTARIE_BYTES_3G DECIMAL(20,2),
    OTARIE_BYTES_4G DECIMAL(20,2),
    OTARIE_BYTES_UKN DECIMAL(20,2)
) 
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY') 