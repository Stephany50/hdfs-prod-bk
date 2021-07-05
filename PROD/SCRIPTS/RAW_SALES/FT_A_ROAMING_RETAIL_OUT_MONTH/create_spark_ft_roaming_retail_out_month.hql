CREATE TABLE AGG.SPARK_FT_A_ROAMING_RETAIL_OUT_MONTH (
    DIRECTION    VARCHAR (255),
    TARRIF_PLAN   VARCHAR (255),
    TYPE_ABONNE       VARCHAR(20),
    SEGMENT_CLIENT VARCHAR(10), 
    PAYS_VISITE    VARCHAR (255),
    TAP_CODE   VARCHAR (255),
    OPERATEUR_VISITE VARCHAR(255), 
    FORFAIT_ROAMING VARCHAR(255), 
    SUBS_CHANNEL VARCHAR(255), 
    SERVICE_TYPE VARCHAR(255), 
    SERVICE_DETAIL VARCHAR(255), 
    DESTINATION VARCHAR(255), 
    NOMBRE_ACTE INT,
    VOLUME_MINUTE DECIMAL(20,5),
    VOLUME_DATA DECIMAL(20,5),
    REVENU_MAIN DECIMAL(20,5),
    NUMBERS_SUBS DECIMAL(20,5),
    UNIQUE_ROAMERS DECIMAL(20,5),
    ACTIVE_ROAMERS DECIMAL(20,5),
    ROAMERS_UNDER_BUNDLE DECIMAL(20,5)
)
COMMENT 'SPARK_FT_A_ROAMING_RETAIL_OUT - FT'
PARTITIONED BY (EVENT_MONTH VARCHAR(10))
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')