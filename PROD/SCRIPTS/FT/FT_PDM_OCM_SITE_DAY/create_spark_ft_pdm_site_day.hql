CREATE TABLE MON.SPARK_FT_PDM_OCM_SITE_DAY
(
MSISDN VARCHAR(26),
CI VARCHAR(14),
NETWORK VARCHAR(50),
REFRESH_DATE TIMESTAMP,
DUREE_SORTANT INT,
NBRE_TEL_SORTANT INT,
DUREE_ENTRANT INT,
NBRE_TEL_ENTRANT INT,
NBRE_SMS_SORTANT INT,
NBRE_SMS_ENTRANT INT
) PARTITIONED BY (EVENT_DATE    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')