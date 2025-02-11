CREATE TABLE MON.SPARK_FT_CLIENT_LAC_TRAFFIC_DAY 
(
MSISDN VARCHAR(25),
LAC VARCHAR(14),
DUREE_SORTANT DOUBLE,
NBRE_TEL_SORTANT INT,
DUREE_ENTRANT DOUBLE,
NBRE_TEL_ENTRANT INT,
NBRE_SMS_SORTANT INT,
NBRE_SMS_ENTRANT INT,
REFRESH_DATE DATE,
IMEI VARCHAR(15),
LAC_CORRECTED VARCHAR(14),
SERVED_PARTY_LOCATION VARCHAR(25),
NBRE_TEL_MTN_SORTANT INT,
NBRE_TEL_CAMTEL_SORTANT INT,
NBRE_TEL_OCM_SORTANT INT,
DUREE_TEL_MTN_SORTANT DOUBLE,
DUREE_TEL_CAMTEL_SORTANT DOUBLE,
DUREE_TEL_OCM_SORTANT DOUBLE,
NBRE_SMS_MTN_SORTANT INT,
NBRE_SMS_CAMTEL_SORTANT INT,
NBRE_SMS_OCM_SORTANT INT,
NBRE_SMS_ZEBRA_SORTANT INT,
NBRE_TEL_MTN_ENTRANT INT,
NBRE_TEL_CAMTEL_ENTRANT INT,
NBRE_TEL_OCM_ENTRANT INT,
DUREE_TEL_MTN_ENTRANT DOUBLE,
DUREE_TEL_CAMTEL_ENTRANT DOUBLE,
DUREE_TEL_OCM_ENTRANT DOUBLE,
NBRE_SMS_MTN_ENTRANT INT,
NBRE_SMS_CAMTEL_ENTRANT INT,
NBRE_SMS_OCM_ENTRANT INT,
NBRE_SMS_ZEBRA_ENTRANT INT,
NBRE_TEL_NEXTTEL_SORTANT INT,
DUREE_TEL_NEXTTEL_SORTANT DOUBLE,
NBRE_SMS_NEXTTEL_SORTANT INT,
NBRE_TEL_NEXTTEL_ENTRANT INT,
DUREE_TEL_NEXTTEL_ENTRANT DOUBLE,
NBRE_SMS_NEXTTEL_ENTRANT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')