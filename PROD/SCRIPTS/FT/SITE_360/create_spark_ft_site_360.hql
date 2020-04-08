CREATE TABLE MON.SPARK_FT_SITE_360
(
    LOC_SITE_NAME VARCHAR(100),
    LOC_TOWN_NAME VARCHAR(100),
    LOC_ADMINISTRATIVE_REGION VARCHAR(100),
    LOC_COMMERCIAL_REGION VARCHAR(100),
    LOC_ZONE_PMO VARCHAR(100),
    LOC_QUARTIER VARCHAR(100),
    LOC_ARRONDISSEMENT VARCHAR(100),
    LOC_DEPARTEMENT VARCHAR(100),
    LOC_SECTOR VARCHAR(100),
    PARC_GROUPE INT,
    PARC_ART INT,
    PARC_ACTIF_PERIOD INT,
    PARC_OM INT,
    DATA_USERS INT,
    VOICE_USERS INT,
    GROSS_ADD INT,
    NBRE_CALL_BOX INT,
    RUPTURE_STOCK INT,
    NBRE_FAMOCO INT,
    SMARTPHONES_3G INT,
    SMARTPHONES_4G INT,
    TOTAL_REVENUE DECIMAL(17, 2),
    TOTAL_VOICE_REVENUE DECIMAL(17, 2),
    TOTAL_SMS_REVENUE DECIMAL(17, 2),
    TOTAL_SUBS_REVENUE DECIMAL(17, 2),
    ROAM_IN_VOICE_REVENUE DECIMAL(17, 2),
    ROAM_OUT_VOICE_REVENUE DECIMAL(17, 2),
    ROAM_IN_SMS_REVENUE DECIMAL(17, 2),
    ROAM_OUT_SMS_REVENUE DECIMAL(17, 2),
    ROAM_DATA_REVENUE DECIMAL(17, 2),
    MAIN_RATED_TEL_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_OCM_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_MTN_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_NEXTTEL_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_CAMTEL_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_SET_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_ROAM_IN_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_ROAM_OUT_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_SVA_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_TEL_INT_AMOUNT DECIMAL(17, 2),
    MAIN_RATED_SMS_AMOUNT DECIMAL(17, 2),
    DATA_MAIN_RATED_AMOUNT DECIMAL(17, 2),
    DATA_GOS_MAIN_RATED_AMOUNT DECIMAL(17, 2),
    DATA_ROAM_MAIN_RATED_AMOUNT DECIMAL(17, 2),
    DATA_VIA_OM DECIMAL(17, 2),
    AMOUNT_EMERGENCY_DATA DECIMAL(17, 2),
    REVENU_OM DECIMAL(17, 2),
    C2S_REFILL_COUNT INT,
    C2S_MAIN_REFILL_AMOUNT DECIMAL(17, 2),
    C2S_PROMO_REFILL_AMOUNT DECIMAL(17, 2),
    P2P_REFILL_COUNT INT,
    P2P_REFILL_AMOUNT DECIMAL(17, 2),
    SCRATCH_REFILL_COUNT INT,
    SCRATCH_MAIN_REFILL_AMOUNT DECIMAL(17, 2),
    SCRATCH_PROMO_REFILL_AMOUNT DECIMAL(17, 2),
    P2P_REFILL_FEES DECIMAL(17, 2),
    DATA_REFILL_FEES DECIMAL(17, 2),
    OM_REFILL_COUNT INT,
    OM_REFILL_AMOUNT DECIMAL(17, 2),
    OG_RATED_CALL_DURATION DECIMAL(17, 2),
    OG_TOTAL_CALL_DURATION DECIMAL(17, 2),
    RATED_TEL_OCM_DURATION DECIMAL(17, 2),
    RATED_TEL_MTN_DURATION DECIMAL(17, 2),
    RATED_TEL_NEXTTEL_DURATION DECIMAL(17, 2),
    RATED_TEL_CAMTEL_DURATION DECIMAL(17, 2),
    RATED_TEL_SET_DURATION DECIMAL(17, 2),
    RATED_TEL_ROAM_IN_DURATION DECIMAL(17, 2),
    RATED_TEL_ROAM_OUT_DURATION DECIMAL(17, 2),
    RATED_TEL_SVA_DURATION DECIMAL(17, 2),
    RATED_TEL_INT_DURATION DECIMAL(17, 2),
    OG_SMS_TOTAL_COUNT BIGINT,
    OG_SMS_OCM_COUNT BIGINT,
    OG_SMS_MTN_COUNT BIGINT,
    OG_SMS_NEXTTEL_COUNT BIGINT,
    OG_SMS_CAMTEL_COUNT BIGINT,
    OG_SMS_SET_COUNT BIGINT,
    OG_SMS_ROAM_IN_COUNT BIGINT,
    OG_SMS_ROAM_OUT_COUNT BIGINT,
    OG_SMS_SVA_COUNT BIGINT,
    OG_SMS_INTERNATIONAL_COUNT BIGINT,
    DATA_BYTES_RECEIVED BIGINT,
    DATA_BYTES_SENT BIGINT,
    DATA_BYTES_USED_IN_BUNDLE_ROAM BIGINT,
    DATA_BYTES_USED_PAYGO_ROAM BIGINT,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')