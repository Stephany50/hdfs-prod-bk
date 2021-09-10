CREATE TABLE CDR.SPARK_IT_DIM_REF_SUBSCRIPTIONS (
    bdle_name varchar(500),
    prix decimal(17, 2),
    coeff_onnet decimal(17, 2),
    coeff_offnet decimal(17, 2),
    coeff_inter decimal(17, 2),
    coeff_data decimal(17, 2),
    coef_sms decimal(17, 2),
    coef_sva decimal(17, 2),
    coeff_roaming decimal(17, 2),
    coeff_roaming_voix decimal(17, 2),
    coeff_roaming_data decimal(17, 2),
    coeff_roaming_sms decimal(17, 2), 
    validite bigint,
    type_forfait varchar(500),
    destination varchar(500),
    type_ocm varchar(500),
    offre varchar(500),
    offer varchar(500),
    offer_1 varchar(500),
    offer_2 varchar(500),
    combo varchar(500),
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE VARCHAR(100))
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
