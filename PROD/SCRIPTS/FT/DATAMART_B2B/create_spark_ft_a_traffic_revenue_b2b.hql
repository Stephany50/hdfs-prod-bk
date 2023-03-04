CREATE TABLE AGG.SPARK_TRAFFIC_REVENUE_B2B (
    PRODUCT VARCHAR (200),
    PRODUCT_DESCRIPTION VARCHAR (200),
    PROFIL VARCHAR (200),
    MAIN_PRODUCT VARCHAR (200),
    OFFER VARCHAR (200),
    REVENUE FLOAT
)
COMMENT 'TRAFFIC REVENUE B2B - FT'
PARTITIONED BY ( EVENT_DATE  DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


CREATE EXTERNAL TABLE TT.REF_DIM_USAGE_NEW (
    USAGE_CODE VARCHAR(100),
    PARENT_CODE VARCHAR(100),
    USAGE_DESCRIPTION VARCHAR(100),
    TELESERVICE_INDICATOR VARCHAR(100),
    USAGE_TYP_REF VARCHAR(100),
    DOMAIN VARCHAR(100),
    PRODUCT VARCHAR(100),
    USAGE_MEAN VARCHAR(100)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/MARCELE/REFERENTIEL'
TBLPROPERTIES ('serialization.null.format'='');

CREATE TABLE TT.DIM_DT_USAGE_NEW AS

SELECT * FROM TT.REF_DIM_USAGE_NEW;

CREATE EXTERNAL TABLE TT.REF_DIM_PROILE_NEW (
    OFFER_PROFILE_ID VARCHAR(100),
    CUSTOMER_TYPE VARCHAR(100),
    CUSTOMER_PROFILE VARCHAR(100),
    OFFER_NAME VARCHAR(100),
    PROFILE_CODE VARCHAR(100),
    PROFILE_NAME VARCHAR(100),
    INITIAL_CREDIT VARCHAR(100),
    RESALE_OF_TRAFFIC VARCHAR(100),
    RATEPLAN_ID VARCHAR(100),
    PLATFORM VARCHAR(100),
    CRM_SEGMENTATION VARCHAR(100),
    DECILE_TYPE VARCHAR(100),
    VALID_FROM_DATECODE VARCHAR(100),
    VALID_TO_DATECODE VARCHAR(100),
    IVR_NUMBER VARCHAR(100),
    PROFILE_ID VARCHAR(100),
    CONTRACT_TYPE VARCHAR(100),
    ESSBASE_SEGMENTATION VARCHAR(100),
    ESSBASE_RATEPLAN VARCHAR(100),
    OFFER_GROUP VARCHAR(100),
    OPERATOR_CODE VARCHAR(100),
    HORIZON_DOMAIN_CODE VARCHAR(100),
    HORIZON_DOMAIN_DESC VARCHAR(100),
    HORIZON_MARKET_CODE VARCHAR(100),
    HORIZON_MARKET_DESC VARCHAR(100),
    HORIZON_OFFER_CODE VARCHAR(100),
    HORIZON_OFFER_DESC VARCHAR(100),
    SEGMENTATION VARCHAR(100),
    OFFRE_B_TO_B VARCHAR(100),
    CAT_OFFRE_B_TO_B VARCHAR(100),
    INSERT_DATE VARCHAR(100)

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/MARCELE/DATAMARTB2B/PROFILE'
TBLPROPERTIES ('serialization.null.format'='');

CREATE TABLE TT.DIM_DT_PROFILE_NEW AS

SELECT * FROM TT.REF_DIM_PROILE_NEW;


CREATE TABLE AGG.SPARK_TRAFFIC_REVENUE_B2B_FINAL (
    PRODUCT VARCHAR (200),
    PRODUCT_DESCRIPTION VARCHAR (200),
    PROFIL VARCHAR (200),
    MAIN_PRODUCT VARCHAR (200),
    OFFER VARCHAR (200),
    REVENUE FLOAT
)
COMMENT 'TRAFFIC REVENUE B2B FINAL - FT'
PARTITIONED BY ( EVENT_DATE  DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
