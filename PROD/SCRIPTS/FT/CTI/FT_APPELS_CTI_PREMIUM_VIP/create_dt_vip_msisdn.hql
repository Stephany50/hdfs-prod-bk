CREATE EXTERNAL TABLE cti.tt_vip_msisdn (
    INSTITUTIONS varchar(200),
    FONCTION_TWITTER varchar(200),
    TITULAIRE varchar(200),
    MOBILE varchar(20),
    FOLLOWERS varchar(200)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/SNR/CTI_VIP_MSISDN'
TBLPROPERTIES ('serialization.null.format'='')

CREATE TABLE cti.dt_vip_msisdn (
    INSTITUTIONS varchar(200),
    FONCTION_TWITTER varchar(200),
    TITULAIRE varchar(200),
    MOBILE varchar(20),
    FOLLOWERS varchar(200),
    insert_date timestamp,
    updated_date timestamp
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

CREATE EXTERNAL TABLE TMP.DT_CTI_PREMIUM_NUMBER (
    MSISDN varchar(20),
    NOM varchar(50),
    PRENOM varchar(50),
    VILLE varchar(50),
    REGION varchar(50),
    MARQUE varchar(50),
    MODELE varchar(50),
    ACTIVATION_DATE varchar(20),
    TECHNOLOGIE varchar(20),
    ARPU1 varchar(20),
    ARPU varchar(20),
    Segmentation varchar(20),
    ARPU2 varchar(20),
    Segment varchar(20)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/SNR/CTI_PREMIUM/'
TBLPROPERTIES ('serialization.null.format'='')

CREATE TABLE DIM.DT_CTI_PREMIUM_NUMBER STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY') AS
SELECT * FROM TMP.DT_CTI_PREMIUM_NUMBER;

create table dim.spark_dt_offer_profiles (
offer_profile_id   decimal(10,0),
customer_type      varchar(50)  ,
customer_profile   varchar(50)  ,
offer_name         varchar(50)  ,
profile_code       varchar(100) ,
profile_name       varchar(50)  ,
initial_credit     decimal(10,0),
resale_of_traffic  char(1)      ,
rateplan_id        decimal(10,0),
platform           varchar(5)   ,
crm_segmentation   varchar(13)  ,
decile_type        varchar(4)   ,
valid_from_datecode varchar(8)   ,
valid_to_datecode  varchar(8)   ,
ivr_number         varchar(3)   ,
profile_id         varchar(5)   ,
contract_type      varchar(25)  ,
essbase_segmentation          varchar(15)  ,
essbase_rateplan   varchar(15)  ,
offer_group        varchar(50)  ,
operator_code      varchar(50)  ,
horizon_domain_code varchar(30)  ,
horizon_domain_desc varchar(50)  ,
horizon_market_code varchar(30)  ,
horizon_market_desc varchar(50)  ,
horizon_offer_code varchar(30)  ,
horizon_offer_desc varchar(50)  ,
segmentation       varchar(50)  ,
offre_b_to_b       varchar(100) ,
cat_offre_b_to_b   varchar(200) ,
insert_date        timestamp
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')