
-- ***************************************
--*****************************************
--*****      CHANNEL DATAMART ECHEC   *****
--*****************************************
-- ***************************************
CREATE TABLE MON.DATAMART_OM_CHANNELS(
    ACTIF_ID STRING,
    MSISDN VARCHAR(15),
    SERVICE_TYPE STRING,
    STYLES STRING,
    TECHNOLOGY VARCHAR(255),
    PRODUCT_LINE VARCHAR(255),
    PRODUCT VARCHAR(255),
    DETAILS_MARKETING VARCHAR(255),
    DETAILS_CONFOMITY VARCHAR(255),
    BEAC VARCHAR(255),
    VOL BIGINT,
    VAL DECIMAL(27,2),
    REVENU DECIMAL(27,2),
    REVENU_INDIRECT DECIMAL(27,2),
    COMMISSION DECIMAL(27,2),
    SITE_NAME STRING
)
PARTITIONED BY (JOUR    DATE)
STORED AS ORC TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")
;