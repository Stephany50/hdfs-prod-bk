-- *****************************************
--*******************************************
--*****      MARKETING DATAMART ECHEC   *****
--*******************************************
-- *****************************************
CREATE TABLE MON.DATAMART_ECHEC_MARKETING (
  ACTIF_ID STRING,
  MSISDN VARCHAR(15),
  SERVICE_TYPE VARCHAR(10),
  STYLES STRING,
  TECHNOLOGY STRING,
  PRODUCT_LINE STRING,
  PRODUCT STRING,
  DETAILS_MARKETING STRING,
  DETAILS_CONFOMITY STRING,
  TRANSFER_STATUS VARCHAR(3),
  ERROR_CODE VARCHAR(20),
  ERROR_DESC VARCHAR(350),
  BEAC STRING,
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