--*********************************
--***      MARKETING DATAMART    **
--*********************************
CREATE TABLE MON.SPARK_DATAMART_OM_MARKETING (
  ACTIF_ID STRING,
  MSISDN VARCHAR(15),
  SERVICE_TYPE VARCHAR(10),
  STYLES STRING,
  TECHNOLOGY STRING,
  PRODUCT_LINE STRING,
  PRODUCT STRING,
  DETAILS_MARKETING STRING,
  DETAILS_CONFOMITY STRING,
  BEAC STRING,
  VOL BIGINT,
  VAL DECIMAL(27,2),
  direct_revenue DECIMAL(27,2),
  REVENU_INDIRECT DECIMAL(27,2),
  other_revenue DECIMAL(27,2),
  COMMISSION DECIMAL(27,2),
  SITE_NAME STRING
)
PARTITIONED BY (JOUR    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

| actif_id                      | string                                             |                             |
| msisdn                        | varchar(15)                                        |                             |
| service_type                  | varchar(10)                                        |                             |
| styles                        | string                                             |                             |
| technology                    | string                                             |                             |
| product_line                  | string                                             |                             |
| product                       | string                                             |                             |
| details_marketing             | string                                             |                             |
| details_confomity             | string                                             |                             |
| beac                          | string                                             |                             |
| vol                           | bigint                                             |                             |
| val                           | decimal(27,2)                                      |                             |
| direct_revenue                | decimal(27,2)                                      |                             |
| indirect_revenue              | decimal(27,2)                                      |                             |
| other_revenue                 | decimal(27,2)                                      |                             |
| commission                    | decimal(27,2)                                      |                             |
| site_name                     | string                                             |                             |
| jour                          | date