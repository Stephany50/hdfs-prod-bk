--*********************************
--***      MARKETING DATAMART    **
--*********************************
CREATE TABLE MON.DATAMART_OM_MARKETING (
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
  REVENU DECIMAL(27,2),
  REVENU_INDIRECT DECIMAL(27,2),
  COMMISSION DECIMAL(27,2),
  SITE_NAME STRING
)
PARTITIONED BY (JOUR    DATE)
STORED AS ORC TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")
;

 CREATE TABLE `MON.SPARK_DATAMART_OM_MARKETING2`(
   `msisdn` varchar(12),                          
   `user_id` varchar(50),                         
   `site_name` varchar(200),                      
   `service_type` varchar(50),                    
   `vol` DOUBLE,                           
   `val` DOUBLE,                           
   `commission` DOUBLE,                    
   `revenu` DOUBLE,                        
   `style` varchar(100),                          
   `details` varchar(100)
)                        
PARTITIONED BY (jour  DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

insert into MON.SPARK_DATAMART_OM_MARKETING2 
select 
    MSISDN,
    USER_ID,
    SITE_NAME,
    SERVICE_TYPE,
    VOL,
    VAL,
    COMMISSION,
    REVENU,
    STYLE,
    DETAILS,
    to_date(jour) jour
from backup_dwh.DATAMART_OM_MARKETING6