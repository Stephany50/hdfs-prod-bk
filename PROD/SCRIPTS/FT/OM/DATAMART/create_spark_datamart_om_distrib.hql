-- ***************************************
--*****************************************
--*****      CHANNEL DATAMART ECHEC   *****
--*****************************************
-- ***************************************
CREATE TABLE MON.SPARK_DATAMART_OM_DISTRIB(
  `msisdn` varchar(100),
   `user_id` varchar(100),
   `site_name` varchar(50),
   `service_type` varchar(100),
   `vol` DOUBLE,
   `val` DOUBLE,
   `commission` DOUBLE,
   `revenu` DOUBLE,
   `style` varchar(13)
)
PARTITIONED BY (JOUR    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

INSERT INTO MON.SPARK_DATAMART_OM_DISTRIB
SELECT msisdn ,user_id,site_name,service_type,vol,val,commission,revenu,style,to_date(JOUR) JOUR from  backup_dwh.SPARK_DATAMART_OM_DISTRIB