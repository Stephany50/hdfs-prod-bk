create table mon.spark_ft_cbm_recycles_daily 
  (
  msisdn varchar(50),
  activation_date date
  )PARTITIONED BY(recyclage_date date)
STORED AS PARQUET TBLPROPERTIES('PARQUET.COMPRESS'='SNAPPY')