create table mon.spark_ft_cbm_recycles_daily 
  (
  subs_id int,
  msisdn varchar(50),
  prod_state varchar(10),
  block_reason varchar(50),
  activation_date date
  )PARTITIONED BY(recyclage_date date)
STORED AS PARQUET TBLPROPERTIES('PARQUET.COMPRESS'='SNAPPY')