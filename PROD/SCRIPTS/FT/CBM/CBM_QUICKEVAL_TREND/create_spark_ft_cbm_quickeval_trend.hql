create table mon.spark_ft_cbm_quickeval_trend
  (
  _date date,
  temoin varchar(5),
  ipp1 varchar(50),
  problematique varchar(50),
  ca_forfait_periode2 decimal(30, 8),
  volume_data_periode2 decimal(30, 8),
  mou_periode2 decimal(30, 8)
  )PARTITIONED BY(event_date date)
STORED AS PARQUET TBLPROPERTIES('PARQUET.COMPRESS'='SNAPPY')