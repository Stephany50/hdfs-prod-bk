CREATE  TABLE  MON.SPARK_TT_PICO_KPIS
(
  kpi  varchar(90),
  msisdn varchar(90),
  val  float,
  insert_date  timestamp
 )
PARTITIONED  BY  (event_month  varchar(7)  )
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'  )