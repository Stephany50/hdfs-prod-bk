CREATE TABLE AGG.FT_A_APPELS_CTI_COUNT  (
mois varchar(10),
semaine varchar(10),
type_periode varchar(20),
nombre_client int,
insert_date timestamp
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;